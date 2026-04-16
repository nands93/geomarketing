from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import geopandas as gpd
import pandas as pd
import statistics
from shapely.geometry import Point, shape
import httpx
import redis
import json
import os
import re
from typing import Any
import logging
import time
import uuid
from pydantic import BaseModel
from collections import deque
from datetime import datetime, timezone

# ==============================================================================
# CONFIGURAÇÃO
# ==============================================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR_DADOS = os.getenv("DATA_DIR", os.path.join(BASE_DIR, "dados_processados"))

ARQUIVO_SETORES = os.path.join(DIR_DADOS, "setores_com_renda.parquet")
ARQUIVO_RUAS    = os.path.join(DIR_DADOS, "ruas_rj_com_renda.parquet")

REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_TTL_DIAS  = 30  # CEPs ficam em cache por 30 dias
DATASET_VERSION = os.getenv("DATASET_VERSION", "censo-2022-v1")
CENSO_ANO = int(os.getenv("CENSO_ANO", "2022"))
ALLOWED_ESTADOS = tuple(
    estado.strip().upper()
    for estado in os.getenv("ALLOWED_ESTADOS", "RJ,SP").split(",")
    if estado.strip()
)
ENFORCE_ALLOWED_ESTADOS = os.getenv("ENFORCE_ALLOWED_ESTADOS", "1") == "1"
RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "1") == "1"
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))
RATE_LIMIT_MAX_REQUESTS = int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "30"))
RATE_LIMIT_PATH_PREFIXES = tuple(
    prefix.strip()
    for prefix in os.getenv("RATE_LIMIT_PATH_PREFIXES", "/renda/cep,/renda/ponto").split(",")
    if prefix.strip()
)

REQUIRED_SETORES_COLUMNS = {"code_tract", "renda_media", "geometry"}
_rate_limit_buckets: dict[str, deque[float]] = {}

logger = logging.getLogger("geomarketing.api")
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

# ==============================================================================
# LIFESPAN — carrega dados UMA vez na inicialização
# ==============================================================================

gdf_setores = None
gdf_ruas    = None
redis_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global gdf_setores, gdf_ruas, redis_client

    logger.info("startup.begin base_dir=%s data_dir=%s", BASE_DIR, DIR_DADOS)

    # Redis
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        logger.info("startup.redis ok=true")
    except Exception as e:
        logger.warning("startup.redis ok=false error=%s", e)
        redis_client = None

    # Setores censitários
    if os.path.exists(ARQUIVO_SETORES):
        try:
            gdf_setores = gpd.read_parquet(ARQUIVO_SETORES)
            if gdf_setores.crs.to_epsg() != 3857:
                gdf_setores = gdf_setores.to_crs(epsg=3857)
            logger.info("startup.setores ok=true registros=%s", len(gdf_setores))
        except Exception as e:
            logger.exception("startup.setores ok=false error=%s", e)
    else:
        logger.warning("startup.setores file_missing=true path=%s", ARQUIVO_SETORES)

    # Ruas
    if os.path.exists(ARQUIVO_RUAS):
        try:
            gdf_ruas = gpd.read_parquet(ARQUIVO_RUAS)
            logger.info("startup.ruas ok=true registros=%s", len(gdf_ruas))
        except Exception as e:
            logger.exception("startup.ruas ok=false error=%s", e)
    else:
        logger.warning("startup.ruas file_missing=true path=%s", ARQUIVO_RUAS)

    logger.info("startup.ready")
    yield  # A API roda aqui
    logger.info("startup.shutdown")


# ==============================================================================
# APP
# ==============================================================================

app = FastAPI(
    title="GeoMarketing RJ",
    version="3.0",
    description="Inteligência geoespacial socioeconômica para o município do Rio de Janeiro.",
    lifespan=lifespan,
)


class HealthDataComponent(BaseModel):
    ok: bool
    arquivo: str
    registros: int


class HealthRedisComponent(BaseModel):
    ok: bool
    url: str


class HealthComponents(BaseModel):
    setores: HealthDataComponent
    ruas: HealthDataComponent
    redis: HealthRedisComponent


class HealthResponse(BaseModel):
    status: str
    ready: bool
    componentes: HealthComponents
    metadados: "MetadadosResponse"


class EnderecoResponse(BaseModel):
    logradouro: str
    bairro: str
    cidade: str
    estado: str
    latitude: float
    longitude: float
    geometria: dict[str, Any] | None = None
    geometria_tipo: str | None = None


class ResumoResponse(BaseModel):
    total_setores: int
    renda_mediana: float
    renda_media_minima: float
    renda_media_maxima: float
    classe_predominante: str


class SetorResponse(BaseModel):
    setor_censitario: str
    renda_media: float
    classe_social_estimada: str


class QualidadeResponse(BaseModel):
    geocoding_score: float
    confianca_geocoding: str
    confianca_estimativa: str
    geometria_tipo: str
    total_setores: int


class MetadadosResponse(BaseModel):
    dataset_version: str
    censo_ano: int
    base_setores_atualizada_em: str | None
    base_ruas_atualizada_em: str | None
    rate_limit: dict[str, Any]


class RendaCepResponse(BaseModel):
    cep: str
    endereco: EnderecoResponse
    mensagem: str | None = None
    resumo: ResumoResponse | None = None
    setores: list[SetorResponse] | None = None
    geojson: dict[str, Any] | None = None
    fonte_geocoding: str | None = None
    qualidade: QualidadeResponse
    metadados: MetadadosResponse


def _request_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _is_rate_limited(request: Request) -> tuple[bool, int]:
    if not RATE_LIMIT_ENABLED:
        return False, 0
    if request.method != "GET":
        return False, 0
    if not any(request.url.path.startswith(prefix) for prefix in RATE_LIMIT_PATH_PREFIXES):
        return False, 0

    ip = _request_ip(request)
    bucket_key = f"{ip}:{request.url.path.split('/', 3)[1]}"
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW_SECONDS

    bucket = _rate_limit_buckets.setdefault(bucket_key, deque())
    while bucket and bucket[0] < cutoff:
        bucket.popleft()

    if len(bucket) >= RATE_LIMIT_MAX_REQUESTS:
        retry_after = max(1, int(bucket[0] + RATE_LIMIT_WINDOW_SECONDS - now))
        return True, retry_after

    bucket.append(now)
    return False, 0


def _file_mtime_iso(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    try:
        mtime = os.path.getmtime(path)
        return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _dataset_metadata() -> dict:
    return {
        "dataset_version": DATASET_VERSION,
        "censo_ano": CENSO_ANO,
        "base_setores_atualizada_em": _file_mtime_iso(ARQUIVO_SETORES),
        "base_ruas_atualizada_em": _file_mtime_iso(ARQUIVO_RUAS),
        "rate_limit": {
            "enabled": RATE_LIMIT_ENABLED,
            "window_seconds": RATE_LIMIT_WINDOW_SECONDS,
            "max_requests": RATE_LIMIT_MAX_REQUESTS,
            "path_prefixes": list(RATE_LIMIT_PATH_PREFIXES),
        },
    }


@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = request.headers.get("x-request-id", uuid.uuid4().hex)
    start = time.perf_counter()

    logger.info(
        "request.begin request_id=%s method=%s path=%s",
        request_id,
        request.method,
        request.url.path,
    )

    limited, retry_after = _is_rate_limited(request)
    if limited:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.warning(
            "request.rate_limited request_id=%s ip=%s path=%s retry_after=%s duration_ms=%.2f",
            request_id,
            _request_ip(request),
            request.url.path,
            retry_after,
            elapsed_ms,
        )
        return JSONResponse(
            status_code=429,
            content={
                "detail": (
                    "Muitas requisições para este endpoint. "
                    "Tente novamente em alguns instantes."
                )
            },
            headers={
                "Retry-After": str(retry_after),
                "X-Request-ID": request_id,
            },
        )

    try:
        response = await call_next(request)
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.exception(
            "request.error request_id=%s method=%s path=%s duration_ms=%.2f error=%s",
            request_id,
            request.method,
            request.url.path,
            elapsed_ms,
            exc,
        )
        raise

    elapsed_ms = (time.perf_counter() - start) * 1000
    response.headers["X-Request-ID"] = request_id
    logger.info(
        "request.end request_id=%s method=%s path=%s status=%s duration_ms=%.2f",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response


# ==============================================================================
# HELPERS
# ==============================================================================

def _limpar_cep(cep: str) -> str:
    """Remove traços e espaços do CEP."""
    return re.sub(r"\D", "", cep)


def _classificar_renda(renda: float) -> str:
    """Classifica a renda em faixas baseadas no Critério Brasil."""
    if renda > 22000:
        return "A"
    elif renda > 7100:
        return "B"
    elif renda > 2900:
        return "C"
    return "D/E"


def _buscar_coordenadas_cache(cep: str) -> dict | None:
    """Tenta retornar coordenadas do cache Redis."""
    if redis_client is None:
        return None
    try:
        valor = redis_client.get(f"cep:{cep}")
        if valor:
            return json.loads(valor)
    except Exception as e:
        logger.warning("cache.get error cep=%s detail=%s", cep, e)
    return None


def _salvar_coordenadas_cache(cep: str, dados: dict) -> None:
    """Salva coordenadas no Redis com TTL de 30 dias."""
    if redis_client is None:
        return
    try:
        redis_client.setex(
            f"cep:{cep}",
            REDIS_TTL_DIAS * 86400,
            json.dumps(dados),
        )
    except Exception as e:
        logger.warning("cache.set error cep=%s detail=%s", cep, e)


def _normalizar_texto(valor: Any) -> str:
    """Normaliza texto para comparações tolerantes entre provedores de geocoding."""
    if valor is None:
        return ""
    return str(valor).strip().lower()


def _classificar_confianca_geocoding(score: float, tipo_geom: str) -> str:
    """Classifica confiança do geocoding para comunicação no MVP."""
    if tipo_geom == "Point" and score >= 75:
        return "media"
    if score >= 75:
        return "alta"
    if score >= 50:
        return "media"
    return "baixa"


def _classificar_confianca_estimativa(total_setores: int, tipo_geom: str) -> str:
    """Classifica confiança da estimativa de renda conforme cobertura espacial."""
    if total_setores >= 5 and tipo_geom != "Point":
        return "alta"
    if total_setores >= 2:
        return "media"
    return "baixa"


def _pontuar_resultado_nominatim(hit: dict, dados_cep: dict, cep: str) -> float:
    """Calcula score de confiança para escolher o melhor hit do Nominatim."""
    score = 0.0

    geojson_geom = hit.get("geojson", {})
    tipo_geom = geojson_geom.get("type", "")
    if tipo_geom in ("LineString", "MultiLineString", "Polygon", "MultiPolygon"):
        score += 40.0
    elif tipo_geom == "Point":
        score += 15.0

    importance = hit.get("importance")
    if isinstance(importance, (int, float)):
        score += float(importance) * 10.0

    place_rank = hit.get("place_rank")
    if isinstance(place_rank, (int, float)):
        score += min(float(place_rank), 30.0) / 3.0

    address = hit.get("address", {}) if isinstance(hit.get("address"), dict) else {}

    cep_hit = _normalizar_texto(address.get("postcode", ""))
    cep_alvo = _normalizar_texto(dados_cep.get("cep", cep))
    if cep_hit and cep_alvo and _limpar_cep(cep_hit) == _limpar_cep(cep_alvo):
        score += 20.0

    cidade_alvo = _normalizar_texto(dados_cep.get("localidade", ""))
    cidade_hit = _normalizar_texto(
        address.get("city") or address.get("town") or address.get("municipality")
    )
    if cidade_alvo and cidade_hit and cidade_alvo == cidade_hit:
        score += 12.0

    uf_alvo = _normalizar_texto(dados_cep.get("uf", ""))
    estado_hit = _normalizar_texto(address.get("state", ""))
    if uf_alvo and estado_hit and uf_alvo in estado_hit:
        score += 8.0

    bairro_alvo = _normalizar_texto(dados_cep.get("bairro", ""))
    bairro_hit = _normalizar_texto(address.get("suburb") or address.get("neighbourhood"))
    if bairro_alvo and bairro_hit and bairro_alvo in bairro_hit:
        score += 5.0

    return score


def _selecionar_melhor_hit_nominatim(resultados: list[dict], dados_cep: dict, cep: str) -> tuple[dict, float]:
    """Escolhe o hit mais confiável usando score + fallback por ordem de retorno."""
    if not resultados:
        raise HTTPException(
            status_code=404,
            detail=f"Não foi possível geocodificar o CEP {cep}.",
        )

    melhor_hit = resultados[0]
    melhor_score = _pontuar_resultado_nominatim(melhor_hit, dados_cep, cep)

    for hit in resultados[1:]:
        score = _pontuar_resultado_nominatim(hit, dados_cep, cep)
        if score > melhor_score:
            melhor_hit = hit
            melhor_score = score

    return melhor_hit, melhor_score


async def _geocodificar_cep(cep: str) -> dict:
    """
    Converte CEP em coordenadas geográficas.
    Fluxo: Cache Redis → ViaCEP (endereço) → Nominatim (geometria exata)
    """
    # 1. Cache
    cache = _buscar_coordenadas_cache(cep)
    if cache:
        tipo_geom_cache = cache.get("geometria_tipo", "Point")
        score_cache = float(cache.get("geocoding_score", 40.0 if tipo_geom_cache == "Point" else 75.0))
        cache["fonte"] = "cache"
        cache["geocoding_score"] = score_cache
        cache["confianca_geocoding"] = cache.get("confianca_geocoding") or _classificar_confianca_geocoding(score_cache, tipo_geom_cache)
        logger.info("geocoding.cache_hit cep=%s", cep)
        return cache

    logger.info("geocoding.start cep=%s", cep)

    async with httpx.AsyncClient(timeout=10.0) as client:

        # 2. ViaCEP → endereço
        try:
            resp = await client.get(f"https://viacep.com.br/ws/{cep}/json/")
            resp.raise_for_status()
            dados_cep = resp.json()
        except Exception as e:
            logger.warning("geocoding.viacep_error cep=%s detail=%s", cep, e)
            raise HTTPException(status_code=502, detail="Erro ao consultar ViaCEP.")

        if "erro" in dados_cep:
            raise HTTPException(status_code=404, detail=f"CEP {cep} não encontrado.")

        logradouro = dados_cep.get("logradouro", "")
        bairro     = dados_cep.get("bairro", "")
        cidade     = dados_cep.get("localidade", "Rio de Janeiro")
        estado     = dados_cep.get("uf", "RJ")

        # 3. Nominatim → geometria completa (polygon_geojson=1)
        query = f"{logradouro}, {bairro}, {cidade}, {estado}, Brasil" if logradouro else f"{bairro}, {cidade}, {estado}, Brasil"
        headers = {"User-Agent": "geomarketing-rj/3.0"}
        try:
            resp = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={
                    "q":              query,
                    "format":         "json",
                    "countrycodes":   "br",
                    "addressdetails": 1,
                    "limit":          5,
                    "polygon_geojson": 1,   # pede geometria completa
                },
                headers=headers,
            )
            resp.raise_for_status()
            resultados = resp.json()
        except Exception as e:
            logger.warning("geocoding.nominatim_error cep=%s detail=%s", cep, e)
            raise HTTPException(status_code=502, detail="Erro ao consultar Nominatim.")

        hit, geocoding_score = _selecionar_melhor_hit_nominatim(resultados, dados_cep, cep)

        geojson_geom = hit.get("geojson", {})
        tipo_geom    = geojson_geom.get("type", "")

        # Extrai geometria completa para o cálculo e o centroide apenas para a câmera do mapa
        if tipo_geom in ("LineString", "MultiLineString", "Polygon", "MultiPolygon"):
            geom   = shape(geojson_geom)
            centro = geom.centroid
            lat    = centro.y
            lng    = centro.x
        else:
            # Fallback forçado para Point caso o OSM só tenha um nó
            lat = float(hit["lat"])
            lng = float(hit["lon"])
            geojson_geom = {"type": "Point", "coordinates": [lng, lat]}
            tipo_geom = "Point"

    resultado = {
        "cep":            cep,
        "logradouro":     logradouro,
        "bairro":         bairro,
        "cidade":         cidade,
        "estado":         estado,
        "latitude":       lat,  # Serve apenas para centralizar a câmera no frontend
        "longitude":      lng,  # Serve apenas para centralizar a câmera no frontend
        "geometria":      geojson_geom, # <-- GEOMETRIA COMPLETA SALVA AQUI
        "geometria_tipo": tipo_geom,
        "geocoding_score": round(float(geocoding_score), 2),
        "confianca_geocoding": _classificar_confianca_geocoding(float(geocoding_score), tipo_geom),
        "fonte":          "api",
    }

    _salvar_coordenadas_cache(cep, resultado)
    logger.info("geocoding.success cep=%s geometria_tipo=%s", cep, tipo_geom)
    return resultado


BUFFER_METROS = 50  # raio de busca ao redor da linha ou ponto geocodificado


def _consultar_setor(lat: float, lng: float) -> dict | None:
    """Point-in-polygon simples — usado pelo endpoint /renda/ponto."""
    ponto = gpd.GeoDataFrame(geometry=[Point(lng, lat)], crs="EPSG:4326")
    ponto = ponto.to_crs(gdf_setores.crs)
    resultado = gpd.sjoin(ponto, gdf_setores, how="left", predicate="within")

    if resultado.empty or pd.isna(resultado.iloc[0]["renda_media"]):
        return None

    dado = resultado.iloc[0]
    renda = float(dado["renda_media"])

    return {
        "setor_censitario": str(dado["code_tract"]),
        "renda_media": round(renda, 2),
        "classe_social_estimada": _classificar_renda(renda),
    }


def _consultar_setores_por_buffer(geometria_geojson: dict) -> list[dict]:
    """
    Retorna todos os setores censitários que tocam o buffer da geometria da rua.
    Resolve o caso de CEPs longos transformando a LineString num polígono e intersecionando.
    """
    # 1. Transforma o dict GeoJSON em objeto Shapely (LineString ou Point)
    try:
        geom_shapely = shape(geometria_geojson)
    except Exception as e:
        logger.warning("setores.buffer invalid_geometry detail=%s", e)
        return []
    
    # 2. Cria o GeoDataFrame a partir da geometria original em EPSG:4326
    gdf_geom = gpd.GeoDataFrame(geometry=[geom_shapely], crs="EPSG:4326")
    
    # 3. Converte para o CRS dos setores (geralmente UTM/Mercator - metros)
    gdf_geom = gdf_geom.to_crs(gdf_setores.crs)

    # 4. Cria o Buffer em metros. Se for Point vira círculo. Se for LineString, vira a "salsicha".
    buffer_geom = gpd.GeoDataFrame(geometry=gdf_geom.buffer(BUFFER_METROS), crs=gdf_setores.crs)

    # 5. Spatial Join (cruza o buffer com o grid do IBGE)
    resultado = gpd.sjoin(buffer_geom, gdf_setores, how="left", predicate="intersects")
    resultado = resultado.dropna(subset=["renda_media"])

    if resultado.empty:
        logger.info("setores.buffer empty_result=true")
        return []

    # Remove possíveis duplicatas caso o buffer toque o mesmo setor mais de uma vez
    resultado = resultado.drop_duplicates(subset=["code_tract"])
    logger.info("setores.buffer total_setores=%s", len(resultado))

    setores = []
    for _, dado in resultado.iterrows():
        renda = float(dado["renda_media"])
        setores.append({
            "setor_censitario": str(dado["code_tract"]),
            "renda_media": round(renda, 2),
            "classe_social_estimada": _classificar_renda(renda),
        })

    return setores


def _setores_para_geojson(code_tracts: list[str]) -> dict:
    """
    Retorna um GeoJSON FeatureCollection com os polígonos de todos os setores
    informados. Reprojetado para EPSG:4326 para uso direto no MapLibre.
    """
    filtro = gdf_setores[gdf_setores["code_tract"].isin(code_tracts)]
    if filtro.empty:
        return {"type": "FeatureCollection", "features": []}
    # Simplifica geometria antes de serializar — reduz tamanho do GeoJSON
    filtro = filtro.copy()
    filtro["geometry"] = filtro.geometry.simplify(tolerance=10, preserve_topology=True)
    return json.loads(filtro.to_crs(epsg=4326).to_json())


def _health_snapshot() -> dict:
    """Retorna o estado de saúde dos componentes principais da API."""
    setores_ok = (
        gdf_setores is not None
        and not gdf_setores.empty
        and REQUIRED_SETORES_COLUMNS.issubset(set(gdf_setores.columns))
    )
    ruas_ok = gdf_ruas is not None and not gdf_ruas.empty

    redis_ok = False
    if redis_client is not None:
        try:
            redis_ok = bool(redis_client.ping())
        except Exception:
            redis_ok = False

    return {
        "ready": setores_ok,
        "componentes": {
            "setores": {
                "ok": setores_ok,
                "arquivo": ARQUIVO_SETORES,
                "registros": len(gdf_setores) if gdf_setores is not None else 0,
            },
            "ruas": {
                "ok": ruas_ok,
                "arquivo": ARQUIVO_RUAS,
                "registros": len(gdf_ruas) if gdf_ruas is not None else 0,
            },
            "redis": {
                "ok": redis_ok,
                "url": REDIS_URL,
            },
        },
    }


# ==============================================================================
# ENDPOINTS
# ==============================================================================

@app.get("/", summary="Status da API")
def home():
    health = _health_snapshot()
    return {
        "status": "online",
        "ready": health["ready"],
        "versao": "3.2",
        "dados": {
            "setores": len(gdf_setores) if gdf_setores is not None else 0,
            "ruas": len(gdf_ruas) if gdf_ruas is not None else 0,
        },
        "endpoints": [
            "/health",
            "/renda/cep/{cep}",
            "/renda/ponto/{lat}/{lng}",
            "/ruas/melhores?renda_minima=10000",
        ],
        "metadados": _dataset_metadata(),
    }


@app.get("/health", summary="Readiness da API", response_model=HealthResponse)
def healthcheck():
    health = _health_snapshot()
    if not health["ready"]:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "degradado",
                "motivo": "Base de setores não carregada ou inválida.",
                **health,
            },
        )

    return {"status": "ok", **health, "metadados": _dataset_metadata()}


@app.get(
    "/renda/cep/{cep}",
    summary="Consulta renda por CEP",
    response_model=RendaCepResponse,
    response_model_exclude_none=True,
)
async def consultar_cep(cep: str):
    """
    Recebe um CEP, geocodifica buscando a extensão da rua e retorna os dados
    socioeconômicos dos setores censitários englobados por ela.
    """
    cep_limpo = _limpar_cep(cep)
    if len(cep_limpo) != 8:
        raise HTTPException(status_code=422, detail="CEP inválido. Use 8 dígitos.")

    if gdf_setores is None:
        raise HTTPException(status_code=503, detail="Base de setores não carregada.")

    # Geocoding (com cache)
    geo = await _geocodificar_cep(cep_limpo)
    geometria_tipo = geo.get("geometria_tipo", "Point")
    geocoding_score = float(geo.get("geocoding_score", 40.0 if geometria_tipo == "Point" else 75.0))
    confianca_geocoding = geo.get("confianca_geocoding") or _classificar_confianca_geocoding(geocoding_score, geometria_tipo)

    if ENFORCE_ALLOWED_ESTADOS and geo.get("estado", "").upper() not in ALLOWED_ESTADOS:
        raise HTTPException(
            status_code=422,
            detail=(
                "No ambiente atual, apenas CEPs dos estados "
                f"{', '.join(ALLOWED_ESTADOS)} são suportados."
            ),
        )

    # Busca todos os setores no raio passando a GEOMETRIA COMPLETA
    setores = _consultar_setores_por_buffer(geo["geometria"])

    if not setores:
        return {
            "cep": cep_limpo,
            "endereco": geo,
            "mensagem": "Endereço fora da área mapeada ou sem dados censitários.",
            "fonte_geocoding": geo["fonte"],
            "metadados": _dataset_metadata(),
            "qualidade": {
                "geocoding_score": round(geocoding_score, 2),
                "confianca_geocoding": confianca_geocoding,
                "confianca_estimativa": "baixa",
                "geometria_tipo": geometria_tipo,
                "total_setores": 0,
            },
        }

    rendas = [s["renda_media"] for s in setores]
    renda_mediana = statistics.median(rendas)
    code_tracts = [s["setor_censitario"] for s in setores]
    confianca_estimativa = _classificar_confianca_estimativa(len(setores), geometria_tipo)

    return {
        "cep": cep_limpo,
        "endereco": {
            "logradouro":     geo["logradouro"],
            "bairro":         geo["bairro"],
            "cidade":         geo["cidade"],
            "estado":         geo["estado"],
            "latitude":       geo["latitude"],
            "longitude":      geo["longitude"],
            "geometria":      geo["geometria"],      # Repassa pro frontend desenhar a rua
            "geometria_tipo": geo["geometria_tipo"],
        },
        "resumo": {
            "total_setores":           len(setores),
            "renda_mediana":           round(renda_mediana, 2), # <--- NOVO NÚMERO PRINCIPAL
            "renda_media_minima":      round(min(rendas), 2),
            "renda_media_maxima":      round(max(rendas), 2),
            "classe_predominante":     _classificar_renda(renda_mediana), # Classifica baseado na mediana
        },
        "setores":  setores,
        "geojson":  _setores_para_geojson(code_tracts), # Polígonos dos setores
        "fonte_geocoding": geo["fonte"],
        "metadados": _dataset_metadata(),
        "qualidade": {
            "geocoding_score": round(geocoding_score, 2),
            "confianca_geocoding": confianca_geocoding,
            "confianca_estimativa": confianca_estimativa,
            "geometria_tipo": geometria_tipo,
            "total_setores": len(setores),
        },
    }


@app.get("/renda/ponto/{lat}/{lng}", summary="Consulta renda por coordenadas")
def consultar_ponto(lat: float, lng: float):
    """
    Recebe lat/lng e retorna os dados do setor censitário correspondente.
    """
    if gdf_setores is None:
        raise HTTPException(status_code=503, detail="Base de setores não carregada.")

    setor = _consultar_setor(lat, lng)
    if setor is None:
        return {
            "latitude": lat,
            "longitude": lng,
            "mensagem": "Local fora da área mapeada ou sem dados.",
        }

    return {"latitude": lat, "longitude": lng, **setor}


@app.get("/ruas/melhores", summary="Ranking de ruas por renda mínima")
def recomendar_ruas(renda_minima: float = 5000, top: int = 10):
    """
    Lista as ruas com renda média acima do valor informado, ordenadas pela maior renda.
    """
    if gdf_ruas is None:
        raise HTTPException(status_code=503, detail="Base de ruas não carregada.")

    filtro = gdf_ruas[gdf_ruas["renda_media"] >= renda_minima].copy()

    if filtro.empty:
        return {"msg": f"Nenhuma rua encontrada com renda acima de R$ {renda_minima:.2f}"}

    filtro["name_clean"] = (
        filtro["name"].astype(str).str.replace(r"[\[\]']", "", regex=True)
    )
    ranking = (
        filtro.groupby("name_clean")["renda_media"]
        .mean()
        .sort_values(ascending=False)
        .head(top)
    )

    return {
        "filtro_renda_minima": renda_minima,
        "total_segmentos_encontrados": len(filtro),
        "top_recomendacoes": [
            {"rua": rua, "renda_media": round(renda, 2)}
            for rua, renda in ranking.items()
            if rua != "nan"
        ],
    }