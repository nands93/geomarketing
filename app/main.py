from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import httpx
import redis
import json
import os
import re

# ==============================================================================
# CONFIGURAÇÃO
# ==============================================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR_DADOS = os.path.join(BASE_DIR, "dados_processados")

ARQUIVO_SETORES = os.path.join(DIR_DADOS, "setores_com_renda.parquet")
ARQUIVO_RUAS    = os.path.join(DIR_DADOS, "ruas_rj_com_renda.parquet")

REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_TTL_DIAS  = 30  # CEPs ficam em cache por 30 dias

# ==============================================================================
# LIFESPAN — carrega dados UMA vez na inicialização (modo correto no FastAPI moderno)
# ==============================================================================

gdf_setores = None
gdf_ruas    = None
redis_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global gdf_setores, gdf_ruas, redis_client

    print(f"⏳ Iniciando API... Raiz do projeto: {BASE_DIR}")

    # Redis
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        print("   ✅ Redis conectado.")
    except Exception as e:
        print(f"   ⚠️ Redis indisponível — geocoding sem cache: {e}")
        redis_client = None

    # Setores censitários
    if os.path.exists(ARQUIVO_SETORES):
        try:
            gdf_setores = gpd.read_parquet(ARQUIVO_SETORES)
            if gdf_setores.crs.to_epsg() != 3857:
                gdf_setores = gdf_setores.to_crs(epsg=3857)
            print(f"   ✅ Setores carregados: {len(gdf_setores)} polígonos.")
        except Exception as e:
            print(f"   ❌ Erro ao carregar setores: {e}")
    else:
        print(f"   ⚠️ Arquivo de setores não encontrado: {ARQUIVO_SETORES}")

    # Ruas
    if os.path.exists(ARQUIVO_RUAS):
        try:
            gdf_ruas = gpd.read_parquet(ARQUIVO_RUAS)
            print(f"   ✅ Ruas carregadas: {len(gdf_ruas)} segmentos.")
        except Exception as e:
            print(f"   ❌ Erro ao carregar ruas: {e}")
    else:
        print(f"   ⚠️ Arquivo de ruas não encontrado: {ARQUIVO_RUAS}")

    print("🚀 API Online!")
    yield  # A API roda aqui
    print("🛑 Encerrando API.")


# ==============================================================================
# APP
# ==============================================================================

app = FastAPI(
    title="GeoMarketing RJ",
    version="3.0",
    description="Inteligência geoespacial socioeconômica para o município do Rio de Janeiro.",
    lifespan=lifespan,
)


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
    except Exception:
        pass
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
    except Exception:
        pass


async def _geocodificar_cep(cep: str) -> dict:
    """
    Converte CEP em coordenadas geográficas.
    Fluxo: Cache Redis → ViaCEP (endereço) → Nominatim (lat/lng)
    """
    # 1. Cache
    cache = _buscar_coordenadas_cache(cep)
    if cache:
        cache["fonte"] = "cache"
        return cache

    async with httpx.AsyncClient(timeout=10.0) as client:

        # 2. ViaCEP → endereço
        try:
            resp = await client.get(f"https://viacep.com.br/ws/{cep}/json/")
            resp.raise_for_status()
            dados_cep = resp.json()
        except Exception:
            raise HTTPException(status_code=502, detail="Erro ao consultar ViaCEP.")

        if "erro" in dados_cep:
            raise HTTPException(status_code=404, detail=f"CEP {cep} não encontrado.")

        logradouro = dados_cep.get("logradouro", "")
        bairro     = dados_cep.get("bairro", "")
        cidade     = dados_cep.get("localidade", "Rio de Janeiro")
        estado     = dados_cep.get("uf", "RJ")

        # 3. Nominatim → lat/lng
        query = f"{logradouro}, {bairro}, {cidade}, {estado}, Brasil"
        headers = {"User-Agent": "geomarketing-rj/3.0"}
        try:
            resp = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": query, "format": "json", "limit": 1},
                headers=headers,
            )
            resp.raise_for_status()
            resultados = resp.json()
        except Exception:
            raise HTTPException(status_code=502, detail="Erro ao consultar Nominatim.")

        if not resultados:
            raise HTTPException(
                status_code=404,
                detail=f"Não foi possível geocodificar o CEP {cep}.",
            )

        lat = float(resultados[0]["lat"])
        lng = float(resultados[0]["lon"])

    resultado = {
        "cep": cep,
        "logradouro": logradouro,
        "bairro": bairro,
        "cidade": cidade,
        "estado": estado,
        "latitude": lat,
        "longitude": lng,
        "fonte": "api",
    }

    _salvar_coordenadas_cache(cep, resultado)
    return resultado


def _consultar_setor(lat: float, lng: float) -> dict:
    """Point-in-polygon: retorna dados do setor censitário para um ponto."""
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


# ==============================================================================
# ENDPOINTS
# ==============================================================================

@app.get("/", summary="Status da API")
def home():
    return {
        "status": "online",
        "versao": "3.0",
        "municipio": "Rio de Janeiro",
        "dados": {
            "setores": len(gdf_setores) if gdf_setores is not None else 0,
            "ruas": len(gdf_ruas) if gdf_ruas is not None else 0,
        },
        "endpoints": [
            "/renda/cep/{cep}",
            "/renda/ponto/{lat}/{lng}",
            "/ruas/melhores?renda_minima=10000",
        ],
    }


@app.get("/renda/cep/{cep}", summary="Consulta renda por CEP")
async def consultar_cep(cep: str):
    """
    Recebe um CEP (com ou sem traço), geocodifica e retorna os dados
    socioeconômicos do setor censitário correspondente.
    Cache Redis de 30 dias para geocoding.
    """
    cep_limpo = _limpar_cep(cep)
    if len(cep_limpo) != 8:
        raise HTTPException(status_code=422, detail="CEP inválido. Use 8 dígitos.")

    if gdf_setores is None:
        raise HTTPException(status_code=503, detail="Base de setores não carregada.")

    # Geocoding (com cache)
    geo = await _geocodificar_cep(cep_limpo)

    # Point-in-polygon
    setor = _consultar_setor(geo["latitude"], geo["longitude"])
    if setor is None:
        return {
            "cep": cep_limpo,
            "endereco": geo,
            "mensagem": "Endereço fora da área mapeada ou sem dados censitários.",
        }

    return {
        "cep": cep_limpo,
        "endereco": {
            "logradouro": geo["logradouro"],
            "bairro": geo["bairro"],
            "cidade": geo["cidade"],
            "latitude": geo["latitude"],
            "longitude": geo["longitude"],
        },
        "dados_censitarios": setor,
        "fonte_geocoding": geo["fonte"],  # "cache" ou "api"
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