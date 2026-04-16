"""
Testes básicos da GeoMarketing API.
Execute com: pytest tests/ -v
"""
import json
from pathlib import Path
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock

# Importa o app — os dados não precisam estar carregados para testar a estrutura
from app.main import app
import app.main as main_module

client = TestClient(app)
GOLDEN_CEPS_FILE = Path(__file__).with_name("golden_ceps.json")
GOLDEN_CASES = json.loads(GOLDEN_CEPS_FILE.read_text(encoding="utf-8"))


# ==============================================================================
# TESTES DE SAÚDE
# ==============================================================================

def test_home_retorna_status_online():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "online"


def test_home_lista_endpoints():
    response = client.get("/")
    endpoints = response.json()["endpoints"]
    assert "/renda/cep/{cep}" in endpoints
    assert "/renda/ponto/{lat}/{lng}" in endpoints
    assert "/ruas/melhores?renda_minima=10000" in endpoints


def test_home_expoe_metadados_de_rate_limit():
    response = client.get("/")
    assert response.status_code == 200
    metadados = response.json()["metadados"]
    assert "rate_limit" in metadados
    assert "enabled" in metadados["rate_limit"]
    assert "window_seconds" in metadados["rate_limit"]
    assert "max_requests" in metadados["rate_limit"]


# ==============================================================================
# TESTES DE VALIDAÇÃO DE INPUT
# ==============================================================================

def test_cep_invalido_retorna_422():
    response = client.get("/renda/cep/123")  # CEP curto demais
    assert response.status_code == 422


def test_cep_com_letras_retorna_422():
    response = client.get("/renda/cep/ABCDEFGH")
    assert response.status_code == 422


def test_ponto_sem_dados_retorna_mensagem():
    """Coordenadas no meio do oceano — fora da área mapeada."""
    response = client.get("/renda/ponto/-23.0/-50.0")
    # Se setores não estão carregados no teste, espera 503
    # Se estão, espera mensagem de fora da área
    assert response.status_code in [200, 503]
    if response.status_code == 200:
        assert "mensagem" in response.json()


# ==============================================================================
# TESTES DE RUAS
# ==============================================================================

def test_ruas_sem_dados_retorna_503():
    """Sem o parquet de ruas carregado, deve retornar 503."""
    response = client.get("/ruas/melhores?renda_minima=5000")
    # Pode ser 200 (se dados carregados) ou 503 (se não)
    assert response.status_code in [200, 503]


def test_ruas_parametros_padrao():
    response = client.get("/ruas/melhores")
    assert response.status_code in [200, 503]


def test_ruas_top_parametro():
    response = client.get("/ruas/melhores?renda_minima=10000&top=5")
    assert response.status_code in [200, 503]
    if response.status_code == 200 and "top_recomendacoes" in response.json():
        assert len(response.json()["top_recomendacoes"]) <= 5


# ==============================================================================
# TESTES DETERMINÍSTICOS DO MVP (CEP -> MEDIANA)
# ==============================================================================

def test_cep_calcula_renda_mediana_com_mocks(monkeypatch):
    monkeypatch.setattr(main_module, "gdf_setores", object())

    geocoding_mock = AsyncMock(return_value={
        "cep": "22071100",
        "logradouro": "Rua Exemplo",
        "bairro": "Copacabana",
        "cidade": "Rio de Janeiro",
        "estado": "RJ",
        "latitude": -22.97,
        "longitude": -43.18,
        "geometria": {"type": "Point", "coordinates": [-43.18, -22.97]},
        "geometria_tipo": "Point",
        "geocoding_score": 61.4,
        "confianca_geocoding": "media",
        "fonte": "api",
    })
    monkeypatch.setattr(main_module, "_geocodificar_cep", geocoding_mock)

    monkeypatch.setattr(
        main_module,
        "_consultar_setores_por_buffer",
        lambda _: [
            {"setor_censitario": "1", "renda_media": 3000.0, "classe_social_estimada": "C"},
            {"setor_censitario": "2", "renda_media": 5000.0, "classe_social_estimada": "C"},
            {"setor_censitario": "3", "renda_media": 9000.0, "classe_social_estimada": "B"},
        ],
    )
    monkeypatch.setattr(
        main_module,
        "_setores_para_geojson",
        lambda _: {"type": "FeatureCollection", "features": []},
    )

    response = client.get("/renda/cep/22071-100")
    assert response.status_code == 200
    body = response.json()

    assert body["resumo"]["renda_mediana"] == 5000.0
    assert body["resumo"]["renda_media_minima"] == 3000.0
    assert body["resumo"]["renda_media_maxima"] == 9000.0
    assert body["resumo"]["classe_predominante"] == "C"
    assert body["fonte_geocoding"] == "api"
    assert body["qualidade"]["confianca_geocoding"] == "media"
    assert body["qualidade"]["total_setores"] == 3
    assert body["metadados"]["censo_ano"] == 2022
    assert "dataset_version" in body["metadados"]


def test_cep_retorna_mensagem_quando_sem_setores(monkeypatch):
    monkeypatch.setattr(main_module, "gdf_setores", object())

    geocoding_mock = AsyncMock(return_value={
        "cep": "22071100",
        "logradouro": "Rua Exemplo",
        "bairro": "Copacabana",
        "cidade": "Rio de Janeiro",
        "estado": "RJ",
        "latitude": -22.97,
        "longitude": -43.18,
        "geometria": {"type": "Point", "coordinates": [-43.18, -22.97]},
        "geometria_tipo": "Point",
        "geocoding_score": 48.2,
        "confianca_geocoding": "baixa",
        "fonte": "cache",
    })
    monkeypatch.setattr(main_module, "_geocodificar_cep", geocoding_mock)
    monkeypatch.setattr(main_module, "_consultar_setores_por_buffer", lambda _: [])

    response = client.get("/renda/cep/22071100")
    assert response.status_code == 200
    body = response.json()
    assert "mensagem" in body
    assert body["cep"] == "22071100"
    assert body["qualidade"]["confianca_estimativa"] == "baixa"
    assert body["qualidade"]["total_setores"] == 0


def test_cep_sp_aceito_no_mvp(monkeypatch):
    monkeypatch.setattr(main_module, "gdf_setores", object())
    monkeypatch.setattr(main_module, "ENFORCE_ALLOWED_ESTADOS", True)
    monkeypatch.setattr(main_module, "ALLOWED_ESTADOS", ("RJ", "SP"))

    geocoding_mock = AsyncMock(return_value={
        "cep": "01001000",
        "logradouro": "Praça da Sé",
        "bairro": "Sé",
        "cidade": "São Paulo",
        "estado": "SP",
        "latitude": -23.5505,
        "longitude": -46.6333,
        "geometria": {"type": "Point", "coordinates": [-46.6333, -23.5505]},
        "geometria_tipo": "Point",
        "geocoding_score": 72.0,
        "confianca_geocoding": "media",
        "fonte": "api",
    })
    monkeypatch.setattr(main_module, "_geocodificar_cep", geocoding_mock)
    monkeypatch.setattr(main_module, "_consultar_setores_por_buffer", lambda _: [])

    response = client.get("/renda/cep/01001-000")
    assert response.status_code == 200


def test_cep_fora_dos_estados_permitidos_retorna_422(monkeypatch):
    monkeypatch.setattr(main_module, "gdf_setores", object())
    monkeypatch.setattr(main_module, "ENFORCE_ALLOWED_ESTADOS", True)
    monkeypatch.setattr(main_module, "ALLOWED_ESTADOS", ("RJ", "SP"))

    geocoding_mock = AsyncMock(return_value={
        "cep": "30140071",
        "logradouro": "Avenida Exemplo",
        "bairro": "Centro",
        "cidade": "Belo Horizonte",
        "estado": "MG",
        "latitude": -19.9245,
        "longitude": -43.9352,
        "geometria": {"type": "Point", "coordinates": [-43.9352, -19.9245]},
        "geometria_tipo": "Point",
        "geocoding_score": 70.0,
        "confianca_geocoding": "media",
        "fonte": "api",
    })
    monkeypatch.setattr(main_module, "_geocodificar_cep", geocoding_mock)

    response = client.get("/renda/cep/30140-071")
    assert response.status_code == 422
    assert "RJ, SP" in response.json()["detail"]


def test_rate_limit_retorna_429_com_mensagem_clara(monkeypatch):
    monkeypatch.setattr(main_module, "gdf_setores", object())
    monkeypatch.setattr(main_module, "RATE_LIMIT_ENABLED", True)
    monkeypatch.setattr(main_module, "RATE_LIMIT_WINDOW_SECONDS", 60)
    monkeypatch.setattr(main_module, "RATE_LIMIT_MAX_REQUESTS", 1)
    monkeypatch.setattr(main_module, "RATE_LIMIT_PATH_PREFIXES", ("/renda/cep",))
    main_module._rate_limit_buckets.clear()

    geocoding_mock = AsyncMock(return_value={
        "cep": "22071100",
        "logradouro": "Rua Exemplo",
        "bairro": "Copacabana",
        "cidade": "Rio de Janeiro",
        "estado": "RJ",
        "latitude": -22.97,
        "longitude": -43.18,
        "geometria": {"type": "Point", "coordinates": [-43.18, -22.97]},
        "geometria_tipo": "Point",
        "geocoding_score": 61.4,
        "confianca_geocoding": "media",
        "fonte": "api",
    })
    monkeypatch.setattr(main_module, "_geocodificar_cep", geocoding_mock)
    monkeypatch.setattr(main_module, "_consultar_setores_por_buffer", lambda _: [])

    headers = {"x-forwarded-for": "198.51.100.10"}
    primeira = client.get("/renda/cep/22071100", headers=headers)
    segunda = client.get("/renda/cep/22071100", headers=headers)

    assert primeira.status_code == 200
    assert segunda.status_code == 429
    assert "Muitas requisições" in segunda.json()["detail"]
    assert "Retry-After" in segunda.headers


@pytest.mark.parametrize("case", GOLDEN_CASES, ids=[case["id"] for case in GOLDEN_CASES])
def test_golden_ceps_regression(case, monkeypatch):
    monkeypatch.setattr(main_module, "gdf_setores", object())
    monkeypatch.setattr(main_module, "RATE_LIMIT_ENABLED", False)
    monkeypatch.setattr(main_module, "ENFORCE_ALLOWED_ESTADOS", True)
    monkeypatch.setattr(main_module, "ALLOWED_ESTADOS", ("RJ", "SP"))

    geocoding_mock = AsyncMock(return_value=case["geocoding"])
    monkeypatch.setattr(main_module, "_geocodificar_cep", geocoding_mock)

    setores_brutos = case.get("setores_renda", [])
    monkeypatch.setattr(
        main_module,
        "_consultar_setores_por_buffer",
        lambda _: [
            {
                "setor_censitario": str(i + 1),
                "renda_media": renda,
                "classe_social_estimada": "C",
            }
            for i, renda in enumerate(setores_brutos)
        ],
    )
    monkeypatch.setattr(
        main_module,
        "_setores_para_geojson",
        lambda _: {"type": "FeatureCollection", "features": []},
    )

    response = client.get(f"/renda/cep/{case['cep_input']}")
    assert response.status_code == case["expected"]["status_code"]

    if response.status_code == 200:
        body = response.json()
        if "renda_mediana" in case["expected"]:
            assert body["resumo"]["renda_mediana"] == case["expected"]["renda_mediana"]
        assert body["qualidade"]["confianca_geocoding"] == case["expected"]["confianca_geocoding"]
        assert body["qualidade"]["confianca_estimativa"] == case["expected"]["confianca_estimativa"]
    else:
        assert case["expected"]["detail_contains"] in response.json()["detail"]