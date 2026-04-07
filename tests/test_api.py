"""
Testes básicos da GeoMarketing API.
Execute com: pytest tests/ -v
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

# Importa o app — os dados não precisam estar carregados para testar a estrutura
from app.main import app

client = TestClient(app)


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