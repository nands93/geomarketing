from fastapi import FastAPI, HTTPException
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import os

app = FastAPI(title="GeoAPI Pro - Renda & Ruas", version="2.0")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR_DADOS = os.path.join(BASE_DIR, "dados_processados")

ARQUIVO_SETORES = os.path.join(DIR_DADOS, "setores_com_renda.parquet")
ARQUIVO_RUAS = os.path.join(DIR_DADOS, "ruas_rj_com_renda.parquet")

print(f"⏳ Iniciando API... Raiz do projeto: {BASE_DIR}")

gdf_setores = None
gdf_ruas = None

if os.path.exists(ARQUIVO_SETORES):
    try:
        gdf_setores = gpd.read_parquet(ARQUIVO_SETORES)
        if gdf_setores.crs != "EPSG:3857":
            gdf_setores = gdf_setores.to_crs(epsg=3857)
        print(f"   ✅ Setores carregados: {len(gdf_setores)} polígonos.")
    except Exception as e:
        print(f"   Erro ao ler setores: {e}")
else:
    print(f"   ⚠️ Arquivo de setores não encontrado: {ARQUIVO_SETORES}")

if os.path.exists(ARQUIVO_RUAS):
    try:
        gdf_ruas = gpd.read_parquet(ARQUIVO_RUAS)
        print(f"   Ruas carregadas: {len(gdf_ruas)} segmentos.")
    except Exception as e:
        print(f"   Erro ao ler ruas: {e}")
else:
    print(f"   ⚠️ Arquivo de ruas não encontrado: {ARQUIVO_RUAS}")

print("🚀 API Online! Aguardando requisições...")

@app.get("/")
def home():
    """Rota de boas-vindas para verificar se está tudo online."""
    status_ruas = "Disponível" if gdf_ruas is not None else "Indisponível"
    return {
        "status": "online",
        "dados_ruas": status_ruas,
        "endpoints": [
            "/renda/ponto/{lat}/{lng}",
            "/ruas/melhores?renda_minima=10000"
        ]
    }

@app.get("/renda/ponto/{lat}/{lng}")
def consultar_ponto(lat: float, lng: float):
    """
    Recebe Lat/Long e diz qual a renda do setor censitário correspondente.
    """
    if gdf_setores is None:
        raise HTTPException(status_code=503, detail="Base de setores não carregada.")

    ponto = gpd.GeoDataFrame(geometry=[Point(lng, lat)], crs="EPSG:4326")
    
    ponto = ponto.to_crs(gdf_setores.crs)
    
    resultado = gpd.sjoin(ponto, gdf_setores, how="left", predicate="within")
    
    # 4. Verifica se achou
    if resultado.empty or pd.isna(resultado.iloc[0]['renda_media']):
        return {
            "latitude": lat,
            "longitude": lng,
            "mensagem": "Local fora da área mapeada ou sem dados."
        }
        
    dado = resultado.iloc[0]
    
    renda = dado['renda_media']
    classe = "A" if renda > 22000 else ("B" if renda > 7100 else ("C" if renda > 2900 else "D/E"))

    return {
        "latitude": lat,
        "longitude": lng,
        "setor_censitario": dado['code_tract'],
        "renda_estimada": round(renda, 2),
        "classe_social_estimada": classe
    }

@app.get("/ruas/melhores")
def recomendar_ruas(renda_minima: float = 5000, top: int = 10):
    """
    Lista as ruas com renda média superior ao valor informado.
    """
    if gdf_ruas is None:
        raise HTTPException(status_code=503, detail="Base de ruas não carregada. Rode o ETL primeiro.")
    
    # 1. Filtra ruas ricas
    filtro = gdf_ruas[gdf_ruas['renda_media'] >= renda_minima].copy()
    
    if filtro.empty:
        return {"msg": f"Nenhuma rua encontrada com renda acima de R$ {renda_minima}"}

    filtro['name_clean'] = filtro['name'].astype(str).str.replace(r"[\[\]']", "", regex=True)

    ranking = filtro.groupby('name_clean')['renda_media'].mean().sort_values(ascending=False).head(top)
    
    lista_ruas = []
    for rua, renda in ranking.items():
        if rua != "nan":
            lista_ruas.append({"rua": rua, "renda_media": round(renda, 2)})
        
    return {
        "filtro_renda_minima": renda_minima,
        "total_segmentos_encontrados": len(filtro),
        "top_recomendacoes": lista_ruas
    }