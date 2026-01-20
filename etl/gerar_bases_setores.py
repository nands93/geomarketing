import geopandas as gpd
import pandas as pd
import os

# --- CONFIGURAÇÃO ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Entradas
ARQUIVO_MAPA_BRUTO = os.path.join(BASE_DIR, "dados_processados", "setores_RJ_2022.parquet")
ARQUIVO_CSV_BRASIL = os.path.join(BASE_DIR, "dados_brutos", "Agregados_por_setores_renda_responsavel_BR.csv")

# Saída (O Arquivo que falta para a API)
ARQUIVO_FINAL = os.path.join(BASE_DIR, "dados_processados", "setores_com_renda.parquet")

def gerar_base_definitiva():
    print("--- GERANDO BASE DE SETORES ENRIQUECIDA ---")
    
    # 1. Carrega Mapa
    print("1. Lendo geometria dos setores...")
    if not os.path.exists(ARQUIVO_MAPA_BRUTO):
        print("Erro: Mapa bruto não encontrado.")
        return
    gdf = gpd.read_parquet(ARQUIVO_MAPA_BRUTO)
    # Limpeza de chave
    gdf['code_tract'] = gdf['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)

    # 2. Carrega CSV Brasil (Otimizado)
    print("2. Lendo dados de renda do Brasil...")
    df = pd.read_csv(
        ARQUIVO_CSV_BRASIL, 
        sep=';', 
        encoding='utf-8', 
        dtype={'CD_SETOR': str},
        usecols=['CD_SETOR', 'V06004'] # Só carrega o que importa
    )
    df = df.rename(columns={'CD_SETOR': 'code_tract', 'V06004': 'renda_media'})
    df['code_tract'] = df['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    # Trata números
    df['renda_media'] = pd.to_numeric(
        df['renda_media'].astype(str).str.replace(',', '.', regex=False), 
        errors='coerce'
    )

    # 3. Cruzamento
    print("3. Cruzando Mapa + Renda...")
    gdf_final = gdf.merge(df, on='code_tract', how='inner')
    
    # Remove vazios e projeta
    gdf_final = gdf_final.dropna(subset=['renda_media'])
    gdf_final = gdf_final.to_crs(epsg=3857)
    
    print(f"   > Total de setores com renda no RJ: {len(gdf_final)}")

    # 4. Salvar
    print(f"4. Salvando arquivo final em: {ARQUIVO_FINAL}")
    gdf_final.to_parquet(ARQUIVO_FINAL)
    print("✅ SUCESSO! Base pronta para a API.")

if __name__ == "__main__":
    gerar_base_definitiva()