import geopandas as gpd
import pandas as pd
import os
from config import ESTADOS, ANO_CENSO

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARQUIVO_CSV_BRASIL = os.path.join(BASE_DIR, "dados_brutos", "Agregados_por_setores_renda_responsavel_BR.csv")
ARQUIVO_FINAL = os.path.join(BASE_DIR, "dados_processados", "setores_com_renda.parquet")

def gerar_base_definitiva():
    print("--- GERANDO BASE UNIFICADA DE SETORES (RJ + SP) ---")
    
    gdfs = []
    
    for estado in ESTADOS:
        print(f"\n▶ Processando {estado}...")
        
        ARQUIVO_MAPA_BRUTO = os.path.join(BASE_DIR, "dados_processados", f"setores_{estado}_2022.parquet")
        
        if not os.path.exists(ARQUIVO_MAPA_BRUTO):
            print(f"  ⚠ Mapa bruto não encontrado: {ARQUIVO_MAPA_BRUTO}")
            continue
        
        gdf = gpd.read_parquet(ARQUIVO_MAPA_BRUTO)
        gdf['code_tract'] = gdf['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)
        
        df = pd.read_csv(
            ARQUIVO_CSV_BRASIL,
            sep=';',
            encoding='utf-8',
            dtype={'CD_SETOR': str},
            usecols=['CD_SETOR', 'V06004']
        )
        df = df.rename(columns={'CD_SETOR': 'code_tract', 'V06004': 'renda_media'})
        df['code_tract'] = df['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)
        
        df['renda_media'] = pd.to_numeric(
            df['renda_media'].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        )
        
        gdf_final = gdf.merge(df, on='code_tract', how='inner')
        gdf_final = gdf_final.dropna(subset=['renda_media'])
        gdf_final = gdf_final.to_crs(epsg=3857)
        
        gdf_final['estado'] = estado
        
        print(f"   ✓ Setores com renda em {estado}: {len(gdf_final)}")
        gdfs.append(gdf_final)
    
    if gdfs:
        print(f"\n4. Consolidando {len(gdfs)} estado(s)...")
        gdf_unificado = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
        
        print(f"5. Salvando arquivo unificado: {ARQUIVO_FINAL}")
        gdf_unificado.to_parquet(ARQUIVO_FINAL)
        print(f"✅ SUCESSO! Total de setores: {len(gdf_unificado)}")
    else:
        print("❌ Nenhum estado foi processado.")

if __name__ == "__main__":
    gerar_base_definitiva()