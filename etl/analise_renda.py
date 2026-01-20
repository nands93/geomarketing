import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path

def gerar_mapa_final(arquivo_mapa, arquivo_dados):
    print("--- Iniciando Processamento de Geomarketing ---")
    
    print(f"1. Carregando mapa digital: {arquivo_mapa}")
    if not arquivo_mapa.exists():
        raise FileNotFoundError("Arquivo de mapa não encontrado.")
    
    gdf = gpd.read_parquet(arquivo_mapa)
    
    print("   > Normalizando códigos dos setores no mapa...")
    gdf['code_tract'] = gdf['code_tract'].astype(str)
    gdf['code_tract'] = gdf['code_tract'].str.replace(r'\.0$', '', regex=True)
    
    print(f"2. Carregando dados de Renda: {arquivo_dados}")
    if not arquivo_dados.exists():
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {arquivo_dados}")
    
    df = pd.read_csv(arquivo_dados, sep=';', encoding='utf-8', dtype={'CD_SETOR': str})
    
    df = df.rename(columns={'CD_SETOR': 'code_tract', 'V06004': 'renda_media'})
    
    df['code_tract'] = df['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)

    print("   > Tratando dados ocultos e convertendo valores...")
    df['renda_media'] = df['renda_media'].astype(str).str.replace(',', '.', regex=False)
    df['renda_media'] = pd.to_numeric(df['renda_media'], errors='coerce')
    
    print("3. Cruzando Mapa + Dados...")
    gdf_final = gdf.merge(df[['code_tract', 'renda_media']], on='code_tract', how='inner')
    
    gdf_final = gdf_final.dropna(subset=['renda_media'])
    
    if gdf_final.empty:
        print(f"❌ ERRO CRÍTICO: O mapa continua vazio.")
        print(f"   Exemplo Mapa (RJ): '{gdf['code_tract'].iloc[0]}'")
        exemplo_rj = df[df['code_tract'].str.startswith('33')]
        if not exemplo_rj.empty:
            print(f"   Exemplo Planilha (RJ): '{exemplo_rj['code_tract'].iloc[0]}'")
        else:
            print("   O CSV não parece ter dados do Rio de Janeiro (começando com 33). Verifique se baixou o arquivo certo.")
        return

    print(f"   ✅ Cruzamento OK! {len(gdf_final)} setores com renda identificados.")

    print("   > Projetando para Web Mercator (EPSG:3857)...")
    gdf_final = gdf_final.to_crs(epsg=3857)

    print("4. Plotando o Mapa de Calor...")
    f, ax = plt.subplots(figsize=(15, 15))
    
    gdf_final.plot(
        column='renda_media',
        cmap='Spectral_r', 
        scheme='quantiles', 
        k=5, 
        legend=True,
        legend_kwds={'loc': 'lower right', 'title': 'Renda Média (R$)'},
        missing_kwds={'color': 'lightgrey'},
        ax=ax
    )
    
    ax.set_title("Mapa de Renda por Setor Censitário - RJ (Censo 2022)", fontsize=18)
    ax.set_axis_off()
    
    OUTPUT_IMG = "mapa_final_renda.png"
    plt.savefig(OUTPUT_IMG, dpi=300, bbox_inches='tight')
    print(f"✅ SUCESSO ABSOLUTO! O arquivo '{OUTPUT_IMG}' foi gerado.")