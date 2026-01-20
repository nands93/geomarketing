import geopandas as gpd
import pandas as pd

def gerar_dashboard(arquivo_mapa, arquivo_dados):
    print("--- Gerando Dashboard Interativo (HTML) ---")
    gdf = gpd.read_parquet(arquivo_mapa)
    gdf['code_tract'] = gdf['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)

    df = pd.read_csv(arquivo_dados, sep=';', encoding='utf-8', dtype={'CD_SETOR': str})
    df = df.rename(columns={'CD_SETOR': 'code_tract', 'V06004': 'renda_media'})
    df['code_tract'] = df['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)

    df['renda_media'] = df['renda_media'].astype(str).str.replace(',', '.', regex=False)
    df['renda_media'] = pd.to_numeric(df['renda_media'], errors='coerce')

    gdf_final = gdf.merge(df[['code_tract', 'renda_media']], on='code_tract', how='inner')
    gdf_final = gdf_final.dropna(subset=['renda_media'])
    if gdf_final.empty:
        print("❌ ERRO CRÍTICO: O mapa final está vazio. Verifique os dados de entrada.")
        return
    print("Criando mapa web...")

    m = gdf_final.explore(
        column="renda_media", # A coluna que vai colorir o mapa
        cmap="RdPu_r",    # Paleta de cores
        scheme="quantiles",   # Divisão estatística
        k=7,                  # 7 faixas de renda
        legend=True,
        tooltip=["code_tract", "renda_media"], # O que aparece quando passa o mouse
        popup=["code_tract", "renda_media"],   # O que aparece quando clica
        name="Renda Média (Censo 2022)",
        tiles="CartoDB positron" # Um fundo de mapa limpo e bonito
    )

    ARQUIVO_SAIDA = "dashboard_renda_rj.html"
    m.save(ARQUIVO_SAIDA)

    print(f"✅ SUCESSO! Abra o arquivo '{ARQUIVO_SAIDA}' no seu navegador (Chrome/Firefox).")