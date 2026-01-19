import geopandas as gpd
import pandas as pd

# CONFIGURAÇÃO
ARQUIVO_MAPA = "./dados_processados/setores_RJ_2022.parquet"
ARQUIVO_DADOS = "./dados_brutos/Agregados_por_setores_renda_responsavel_BR.csv" # Use o nome correto do seu CSV

print("--- Gerando Dashboard Interativo (HTML) ---")

# 1. Carregar e Cruzar (Mesma lógica do script anterior, mas resumida)
gdf = gpd.read_parquet(ARQUIVO_MAPA)
# Limpeza de chaves
gdf['code_tract'] = gdf['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)

df = pd.read_csv(ARQUIVO_DADOS, sep=';', encoding='utf-8', dtype={'CD_SETOR': str})
df = df.rename(columns={'CD_SETOR': 'code_tract', 'V06004': 'renda_media'})
df['code_tract'] = df['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)

# Tratamento da Renda
df['renda_media'] = df['renda_media'].astype(str).str.replace(',', '.', regex=False)
df['renda_media'] = pd.to_numeric(df['renda_media'], errors='coerce')

# Merge
gdf_final = gdf.merge(df[['code_tract', 'renda_media']], on='code_tract', how='inner')
gdf_final = gdf_final.dropna(subset=['renda_media'])

# 2. A Mágica do Interactive (Folium/Leaflet)
print("Criando mapa web...")

# O método .explore() cria um mapa Leaflet automaticamente
# Isso é o poder do Geopandas moderno!
m = gdf_final.explore(
    column="renda_media", # A coluna que vai colorir o mapa
    cmap="Spectral_r",    # Paleta de cores
    scheme="quantiles",   # Divisão estatística
    k=5,                  # 5 faixas de renda
    legend=True,
    tooltip=["code_tract", "renda_media"], # O que aparece quando passa o mouse
    popup=["code_tract", "renda_media"],   # O que aparece quando clica
    name="Renda Média (Censo 2022)",
    tiles="CartoDB positron" # Um fundo de mapa limpo e bonito
)

# 3. Salvar como site
ARQUIVO_SAIDA = "dashboard_renda_rj.html"
m.save(ARQUIVO_SAIDA)

print(f"✅ SUCESSO! Abra o arquivo '{ARQUIVO_SAIDA}' no seu navegador (Chrome/Firefox).")