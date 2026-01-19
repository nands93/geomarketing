import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import os

# --- CONFIGURAÇÃO ---
ARQUIVO_MAPA = "./dados_processados/setores_RJ_2022.parquet"
# Confirme se este é o nome exato do seu CSV baixado
ARQUIVO_DADOS = "./dados_brutos/Agregados_por_setores_renda_responsavel_BR.csv" 

def gerar_mapa_final():
    print("--- Iniciando Processamento de Geomarketing ---")
    
    # 1. Carregar Geometria
    print(f"1. Carregando mapa digital: {ARQUIVO_MAPA}")
    if not os.path.exists(ARQUIVO_MAPA):
        raise FileNotFoundError("Arquivo de mapa não encontrado.")
    
    gdf = gpd.read_parquet(ARQUIVO_MAPA)
    gdf['code_tract'] = gdf['code_tract'].astype(str)
    
    # 2. Carregar Dados de Renda
    print(f"2. Carregando dados de Renda: {ARQUIVO_DADOS}")
    if not os.path.exists(ARQUIVO_DADOS):
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {ARQUIVO_DADOS}")
    
    # Lê o CSV
    df = pd.read_csv(ARQUIVO_DADOS, sep=';', encoding='utf-8', dtype={'CD_SETOR': str})
    
    # Renomeia
    df = df.rename(columns={'CD_SETOR': 'code_tract', 'V06004': 'renda_media'})
    
    # --- TRATAMENTO DE DADOS (O FIX DO 'X') ---
    print("   > Tratando dados ocultos e convertendo valores...")
    df['renda_media'] = df['renda_media'].astype(str).str.replace(',', '.', regex=False)
    df['renda_media'] = pd.to_numeric(df['renda_media'], errors='coerce')
    
    # 3. Cruzamento
    print("3. Cruzando Mapa + Dados...")
    gdf_final = gdf.merge(df[['code_tract', 'renda_media']], on='code_tract', how='inner')
    
    # Remove vazios
    gdf_final = gdf_final.dropna(subset=['renda_media'])
    
    # VERIFICAÇÃO DE SEGURANÇA (Para não dar erro em mapa vazio)
    if gdf_final.empty:
        print("❌ ERRO: O cruzamento gerou um mapa vazio. Verifique se os códigos dos setores (CD_SETOR) batem entre o CSV e o Mapa.")
        return

    # --- CORREÇÃO DO ERRO DE ASPECTO ---
    print("   > Projetando para Web Mercator (EPSG:3857) para corrigir erro visual...")
    # Isso transforma Lat/Long em Metros, resolvendo o problema do matplotlib
    gdf_final = gdf_final.to_crs(epsg=3857)

    # 4. Gerar o Mapa
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
    print(f"✅ SUCESSO! O arquivo '{OUTPUT_IMG}' foi gerado na sua pasta.")

if __name__ == "__main__":
    gerar_mapa_final()