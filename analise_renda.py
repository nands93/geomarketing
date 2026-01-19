import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import os

# --- CONFIGURAÇÃO ---
ARQUIVO_MAPA = "./dados_processados/setores_RJ_2022.parquet"
ARQUIVO_DADOS = "./dados_brutos/Agregados_por_setores_renda_responsavel_BR.csv" 
# IMPORTANTE: Se o seu arquivo CSV for o do Brasil, ele vai funcionar igual, 
# desde que a variável ARQUIVO_DADOS aponte para ele.

def gerar_mapa_final():
    print("--- Iniciando Processamento de Geomarketing ---")
    
    # 1. Carregar Geometria
    print(f"1. Carregando mapa digital: {ARQUIVO_MAPA}")
    if not os.path.exists(ARQUIVO_MAPA):
        raise FileNotFoundError("Arquivo de mapa não encontrado.")
    
    gdf = gpd.read_parquet(ARQUIVO_MAPA)
    
    # --- FAXINA NO CÓDIGO DO MAPA (CORREÇÃO DO .0) ---
    print("   > Normalizando códigos dos setores no mapa...")
    gdf['code_tract'] = gdf['code_tract'].astype(str)
    # Remove o ".0" do final se existir
    gdf['code_tract'] = gdf['code_tract'].str.replace(r'\.0$', '', regex=True)
    
    # 2. Carregar Dados de Renda
    print(f"2. Carregando dados de Renda: {ARQUIVO_DADOS}")
    if not os.path.exists(ARQUIVO_DADOS):
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {ARQUIVO_DADOS}")
    
    # dtype={'CD_SETOR': str} força o Pandas a ler como texto puro, evitando erros
    df = pd.read_csv(ARQUIVO_DADOS, sep=';', encoding='utf-8', dtype={'CD_SETOR': str})
    
    # Renomeia
    df = df.rename(columns={'CD_SETOR': 'code_tract', 'V06004': 'renda_media'})
    
    # --- FAXINA NO CÓDIGO DA PLANILHA ---
    # Só por garantia, removemos .0 da planilha também, caso exista
    df['code_tract'] = df['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    # --- TRATAMENTO DOS VALORES DE RENDA ---
    print("   > Tratando dados ocultos e convertendo valores...")
    df['renda_media'] = df['renda_media'].astype(str).str.replace(',', '.', regex=False)
    df['renda_media'] = pd.to_numeric(df['renda_media'], errors='coerce')
    
    # 3. Cruzamento
    print("3. Cruzando Mapa + Dados...")
    gdf_final = gdf.merge(df[['code_tract', 'renda_media']], on='code_tract', how='inner')
    
    # Remove vazios
    gdf_final = gdf_final.dropna(subset=['renda_media'])
    
    # VERIFICAÇÃO FINAL
    if gdf_final.empty:
        print(f"❌ ERRO CRÍTICO: O mapa continua vazio.")
        print(f"   Exemplo Mapa (RJ): '{gdf['code_tract'].iloc[0]}'")
        # Tenta achar o RJ no dataframe para provar que existe
        exemplo_rj = df[df['code_tract'].str.startswith('33')]
        if not exemplo_rj.empty:
            print(f"   Exemplo Planilha (RJ): '{exemplo_rj['code_tract'].iloc[0]}'")
        else:
            print("   O CSV não parece ter dados do Rio de Janeiro (começando com 33). Verifique se baixou o arquivo certo.")
        return

    print(f"   ✅ Cruzamento OK! {len(gdf_final)} setores com renda identificados.")

    # --- PROJEÇÃO PARA CORRIGIR ERRO VISUAL ---
    print("   > Projetando para Web Mercator (EPSG:3857)...")
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
    print(f"✅ SUCESSO ABSOLUTO! O arquivo '{OUTPUT_IMG}' foi gerado.")

if __name__ == "__main__":
    gerar_mapa_final()