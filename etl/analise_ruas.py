import osmnx as ox
import geopandas as gpd
import pandas as pd
import os
import warnings

# Ignorar avisos
warnings.filterwarnings('ignore')

# --- CONFIGURAÇÃO ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 1. ARQUIVOS (Certifique-se que o CSV do BR está na pasta correta)
ARQUIVO_MAPA = os.path.join(BASE_DIR, "dados_processados", "setores_RJ_2022.parquet")
ARQUIVO_DADOS = os.path.join(BASE_DIR, "dados_brutos", "Agregados_por_setores_renda_responsavel_BR.csv")

# 2. SAÍDA
ARQUIVO_SAIDA = os.path.join(BASE_DIR, "dados_processados", "ruas_rj_com_renda.parquet")

# 3. LOCAL
LOCAL_ALVO = "Copacabana, Rio de Janeiro, Brazil"

def mapear_ruas_ricas():
    print(f"--- INICIANDO ETL DE RUAS (Base BR) ---")
    print(f"Local Alvo: {LOCAL_ALVO}")

    # ==============================================================================
    # 1. CARREGAR E CRUZAR
    # ==============================================================================
    print("1. Carregando mapa e cruzando com a base Brasil...")
    
    if not os.path.exists(ARQUIVO_MAPA):
        print(f"❌ Erro: Mapa não encontrado ({ARQUIVO_MAPA}).")
        return
    if not os.path.exists(ARQUIVO_DADOS):
        print(f"❌ Erro: CSV do Brasil não encontrado ({ARQUIVO_DADOS}).")
        return

    # A) Carrega Mapa
    gdf_setores = gpd.read_parquet(ARQUIVO_MAPA)
    gdf_setores['code_tract'] = gdf_setores['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    # B) Carrega Dados (Otimizado com usecols)
    print("   > Lendo CSV gigante do Brasil...")
    df_renda = pd.read_csv(
        ARQUIVO_DADOS, 
        sep=';', 
        encoding='utf-8', 
        dtype={'CD_SETOR': str},
        usecols=['CD_SETOR', 'V06004'] 
    )
    
    df_renda = df_renda.rename(columns={'CD_SETOR': 'code_tract', 'V06004': 'renda_media'})
    df_renda['code_tract'] = df_renda['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    # Tratamento numérico
    df_renda['renda_media'] = pd.to_numeric(
        df_renda['renda_media'].astype(str).str.replace(',', '.', regex=False), 
        errors='coerce'
    )
    
    # C) Cruzamento
    gdf_rico = gdf_setores.merge(df_renda, on='code_tract', how='inner')
    gdf_rico = gdf_rico.dropna(subset=['renda_media'])
    gdf_rico = gdf_rico.to_crs(epsg=3857)

    print(f"   > Setores mapeados com renda: {len(gdf_rico)}")

    # ==============================================================================
    # 2. BAIXAR RUAS (OSM)
    # ==============================================================================
    print(f"2. Baixando malha viária de '{LOCAL_ALVO}'...")
    try:
        G = ox.graph_from_place(LOCAL_ALVO, network_type='all')
        gdf_ruas = ox.graph_to_gdfs(G, nodes=False, edges=True)
        gdf_ruas = gdf_ruas.to_crs(epsg=3857)
    except Exception as e:
        print(f"❌ Erro ao baixar ruas: {e}")
        return

    # ==============================================================================
    # 3. SPATIAL JOIN (Agrupamento por Índice)
    # ==============================================================================
    print("3. Cruzando Ruas com Setores...")
    
    ruas_com_renda = gpd.sjoin(
        gdf_ruas, 
        gdf_rico[['renda_media', 'geometry']], 
        how='inner', 
        predicate='intersects'
    )
    
    print("   > Calculando média por trecho de rua...")
    medias_por_rua = ruas_com_renda.groupby(ruas_com_renda.index)['renda_media'].mean()
    
    ruas_limpas = ruas_com_renda[~ruas_com_renda.index.duplicated(keep='first')].copy()
    ruas_limpas['renda_media'] = medias_por_rua
    
    # ==============================================================================
    # 4. SALVAR (COM BLINDAGEM CONTRA LISTAS)
    # ==============================================================================
    print(f"4. Salvando em: {ARQUIVO_SAIDA}")
    
    # Seleciona colunas úteis
    cols_to_keep = [c for c in ['name', 'geometry', 'renda_media', 'highway', 'length'] if c in ruas_limpas.columns]
    ruas_finais = ruas_limpas[cols_to_keep].copy() # .copy() é importante aqui

    # --- FIX PARA O ERRO PYARROW (SANITIZAÇÃO) ---
    print("   > Convertendo tipos complexos para texto simples...")
    for col in ruas_finais.columns:
        # Se a coluna não for geometria e for do tipo 'object' (texto ou lista misturada)
        if col != 'geometry' and ruas_finais[col].dtype == 'object':
            # Força virar string. Ex: ['Rua A', 'Rua B'] vira "['Rua A', 'Rua B']"
            ruas_finais[col] = ruas_finais[col].astype(str)

    # Agora salva sem medo
    ruas_finais.to_parquet(ARQUIVO_SAIDA)
    print("✅ SUCESSO! Base de ruas salva.")

    # ==============================================================================
    # 5. AMOSTRA
    # ==============================================================================
    print("\n--- TOP 5 RUAS MAIS RICAS (Amostra) ---")
    if 'name' in ruas_finais.columns:
        ranking = ruas_finais.dropna(subset=['name']).copy()
        top = ranking.groupby('name')['renda_media'].mean().sort_values(ascending=False).head(5)
        for rua, renda in top.items():
            print(f"💰 R$ {renda:,.2f} | {rua}")

if __name__ == "__main__":
    mapear_ruas_ricas()