import pandas as pd
import geopandas as gpd

ARQUIVO_MAPA = "./dados_processados/setores_RJ_2022.parquet"
ARQUIVO_DADOS = "./dados_brutos/Agregados_por_setores_renda_responsavel_BR.csv" 

print("--- INICIANDO DIAGNÓSTICO ---")

# 1. Analisando o Mapa
print("\n1. MAPA (Geometria):")
gdf = gpd.read_parquet(ARQUIVO_MAPA)
print(f"   Total de setores: {len(gdf)}")
exemplo_mapa = str(gdf['code_tract'].iloc[0])
print(f"   Exemplo de Código (Original): '{gdf['code_tract'].iloc[0]}'")
print(f"   Tipo do dado: {type(gdf['code_tract'].iloc[0])}")
# Forçando string para simular o que seu script faz
gdf['code_tract'] = gdf['code_tract'].astype(str)
print(f"   Exemplo de Código (Como está indo pro Merge): '{gdf['code_tract'].iloc[0]}'")

# 2. Analisando a Planilha (CSV)
print("\n2. PLANILHA (Dados):")
# Vamos ler sem converter nada primeiro para ver a verdade nua e crua
df_raw = pd.read_csv(ARQUIVO_DADOS, sep=';', encoding='utf-8', nrows=5)
print("   Colunas encontradas:", df_raw.columns.tolist())

# Verifica se a coluna CD_SETOR existe
if 'CD_SETOR' in df_raw.columns:
    exemplo_csv = str(df_raw['CD_SETOR'].iloc[0])
    print(f"   Exemplo de Código (Original): '{df_raw['CD_SETOR'].iloc[0]}'")
else:
    print("   ❌ ERRO CRÍTICO: Não achei a coluna 'CD_SETOR'. O nome deve ser diferente.")
    # Tenta achar parecidos
    for col in df_raw.columns:
        if 'setor' in col.lower():
            print(f"      > Candidato encontrado: '{col}'")

# 3. Teste de Match
print("\n3. VEREDITO:")
if 'CD_SETOR' in df_raw.columns:
    # Comparação visual para você
    print(f"   Mapa diz:      '{gdf['code_tract'].iloc[0]}' (Tamanho: {len(gdf['code_tract'].iloc[0])})")
    # Pega o mesmo código no CSV se existir, senão pega o primeiro
    primeiro_csv = str(df_raw['CD_SETOR'].iloc[0])
    print(f"   Planilha diz:  '{primeiro_csv}' (Tamanho: {len(primeiro_csv)})")
    
    if gdf['code_tract'].iloc[0] == primeiro_csv:
        print("   ✅ ELES SÃO IGUAIS! O erro deve ser outra coisa.")
    else:
        print("   ❌ ELES SÃO DIFERENTES!")
        print("   Dicas do que procurar:")
        print("   - Um tem aspas e o outro não?")
        print("   - Um tem .0 no final (ex: '3304557.0')?")
        print("   - O tamanho da string é diferente?")