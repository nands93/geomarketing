import pandas as pd

# Configuração
ARQUIVO_BR = "./dados_brutos/Agregados_por_setores_basico_BR_20250417.csv" # Coloque o nome exato do arquivo BR que vc baixou
ESTADO_ALVO = "Rio de Janeiro"

print("1. Lendo o arquivo gigante do Brasil (isso pode levar alguns segundos)...")
# O IBGE usa separador ';' e encoding 'latin1' (ou iso-8859-1)
# low_memory=False ajuda a usar sua RAM de 32GB para carregar tudo rápido
df = pd.read_csv(ARQUIVO_BR, sep=';', encoding='latin1', low_memory=False)

print(f"2. Filtrando apenas {ESTADO_ALVO}...")
df_estado = df[df['NM_UF'] == ESTADO_ALVO]

print(f"   Encontrados {len(df_estado)} setores censitários.")

print("3. Salvando arquivo limpo...")
df_estado.to_csv(f"./dados_brutos/Basico_{ESTADO_ALVO}.csv", sep=';', index=False, encoding='utf-8')

print("Sucesso! Pode fechar o LibreOffice.")