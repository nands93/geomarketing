from pathlib import Path

ESTADOS = ['RJ', 'SP']
ANO_CENSO = 2022
OUTPUT_DIR = Path('../dados_processados')
ARQUIVO_DADOS = Path("../dados_brutos/Agregados_por_setores_renda_responsavel_BR.csv")

ESTADO_MAP = {
    'RJ': 'Rio de Janeiro',
    'SP': 'São Paulo'
}