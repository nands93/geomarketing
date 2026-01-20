import ingestao_dados
import debug_chaves
import analise_renda
import gerar_dashboard 

ESTADO_ALVO = 'RJ'
ANO_CENSO = 2022
OUTPUT_DIR = './dados_processados'
ARQUIVO_MAPA = "./dados_processados/setores_RJ_2022.parquet"
ARQUIVO_DADOS = "./dados_brutos/Agregados_por_setores_renda_responsavel_BR.csv" 

arquivo = ingestao_dados.ingestao_dados(OUTPUT_DIR, ESTADO_ALVO, ANO_CENSO)
debug_chaves.debug_chaves(ARQUIVO_MAPA, ARQUIVO_DADOS)
analise_renda.gerar_mapa_final(ARQUIVO_MAPA, ARQUIVO_DADOS)
gerar_dashboard.gerar_dashboard(ARQUIVO_MAPA, ARQUIVO_DADOS)