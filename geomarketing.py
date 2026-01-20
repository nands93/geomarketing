import ingestao_dados
import debug_chaves
import analise_renda
import gerar_dashboard 

ESTADO_ALVO = 'RJ'
ANO_CENSO = 2022
OUTPUT_DIR = './dados_processados'
ARQUIVO_DADOS = "./dados_brutos/Agregados_por_setores_renda_responsavel_BR.csv" 

arquivo_mapa = ingestao_dados.ingestao_dados(OUTPUT_DIR, ESTADO_ALVO, ANO_CENSO)
debug_chaves.debug_chaves(arquivo_mapa, ARQUIVO_DADOS)
analise_renda.gerar_mapa_final(arquivo_mapa, ARQUIVO_DADOS)
gerar_dashboard.gerar_dashboard(arquivo_mapa, ARQUIVO_DADOS)