import ingestao_dados
import debug_chaves
import analise_renda
import gerar_dashboard
import gerar_bases_setores
from config import ESTADO_ALVO, ANO_CENSO, OUTPUT_DIR, ARQUIVO_DADOS

def main():
    arquivo_mapa = ingestao_dados.ingestao_dados(OUTPUT_DIR, ESTADO_ALVO, ANO_CENSO)
    if arquivo_mapa is None:
        print("Falha na ingestão. Abortando.")
        exit(1)
    debug_chaves.debug_chaves(arquivo_mapa, ARQUIVO_DADOS)
    analise_renda.gerar_mapa_final(arquivo_mapa, ARQUIVO_DADOS)
    gerar_bases_setores.gerar_base_definitiva()
    gerar_dashboard.gerar_dashboard(arquivo_mapa, ARQUIVO_DADOS)

if __name__ == "__main__":
    main()