import ingestao_dados
import analise_renda
import gerar_dashboard
import gerar_bases_setores
from config import ESTADOS, ANO_CENSO, OUTPUT_DIR, ARQUIVO_DADOS, ESTADO_MAP

def main():
    # 1. Download geometry for each state
    for estado in ESTADOS:
        estado_nome = ESTADO_MAP[estado]
        print(f"\n{'='*70}")
        print(f"BAIXANDO DADOS: {estado} ({estado_nome})")
        print(f"{'='*70}")
        
        arquivo_mapa = ingestao_dados.ingestao_dados(OUTPUT_DIR, estado, ANO_CENSO)  # ✅ Pass estado (code), not estado_nome
        if arquivo_mapa is None:
            print(f"⚠ Falha ao baixar {estado}. Pulando...")
            continue
    
    # 2. Then merge all states
    print(f"\n{'='*70}")
    print("UNIFICANDO DADOS DE TODOS OS ESTADOS")
    print(f"{'='*70}")
    gerar_bases_setores.gerar_base_definitiva()
    print("✅ Pipeline concluído!")

if __name__ == "__main__":
    main()