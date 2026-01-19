import geobr
import pandas as pd
import matplotlib.pyplot as plt
import os

# Configuração
ESTADO_ALVO = 'RJ' # Vamos começar pelo Rio de Janeiro como exemplo
ANO_CENSO = 2022
OUTPUT_DIR = './dados_processados'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def baixar_malha_setores():
    print(f"--- Iniciando download da Malha de Setores Censitários ({ESTADO_ALVO}) ---")
    
    # read_census_tract: baixa a geometria (o polígono do setor)
    try:
        gdf_setores = geobr.read_census_tract(code_tract=ESTADO_ALVO, year=ANO_CENSO)
        
        print(f"Download concluído! Total de setores encontrados: {len(gdf_setores)}")
        return gdf_setores
    except Exception as e:
        print(f"Erro ao baixar dados: {e}")
        return None

def salvar_localmente(gdf):
    caminho_arquivo = f"{OUTPUT_DIR}/setores_{ESTADO_ALVO}_{ANO_CENSO}.parquet"
    
    print(f"Salvando dados em: {caminho_arquivo}...")
    
    gdf.to_parquet(caminho_arquivo)
    print("Salvo com sucesso!")
    return caminho_arquivo

def gerar_visualizacao_teste(gdf):
    print("Gerando mapa de verificação...")
    f, ax = plt.subplots(figsize=(10, 10))
    gdf.plot(ax=ax, edgecolor='gray', linewidth=0.1, alpha=0.5)
    ax.set_title(f"Malha de Setores Censitários - {ESTADO_ALVO} ({ANO_CENSO})")
    ax.set_axis_off()
    plt.savefig(f"{OUTPUT_DIR}/mapa_verificacao.png", dpi=150)
    print("Mapa de verificação salvo na pasta.")

if __name__ == "__main__":
    gdf = baixar_malha_setores()
    
    if gdf is not None:
        arquivo = salvar_localmente(gdf)
        
        gerar_visualizacao_teste(gdf)
        
        print("\n--- Processo Finalizado ---")
        print("Próximo passo: Cruzar o 'code_tract' (CD_SETOR) com a tabela de renda do IBGE.")