import geobr
import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path

def baixar_malha_setores(estado_alvo, ano_censo):
    print(f"--- Iniciando download da Malha de Setores Censitários ({estado_alvo}) ---")
    
    try:
        gdf_setores = geobr.read_census_tract(code_tract=estado_alvo, year=ano_censo)
        
        print(f"Download concluído! Total de setores encontrados: {len(gdf_setores)}")
        return gdf_setores
    except Exception as e:
        print(f"Erro ao baixar dados: {e}")
        return None

def salvar_localmente(gdf, estado_alvo):
    caminho_arquivo = f"../dados_processados/setores_{estado_alvo}_2022.parquet"
    print(f"Salvando dados em: {caminho_arquivo}...")
    gdf.to_parquet(caminho_arquivo)
    print("Salvo com sucesso!")
    return Path(caminho_arquivo)

def gerar_visualizacao_teste(gdf, estado_alvo, ano_censo, output_dir):
    print("Gerando mapa de verificação...")
    f, ax = plt.subplots(figsize=(10, 10))
    gdf.plot(ax=ax, edgecolor='gray', linewidth=0.1, alpha=0.5)
    ax.set_title(f"Malha de Setores Censitários - {estado_alvo} ({ano_censo})")
    ax.set_axis_off()
    plt.savefig(f"{output_dir}/mapa_verificacao.png", dpi=150)
    print("Mapa de verificação salvo na pasta.")

def ingestao_dados(output_dir, estado_codigo, ano_censo):
    ESTADO_MAP = {'RJ': 'Rio de Janeiro', 'SP': 'São Paulo'}
    estado_nome = ESTADO_MAP.get(estado_codigo, estado_codigo)

    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    gdf = baixar_malha_setores(estado_codigo, ano_censo)

    if gdf is not None:
        arquivo = salvar_localmente(gdf, estado_codigo)
        gerar_visualizacao_teste(gdf, estado_nome, ano_censo, output_dir)
        print("\n--- Processo Finalizado ---")
        return arquivo

    return None