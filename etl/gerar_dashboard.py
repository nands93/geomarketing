import geopandas as gpd
import pandas as pd
import folium
from folium import Element
import matplotlib.colors as mcolors
import mapclassify
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARQUIVO_MAPA = os.path.join(BASE_DIR, "dados_processados", "setores_com_renda.parquet")

def gerar_dashboard():
    print("--- Gerando Dashboard (Híbrido: Salário Mínimo + Jenks) ---")

    if not os.path.exists(ARQUIVO_MAPA):
        print(f"❌ Erro: Arquivo {ARQUIVO_MAPA} não encontrado.")
        return

    print("1. Carregando dados...")
    gdf = gpd.read_parquet(ARQUIVO_MAPA)
    
    print("2. Simplificando geometria...")
    if gdf.crs != "EPSG:3857":
        gdf = gdf.to_crs(epsg=3857)
    gdf['geometry'] = gdf.simplify(tolerance=10, preserve_topology=True)

    print("3. Calculando faixas: Isolando Salário Mínimo + Jenks no resto...")
    
    SALARIO_MINIMO = 1621
    
    renda_acima_minimo = gdf[gdf['renda_media'] > SALARIO_MINIMO]['renda_media']
    
    classificador_topo = mapclassify.FisherJenks(renda_acima_minimo, k=5)
    
    bins_finais = [0, SALARIO_MINIMO] + list(classificador_topo.bins)
    
    bins_finais = sorted(list(set(bins_finais)))
    
    print(f"   > Faixas calculadas: {bins_finais}")

    gdf['categoria_id'] = pd.cut(
        gdf['renda_media'], 
        bins=bins_finais, 
        labels=range(len(bins_finais)-1), 
        include_lowest=True
    )
    
    cores_hex = ['#542788', '#4575b4', '#99d594', '#ffffbf', '#fc8d59', '#d73027']
    
    if len(bins_finais)-1 > len(cores_hex):
        cmap_personalizado = 'Spectral_r'
    else:
        cmap_personalizado = mcolors.ListedColormap(cores_hex[:len(bins_finais)-1])

    # Monta legenda
    labels_legenda = []
    limite_anterior = 0
    
    for i in range(len(bins_finais)-1):
        limite_superior = bins_finais[i+1]
        
        val_inf = f"{limite_anterior:,.0f}".replace(",", ".")
        val_sup = f"{limite_superior:,.0f}".replace(",", ".")
        
        if limite_superior == SALARIO_MINIMO:
            label = f"Grupo 1: Até 1 Sal. Mín. (R$ {val_sup})"
        elif i == len(bins_finais)-2:
            label = f"Grupo {i+1}: Acima de R$ {val_inf}"
        else:
            label = f"Grupo {i+1}: R$ {val_inf} a R$ {val_sup}"
            
        if isinstance(cmap_personalizado, mcolors.ListedColormap):
            cor_atual = cores_hex[i]
        else:
            cor_atual = mcolors.to_hex(plt.get_cmap(cmap_personalizado)(i))

        labels_legenda.append({'cor': cor_atual, 'texto': label})
        limite_anterior = limite_superior

    print("4. Renderizando mapa...")

    m = gdf.explore(
        column="categoria_id",
        cmap=cmap_personalizado,
        legend=False,
        tooltip=["code_tract", "renda_media"],
        popup=["code_tract", "renda_media"],
        name="Renda Híbrida",
        tiles="CartoDB positron",
        style_kwds={'fillOpacity': 0.8, 'weight': 0.1}
    )

    titulo_html = '''
        <div style="
            position: fixed; top: 20px; right: 20px; width: 300px; z-index:9999;
            background-color: white; opacity: 0.95; padding: 15px; border-radius: 8px;
            box-shadow: 0 0 10px rgba(0,0,0,0.2); border-left: 5px solid #542788; font-family: sans-serif;
        ">
            <h4 style="margin:0; color:#333;"><b>Mapa de Renda do Rio de Janeiro</b></h4>
            <small style="color:#666;">Censo Demográfico 2022</small><br>
            <small style="color:#666;">Fonte: IBGE | Fernando Marques</small>
        </div>
    '''
    m.get_root().html.add_child(Element(titulo_html))

    itens_html = ""
    for item in labels_legenda:
        itens_html += f'''
            <div style="display: flex; align-items: center; margin-bottom: 6px;">
                <div style="width: 18px; height: 18px; background:{item['cor']}; border: 1px solid #ccc; margin-right: 10px; border-radius: 3px;"></div>
                <span style="font-size: 13px; color: #444;">{item['texto']}</span>
            </div>
        '''

    legenda_html = f'''
        <div style="
            position: fixed; bottom: 30px; right: 20px; width: 320px; z-index:9999;
            background-color: white; opacity: 0.95; padding: 15px; border-radius: 8px;
            box-shadow: 0 0 15px rgba(0,0,0,0.2); font-family: sans-serif;
        ">
            <p style="margin:0 0 10px 0; font-weight:bold; font-size:14px; border-bottom:1px solid #eee; padding-bottom:5px;">
                Faixas de Renda (Realidade Social)
            </p>
            {itens_html}
        </div>
    '''
    m.get_root().html.add_child(Element(legenda_html))

    ARQUIVO_SAIDA = os.path.join(BASE_DIR, "dashboard_rj_renda.html")
    m.save(ARQUIVO_SAIDA)
    print(f"SUCESSO! Mapa gerado em: '{ARQUIVO_SAIDA}'")

if __name__ == "__main__":
    gerar_dashboard()