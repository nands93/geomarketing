import geopandas as gpd
import pandas as pd
from folium import Element
import mapclassify
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import os

def gerar_dashboard(arquivo_mapa, arquivo_dados):
    print("--- Gerando Dashboard ---")

    if not os.path.exists(arquivo_mapa) or not os.path.exists(arquivo_dados):
        print("Erro: Arquivos não encontrados.")
        return

    gdf = gpd.read_parquet(arquivo_mapa)
    gdf['code_tract'] = gdf['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)

    df = pd.read_csv(arquivo_dados, sep=';', encoding='utf-8', dtype={'CD_SETOR': str})
    df = df.rename(columns={'CD_SETOR': 'code_tract', 'V06004': 'renda_media'})
    df['code_tract'] = df['code_tract'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    df['renda_media'] = df['renda_media'].astype(str).str.replace(',', '.', regex=False)
    df['renda_media'] = pd.to_numeric(df['renda_media'], errors='coerce')

    gdf_final = gdf.merge(df[['code_tract', 'renda_media']], on='code_tract', how='inner')
    gdf_final = gdf_final.dropna(subset=['renda_media'])

    if gdf_final.empty:
        print("Mapa vazio.")
        return
    
    NOME_PALETA = "Spectral_r"
    NUM_CLASSES = 8
    
    cmap = plt.get_cmap(NOME_PALETA, NUM_CLASSES)
    cores_hex = [mcolors.to_hex(cmap(i)) for i in range(NUM_CLASSES)]
    
    classificador = mapclassify.Quantiles(gdf_final['renda_media'], k=NUM_CLASSES)
    bins = classificador.bins 

    labels_legenda = []
    limite_inferior = gdf_final['renda_media'].min()
    
    for i, limite_superior in enumerate(bins):
        val_inf = f"{limite_inferior:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        val_sup = f"{limite_superior:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        
        label = f"De R$ {val_inf} até R$ {val_sup}"
        labels_legenda.append({'cor': cores_hex[i], 'texto': label})
        
        limite_inferior = limite_superior

    m = gdf_final.explore(
        column="renda_media",
        cmap=NOME_PALETA,
        scheme="quantiles",
        k=NUM_CLASSES,
        legend=False,
        tooltip=["code_tract", "renda_media"],
        popup=["code_tract", "renda_media"],
        name="Camada de Renda",
        tiles="CartoDB positron",
        style_kwds={'fillOpacity': 0.8, 'weight': 0.3}
    )

    titulo_html = '''
        <div style="
            position: fixed; top: 20px; right: 20px; width: 300px; z-index:9999;
            background-color: white; opacity: 0.95; padding: 15px; border-radius: 8px;
            box-shadow: 0 0 10px rgba(0,0,0,0.2); border-left: 5px solid #d7191c; font-family: sans-serif;
        ">
            <h4 style="margin:0; color:#333;"><b>Mapa de Renda: RJ</b></h4>
            <small style="color:#666;">Renda média (Censo 2022)</small><br>
            <small style="color:#666;">Fonte: IBGE | Fernando Marques</small>
        </div>
    '''
    m.get_root().html.add_child(Element(titulo_html))

    itens_html = ""
    for item in labels_legenda:
        itens_html += f'''
            <div style="display: flex; align-items: center; margin-bottom: 5px;">
                <div style="width: 15px; height: 15px; background:{item['cor']}; border: 1px solid #ccc; margin-right: 8px;"></div>
                <span style="font-size: 12px; color: #333;">{item['texto']}</span>
            </div>
        '''

    legenda_html = f'''
        <div style="
            position: fixed; bottom: 30px; right: 20px; width: 280px; z-index:9999;
            background-color: white; opacity: 0.95; padding: 10px; border-radius: 8px;
            box-shadow: 0 0 10px rgba(0,0,0,0.2); font-family: sans-serif;
        ">
            <p style="margin:0 0 8px 0; font-weight:bold; font-size:13px; border-bottom:1px solid #eee; padding-bottom:5px;">
                Faixas de Renda Mensal
            </p>
            {itens_html}
        </div>
    '''
    m.get_root().html.add_child(Element(legenda_html))

    ARQUIVO_SAIDA = "dashboard_rj_renda.html"
    m.save(ARQUIVO_SAIDA)
    print(f"✅ SUCESSO! O mapa foi salvo em '{ARQUIVO_SAIDA}'.")