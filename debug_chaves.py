import pandas as pd
import geopandas as gpd


def debug_chaves(arquivo_mapa, arquivo_dados):
    print("--- INICIANDO DIAGNÓSTICO ---")

    print("\n1. MAPA (Geometria):")
    gdf = gpd.read_parquet(arquivo_mapa)
    print(f"   Total de setores: {len(gdf)}")
    exemplo_mapa = str(gdf['code_tract'].iloc[0])
    print(f"   Exemplo de Código (Original): '{gdf['code_tract'].iloc[0]}'")
    print(f"   Tipo do dado: {type(gdf['code_tract'].iloc[0])}")
    gdf['code_tract'] = gdf['code_tract'].astype(str)
    print(f"   Exemplo de Código (Como está indo pro Merge): '{gdf['code_tract'].iloc[0]}'")

    print("\n2. PLANILHA (Dados):")
    df_raw = pd.read_csv(arquivo_dados, sep=';', encoding='utf-8', nrows=5)
    print("   Colunas encontradas:", df_raw.columns.tolist())

    if 'CD_SETOR' in df_raw.columns:
        exemplo_csv = str(df_raw['CD_SETOR'].iloc[0])
        print(f"   Exemplo de Código (Original): '{df_raw['CD_SETOR'].iloc[0]}'")
    else:
        print("   ❌ ERRO CRÍTICO: Não achei a coluna 'CD_SETOR'. O nome deve ser diferente.")
        for col in df_raw.columns:
            if 'setor' in col.lower():
                print(f"      > Candidato encontrado: '{col}'")

    print("\n3. VEREDITO:")
    if 'CD_SETOR' in df_raw.columns:
        print(f"   Mapa diz:      '{gdf['code_tract'].iloc[0]}' (Tamanho: {len(gdf['code_tract'].iloc[0])})")
        primeiro_csv = str(df_raw['CD_SETOR'].iloc[0])
        print(f"   Planilha diz:  '{primeiro_csv}' (Tamanho: {len(primeiro_csv)})")
        
        if gdf['code_tract'].iloc[0] == primeiro_csv:
            print("   ✅ ELES SÃO IGUAIS! O erro deve ser outra coisa.")
        else:
            print("   ❌ ELES SÃO DIFERENTES!")
            print("   Dicas do que procurar:")
            print("   - Um tem aspas e o outro não?")
            print("   - Um tem .0 no final (ex: '3304557.0')?")
            print("   - O tamanho da string é diferente?")