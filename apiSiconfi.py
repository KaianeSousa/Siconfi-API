import requests
import pandas as pd
import time
from tqdm import tqdm

tipo_relatorio = "RREO"
esfera = "M"
cod_ibge_ce = 23  
num_anexo = "03"
nome_arquivo = "RREO_Anexo03_Municipios_Sertao_Central_CE.csv"
anos = range(2015, 2025)
periodos = range(1, 7)

    
codigos_municipios_sertao_central_ce = [
    2310407, 2307635, 2306603, 2303006, 2302800, 2304608, 2313005, 2312700, 2311405,
    2311306, 2310506, 2308351, 2305332, 2305266, 2304269, 2303931, 2301851, 2310902,
    2308500, 2312205, 2302404
]  

def coletar_dados():
    base_url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo?"
    dados = []
    
    combinacoes = [(ano, periodo, cod_ibge) for ano in anos for periodo in periodos for cod_ibge in codigos_municipios_ce]
    
    for ano, periodo, cod_ibge in tqdm(combinacoes, desc="Coletando dados"):
        page = 1
        while True:
            url = (
                f"{base_url}an_exercicio={ano}&nr_periodo={periodo}&"
                f"co_tipo_demonstrativo={tipo_relatorio}&"
                f"no_anexo=RREO-Anexo%20{num_anexo}&"
                f"co_esfera={esfera}&id_ente={cod_ibge}&page={page}&limit=5000"
            )
            
            try:
                response = requests.get(url, timeout=30)
                
                if response.status_code == 200:
                    items = response.json().get("items", [])
                    if items:
                        dados.extend(items)
                        print(f"Município {cod_ibge} - Página {page} coletada ({len(items)} registros)")

                        if len(items) < 5000:
                            print(f"  -> Última página para {cod_ibge} em {ano}/{periodo}, menos de 5000 itens retornados.")
                            break
                        page += 1
                    else:
                        break
                else:
                    print(f"Erro {response.status_code} ao coletar {ano}-{periodo}-{cod_ibge}")
                    break
                
                time.sleep(1) 
                
            except Exception as e:
                print(f"Erro ao requisitar dados do município {cod_ibge}: {str(e)}. Retentando...")
                time.sleep(1)
                continue
    
    return pd.DataFrame(dados) if dados else pd.DataFrame()

def converter_colunas(df):
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            try:
                df[coluna] = pd.to_numeric(df[coluna])
            except ValueError:
                continue  
    return df

def main():
    print("Iniciando coleta de dados...")
    df_final = coletar_dados()
    
    if not df_final.empty:
        df_final = converter_colunas(df_final)

        if 'rotulo' in df_final.columns:
            df_final.drop(columns=['rotulo'], inplace=True) 

        df_final.to_csv(nome_arquivo, index=False, sep=',', encoding='utf-8-sig')
        
        print(f"\nArquivo salvo: {nome_arquivo}")
        print(f"Total de registros: {len(df_final)}")
        print(f"Colunas disponíveis: {df_final.columns.tolist()}")

        colunas_vazias = df_final.columns[df_final.isnull().all()].tolist()
        if colunas_vazias:
            print(f"⚠️ Atenção: Algumas colunas estão completamente vazias: {colunas_vazias}")

        if 'id_ente' in df_final.columns:
            print(f"Municípios incluídos: {df_final['id_ente'].nunique()}")
        else:
            print("⚠️ A coluna 'id_ente' não está presente nos dados!")

    else:
        print("\nNenhum dado coletado.")

if __name__ == "__main__":
    main()
