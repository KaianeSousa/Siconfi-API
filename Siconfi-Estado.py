import requests
import pandas as pd
import time
from tqdm import tqdm
import random
import os

tipo_relatorio = "RREO"
esfera = "E"
num_anexo = "03"
anos = range(2015, 2025)
periodos = range(1, 7)

tempo_espera_requisicao = 2
variacao = 0.5
tentativas = 5
delay_requisicao = [3, 5, 6, 7, 8, 10, 12, 15, 30, 45, 60]

CABECALHOS = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'application/json',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
}

def obter_codigo_estado_ceara():
    return [23]

def fazer_requisicao(url):
    for tentativa in range(tentativas):
        try:
            time.sleep(tempo_espera_requisicao + random.uniform(-variacao, variacao))
            resposta = requests.get(url, headers=CABECALHOS, timeout=60)

            if resposta.status_code == 200:
                return resposta
            elif resposta.status_code == 429:
                tempo_espera = delay_requisicao[tentativa] if tentativa < len(delay_requisicao) else delay_requisicao[-1]
                print(f"Erro 429 - Muitas requisições. Tentando novamente em {tempo_espera} segundos...")
                time.sleep(tempo_espera)
            else:
                print(f"Erro {resposta.status_code}. Tentativa {tentativa + 1}/{tentativas}")
                time.sleep(delay_requisicao[tentativa] if tentativa < len(delay_requisicao) else delay_requisicao[-1])
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {str(e)}. Tentativa {tentativa + 1}/{tentativas}")
            time.sleep(delay_requisicao[tentativa] if tentativa < len(delay_requisicao) else delay_requisicao[-1])
    return None

def coletar_dados(codigos_ibge):
    url_base = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo?"
    dados = []
    combinacoes = [(ano, periodo, cod_ibge) for ano in anos for periodo in periodos for cod_ibge in codigos_ibge]

    for ano, periodo, cod_ibge in tqdm(combinacoes, desc="Coletando dados"):
        pagina = 1
        while True:
            url = (
                f"{url_base}an_exercicio={ano}&nr_periodo={periodo}&"
                f"co_tipo_demonstrativo={tipo_relatorio}&"
                f"no_anexo=RREO-Anexo%20{num_anexo}&"
                f"co_esfera={esfera}&id_ente={cod_ibge}&page={pagina}&limit=5000"
            )

            resposta = fazer_requisicao(url)
            if resposta is None:
                print(f"Falha ao coletar dados para {ano}-{periodo}-{cod_ibge}")
                break

            dados_json = resposta.json()
            itens = dados_json.get("items", [])
            if itens:
                dados.extend(itens)
                print(f"Ente {cod_ibge} - Página {pagina} coletada ({len(itens)} registros)")
                tem_mais = len(itens) >= 5000
                if not tem_mais:
                    break
                pagina += 1
            else:
                if pagina == 1:
                    print(f"Nenhum dado encontrado para {ano}-{periodo}-{cod_ibge}")
                break

    return pd.DataFrame(dados) if dados else pd.DataFrame()

def converter_colunas(df):
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            try:
                df[coluna] = pd.to_numeric(df[coluna])
            except ValueError:
                continue
    return df

def salvar_csv_seguro(df, nome_base):
    contador = 1
    nome_arquivo = f"{nome_base}.csv"
    while os.path.exists(nome_arquivo):
        try:
            with open(nome_arquivo, 'a'):
                break
        except PermissionError:
            nome_arquivo = f"{nome_base}_{contador}.csv"
            contador += 1
    df.to_csv(nome_arquivo, index=False, sep=',', encoding='utf-8-sig')
    return nome_arquivo

def main():
    print("Iniciando coleta de dados...")

    codigos_ibge = obter_codigo_estado_ceara()
    df_final = coletar_dados(codigos_ibge)

    if not df_final.empty:
        df_final = converter_colunas(df_final)

        colunas_para_remover = ['rotulo', 'demonstrativo', 'uf', 'anexo', 'esfera']
        df_final.drop(columns=colunas_para_remover, inplace=True, errors='ignore')

        qtd_antes = len(df_final)
        df_final.drop_duplicates(inplace=True)
        qtd_depois = len(df_final)
        print(f"Removendo dados duplicados: {qtd_antes - qtd_depois} dados removidos.")

        nome_base = f"RREO_Anexo{num_anexo}_CE_estado"
        nome_arquivo = salvar_csv_seguro(df_final, nome_base)

        print(f"\nO arquivo foi salvo como: {nome_arquivo}")
        print(f"Total de dados coletados: {len(df_final)}")
        print(f"Colunas disponíveis: {df_final.columns.tolist()}")

        colunas_vazias = df_final.columns[df_final.isnull().all()].tolist()
        if colunas_vazias:
            print(f"Colunas vazias: {colunas_vazias}")
        if 'id_ente' in df_final.columns:
            print(f"Código IBGE usado: {df_final['id_ente'].unique().tolist()}")
    else:
        print("\nNenhum dado coletado.")

if __name__ == "__main__":
    main()