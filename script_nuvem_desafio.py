import pandas as pd
import psycopg2
import csv
import re
import json
import subprocess
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from datetime import datetime, timedelta

def criar_conexao_banco_dados():
    """
    Estabelece a conexão com o banco de dados PostgreSQL.

    Retorna:
    - engine: Objeto de engine do SQLAlchemy
    - conn: Objeto de conexão psycopg2
    - cursor: Objeto de cursor psycopg2
    """
    engine = create_engine('postgresql://postgres:admin@localhost/nuvem_desafio')
    conn = psycopg2.connect(dbname='nuvem_desafio', user='postgres', password='admin', host='localhost')
    cursor = conn.cursor()
    return engine, conn, cursor

def executar_script_sql(cursor, caminho_script):
    """
    Executa um script SQL usando o cursor psycopg2 fornecido.

    Args:
    - cursor: Objeto do cursor psycopg2
    - caminho_script: Caminho para o arquivo de script SQL
    """
    with open(caminho_script, 'r') as f:
        cursor.execute(f.read())

def processar_arquivos_csv(arquivos_entrada, arquivo_saida):
    """
    Processa arquivos CSV, realiza a limpeza e tratamento dos dados e concatena em um novo arquivo CSV.

    Args:
    - arquivos_entrada: Lista de caminhos dos arquivos CSV de entrada
    - arquivo_saida: Caminho do arquivo CSV de saída
    """
    with open(arquivo_saida, 'w', newline='', encoding='utf-8') as csvoutput:
        writer = csv.writer(csvoutput)

        for arquivo_entrada in arquivos_entrada:
            with open(arquivo_entrada, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                if arquivos_entrada.index(arquivo_entrada) == 0:
                    fieldnames = next(reader)
                    writer.writerow(fieldnames)

                next(reader)
                for linha in reader:
                    if len(linha) == 1:
                        pass
                    else:
                        if len(linha) == 7:
                            # Tratamento de erro A e B
                            if len(linha[5]) == 2:
                                linha[4] = f"{linha[4]}.{linha[5][:2]}"
                                linha[5] = linha[5][2:]
                                linha = [item for item in linha if item != '']
                            else:
                                linha[5] = f"{linha[5]}.{linha[6]}"
                                linha[6] = ''
                                linha = [item for item in linha if item != '']
                            writer.writerow(linha)
                        else:
                            writer.writerow(linha)

def validar_data_hora(string_data_hora):
    """
    Valida se a string representa uma data e hora no formato esperado.

    Args:
    - string_data_hora: String a ser validada

    Retorna:
    - True se for uma data e hora válida, False caso contrário
    """
    try:
        datetime_obj = datetime.strptime(string_data_hora, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False

def processar_dataframe(df):
    """
    Realiza o processamento do DataFrame, renomeia colunas, ajusta datas e trata valores inválidos.

    Args:
    - df: DataFrame a ser processado

    Retorna:
    - DataFrame processado
    """
    df.rename(columns={'soliciado_em': 'solicitado_em'}, inplace=True)
    df['solicitado_em'] = '2021-11-04 ' + df['solicitado_em'].str.split(' ').str[1]
    df['executado_em'] = '2021-12-03 ' + df['executado_em'].str.split(' ').str[1]

    df['solicitado_em_valido'] = df['solicitado_em'].apply(validar_data_hora)
    df['executado_em_valido'] = df['executado_em'].apply(validar_data_hora)

    linhas_invalidas = df[(df['solicitado_em_valido'] == False) | (df['executado_em_valido'] == False)]

    for indice, linha in linhas_invalidas.iterrows():
        if not linha['solicitado_em_valido']:
            segundos_intervalo = (pd.to_datetime(linha['executado_em']) - pd.to_datetime(linha['solicitado_em'])).total_seconds()
            df.at[indice, 'intervalo_em_seg'] = segundos_intervalo
            df.at[indice, 'solicitado_em'] = (pd.to_datetime(linha['executado_em']) - timedelta(seconds=segundos_intervalo)).strftime("%Y-%m-%d %H:%M:%S")
        elif not linha['executado_em_valido']:
            segundos_intervalo = float(linha['intervalo_em_seg'])
            df.at[indice, 'executado_em'] = (pd.to_datetime(linha['solicitado_em']) + timedelta(seconds=segundos_intervalo)).strftime("%Y-%m-%d %H:%M:%S")

    df = df.drop(['solicitado_em_valido', 'executado_em_valido'], axis=1)
    return df

def ler_arquivo_json(caminho_arquivo_json):
    """
    Lê o conteúdo do arquivo JSON e realiza o tratamento.

    Args:
    - caminho_arquivo_json: Caminho do arquivo JSON

    Retorna:
    - Conteúdo do arquivo JSON
    """
    with open(caminho_arquivo_json, 'r', encoding='utf-8') as arquivo:
        conteudo = arquivo.read()
    conteudo_sem_chaves = re.sub(r'^\{|\}$', '', conteudo)
    return json.loads(conteudo_sem_chaves)

def carregar_dados_para_postgres(df, nome_tabela, engine):
    """
    Carrega dados do DataFrame para o PostgreSQL.

    Args:
    - df: DataFrame a ser carregado
    - nome_tabela: Nome da tabela no PostgreSQL
    - engine: Objeto de engine do SQLAlchemy
    """
    df.to_sql(nome_tabela, engine, index=False, if_exists='replace')

def realizar_dump_tabela_para_sql(host, usuario, banco_dados, nome_tabela, arquivo_saida):
    """
    Realiza o dump das tabelas no PostgreSQL para um arquivo SQL.

    Args:
    - host: Endereço do host do banco de dados PostgreSQL
    - usuario: Nome do usuário do PostgreSQL
    - banco_dados: Nome do banco de dados PostgreSQL
    - nome_tabela: Nome da tabela a ser exportada
    - arquivo_saida: Caminho do arquivo de saída SQL
    """
    subprocess.run(['pg_dump', '-h', host, '-U', usuario, '-d', banco_dados, '-t', nome_tabela, '>', arquivo_saida], shell=True)

def particionar_parquet(df, coluna_particao, pasta_saida):
    """
    Realiza o particionamento por regiao do DataFrame em formato Parquet.

    Args:
    - df: DataFrame a ser particionado
    - coluna_particao: Nome da coluna utilizada para particionamento
    - pasta_saida: Pasta de saída para os arquivos Parquet particionados
    """
    df.to_parquet(pasta_saida, partition_cols=[coluna_particao])
    print(f"Arquivo Parquet gerado com sucesso!")

    
def main():
    input_files = [
        r'C:\Users\JUSSA\Desktop\case_nuvem\exercicio\arq.01.csv',
        r'C:\Users\JUSSA\Desktop\case_nuvem\exercicio\arq.02.csv'
    ]
    output_file = 'arquivo_normalizado.csv'
    engine, conn, cursor = criar_conexao_banco_dados()

    #Cria tabelas usando estrutura.sql
    executar_script_sql(cursor, r'C:\Users\JUSSA\Desktop\case_nuvem\exercicio\estrutura.sql')
    conn.commit()

    #Processa arquivos CSV
    processar_arquivos_csv(input_files, output_file)

    #Carrega o CSV tratado para o DataFrame
    df = pd.read_csv(output_file, sep=',')

    #Realiza o tratamento dos dados e retorna o DataFrame
    df = processar_dataframe(df)

    #Carrega e trata os dados regiao.json para DataFrame
    caminho_arquivo_json = r'C:\Users\JUSSA\Desktop\case_nuvem\exercicio\regiao.json'
    df_json = pd.DataFrame(ler_arquivo_json(caminho_arquivo_json))

    #Carga dos dados para o PostgreSQL
    carregar_dados_para_postgres(df, 'solicitacao', engine)
    carregar_dados_para_postgres(df_json, 'regiao', engine)

    #Dump das tabelas
    realizar_dump_tabela_para_sql('localhost', 'postgres', 'nuvem_desafio', 'solicitacao', 'dump_solicitacao.sql')
    realizar_dump_tabela_para_sql('localhost', 'postgres', 'nuvem_desafio', 'regiao', 'dump_regiao.sql')
    
    #Particionamento do Parquet
    particionar_parquet(df_json, 'name', 'Arquivos_parquet')

    #Fecha conexões
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
