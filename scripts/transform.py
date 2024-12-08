import os
import glob
import pandas as pd
from datetime import datetime

def transform(): 
    # Verificar qual delimitador é em cada CSV
    def infer_delimiter(text):
        delimiters = [';', '\t', ',']
        for delimiter in delimiters:
            sample = text.splitlines()[0]
            if delimiter in sample:
                return delimiter
        return ','  # Default delimiter

    # Verificar qual Encoding do CSV
    def check_enconding(csv):
        import chardet
        with open(csv, 'rb') as file:
            result = (chardet.detect(file.read())) 
            return result['encoding']

    download_dir = "/home/airflow/Documentos/airflowFIAP/dags/ibov_techchallenge_dois/csv/"


    # Encontrar o arquivo CSV mais recente no diretório de download
    list_of_files = glob.glob(os.path.join(download_dir, '*.csv'))
    latest_file = max(list_of_files, key=os.path.getctime)


    # Extrair a data do nome do arquivo
    file_name = os.path.basename(latest_file)  # Extrai somente o nome do arquivo do caminho completo
    date_str = file_name.split('_')[1].split('.')[0]  # Extrai a parte da data do nome do arquivo
    date = pd.to_datetime(date_str, format='%d-%m-%y').date()  # Converte a string de data para um objeto datetime.date


    enconding = check_enconding(latest_file) # retorna 'ISO-8859-1'
    #enconding = 'ISO-8859-1'

    # Ler todas as linhas do arquivo CSV, exceto as duas últimas
    with open(latest_file, 'r', encoding=enconding) as file:
        lines = file.readlines()

    # Pegar a segunda linha (índice 1) para inferir o delimitador
    second_line = lines[1]

    # Inferir o delimitador a partir da segunda linha
    delimiter = infer_delimiter(second_line)

    # Ler o CSV completo usando o delimitador inferido
    df = pd.read_csv(latest_file, delimiter=delimiter, encoding=enconding, skiprows=1, index_col=False)
    df.columns = ['codigo', 'acao', 'tipo', 'qtde teorica', 'part (%)']

    # Excluir as duas últimas linhas
    df = df[:-2]

    df['Data'] = date  # Adiciona a data como uma nova coluna no DataFrame
    
    # Deletar o arquivo CSV
    os.remove(latest_file)


    df['part (%)'] = df['part (%)'].str.replace(',', '.')


    # Remover espaços em branco
    df['qtde teorica'] = df['qtde teorica'].str.strip()
    df['part (%)'] = df['part (%)'].str.strip()

    # Remover caracteres especiais
    df['qtde teorica'] = df['qtde teorica'].str.replace('.', '', regex=False)
    df['part (%)'] = df['part (%)'].str.replace(',', '.', regex=False)

    # Lidar com valores nulos
    df['qtde teorica'].replace('', '0', inplace=True)
    df['part (%)'].replace('', '0', inplace=True)

    # Converter para tipos numéricos
    df['qtde teorica'] = pd.to_numeric(df['qtde teorica'], errors='coerce')
    df['part (%)'] = pd.to_numeric(df['part (%)'], errors='coerce')

    # Salvar o DataFrame em um arquivo Parquet no diretório especificado
    parquet_dir = "/home/airflow/Documentos/airflowFIAP/dags/ibov_techchallenge_dois/parquet"
    os.makedirs(parquet_dir, exist_ok=True)  # Certifique-se de que o diretório existe
    file_path = os.path.join(parquet_dir, 'IBOV.parquet')
    df.to_parquet(file_path, index=False)

    print(f'DataFrame salvo em arquivo Parquet: {file_path} da data: {date}')

    return file_path, date


