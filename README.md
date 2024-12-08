# Pipeline ETL Automatizado com Airflow e AWS
Este projeto implementa um pipeline ETL (Extract, Transform, Load) automatizado, combinando o poder do `Apache Airflow` e da `AWS` para processar, transformar e analisar dados financeiros.

O pipeline é dividido em duas grandes etapas:

1. Orquestração e Pré-processamento com Airflow:
    - Código utilizado no Linux 
    - O código Airflow gerencia o download dos dados, transformação inicial, salvamento no formato Parquet e envio para o Amazon S3 na pasta `raw/`.

2. Transformação Avançada e Consultas com AWS:
    - Após o upload dos dados, AWS Glue realiza transformações adicionais e coloca os resultados na pasta `refined/` no S3.
    - Os dados refinados são automaticamente conectados ao Amazon Athena para consultas SQL e análise de dados.

---

## 1. Pré-processamento com Apache Airflow

### Descrição do Código
O `DAG (Directed Acyclic Graph)` gerenciado pelo Airflow contém as seguintes tarefas:

1. Download dos Dados
    - Baixa os dados da página web da 
    [B3](https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br)
    , salvando-os em formato CSV temporariamente.
2. Transformação Inicial
    - Limpa e processa os dados, aplicando operações como cálculo de novas colunas e ajustes no formato.
    - Salva os dados processados em formato Parquet, conhecido por sua eficiência em análise de dados.
3. Upload para Amazon S3
    - Os arquivos são enviados para o bucket S3 na pasta `raw/`, organizados por data de coleta.

### Organização dos Dados no S3
Os dados enviados para o Amazon S3 pelo Airflow são organizados por data de coleta, assim:
```
s3://nome-do-bucket/raw/YYYY-MM-DD/IBOV.parquet
```

## 2. Transformação e Consulta com AWS
Após o upload para o S3, o pipeline na AWS é executado automaticamente:

### AWS Glue
O AWS Glue é acionado sempre que novos dados chegam à pasta `raw/`. Ele realiza:

1. Mudança no Tipo de Dados: Conversão de formatos como string para float ou datetime.
2. Renomeação de Colunas: Ajusta os nomes das colunas para padronização.
3. Sumarização: Realiza cálculos de somas e contagens.

Os dados transformados são salvos na pasta `refined/`, separados por pastas pela data da ação e pela ação, organizados assim como exemplo:
```
s3://nome-do-bucket/refined/data_acao=YYYY-MM-DD/acao=ALLOS/
```

### Amazon Athena
Os dados da pasta `refined/` são automaticamente configurados no Athena, permitindo consultas SQL diretamente sobre os dados refinados, como:

```sql
SELECT * FROM bovespa_etl_glue WHERE acao = 'ALLOS';
```

### Visualização dos Dados
A partir do Athena, é possível explorar os dados usando notebooks para visualizações.

---

## Automação Completa
### Fluxo Geral
1. **Airflow**:
    - Download e transformação inicial.
    - Envio dos dados brutos para o S3 na pasta raw/.

2. **AWS Glue**:
    - Transformação avançada dos dados.
    - Salvamento na pasta refined/ no S3.

3. **Amazon Athena**:
    - Consulta SQL nos dados refinados.

4. **Visualização**:
    - Análise dos dados em notebooks ou ferramentas de BI conectadas ao Athena.

--- 

## Vídeo Demonstrativo
Um vídeo explicativo está disponível na raíz do repositório, demonstrando o Airflow e o fluxo de dados na AWS.

## Possíveis Melhorias
- Adicionar notificações no Airflow para monitorar falhas ou conclusões de tarefas.
- Implementar testes de qualidade nos dados brutos antes do envio ao S3.
- Adicionar logs detalhados no S3 para auditoria e rastreamento de falhas.
- Adicionar a Pipeline de Stream utilizando a AWS Firehose com o código na pasta `add_future/Pipeline_Stream_BTC/main.py`
