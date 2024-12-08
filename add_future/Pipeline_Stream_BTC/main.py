import requests
from bs4 import BeautifulSoup
from datetime import datetime
import boto3
import json
import time
import os

fireHoseClient = boto3.client('firehose',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
    region_name='us-east-1')

# Função para pegar o valor da moeda no google
def get_latest_crypto_price(coin):
    try:
        url = 'https://www.google.com/search?q=' + (coin) + ' price'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        HTML = requests.get(url, headers=headers)
        HTML.raise_for_status()
        soup = BeautifulSoup(HTML.text, 'html.parser')
        text = soup.find('div', attrs={
            'class': 'BNeawe iBp4i AP7Wnd'
        }).find('div', attrs={
            'class': 'BNeawe iBp4i AP7Wnd'
        }).text
        return text
    except Exception as e:
        print(f"Erro ao obter o preço da criptomoeda: {e}")
        return None

while(1):
    now = datetime.now()
    coleta = now.strftime("%Y-%m-%d %H:%M:%S")
    price = get_latest_crypto_price('bitcoin')
    if price is None:
        print("Não foi possível obter o preço. Tentando novamente...")
        continue
    envio = fireHoseClient.put_record(
        DeliveryStreamName='BTC_stream',
        Record={
            'Data': json.dumps({
                'price': price,
                'coleta': coleta
            }) + '\n'
        }
    )
    print(envio, price, coleta)
    time.sleep(60)
