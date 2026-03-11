
import requests
import time
import pandas as pd
from datetime import datetime,timedelta

# ==============================================================================
# DOCUMENTAȚIE PROIECT:
# Dataset-ul extrage date istorice pentru perechea ETH/USDT de pe bursa KuCoin.
# Timeframe: 1 oră (1h)
# Perioada extrasă: 01 Ianuarie 2018 (00:00:00) - 31 Decembrie 2025 (23:59:59)
# ==============================================================================

# Configurari de baza

SYMBOL='ETH-USDT'
TIMEFRAME='1hour'

# Setăm exact începutul anului 2018 și sfârșitul anului 2025
# Folosim timestamp-uri UNIX (în secunde)
START_TIME=int(datetime(2018,1,1,0,0,0).timestamp())
END_TIME=int(datetime(2025,12,31,23,59,59).timestamp())

# Calea unde vom salva fisierul, conform structurii proiectului
OUTPUT_FILE='/home/damian/Work/Trading/Risk_Score_ETH/eth-risk-score-analysis/data/raw/eth_usdt_1h_kucoin_2018_2025.csv'

def fetch_kucoin_klines(symbol,timeframe,start_at,end_at,retries=3):
    """
        Face un request catre API-ul public KuCoin pentru date OHLCV.
        Daca primeste eroare de retea (Connection Reset),
    mai incearca de cateva ori inainte sa renunte (Retry mechanism).
    """
    url="https://api.kucoin.com/api/v1/market/candles"
    params={
        'symbol':symbol,
        'type':timeframe,
        'startAt':start_at,
        'endAt':end_at
    }

    for attempt in range(retries):
        try:
            response=requests.get(url,params=params,timeout=10)
            if response.status_code==200:
                data=response.json()
                if data['code']=='200000':
                    return data['data']
                else:
                    print(f"Eroare API Kucoin: {data['msg']}")
                    return []
            else:
                print(f"Eroare HTTP: {response.status_code}. Incercam din nou...")

        except (requests.exceptions.ConnectionError,requests.exceptions.Timeout) as e:
            print(f"Eroare de conexiune (incercarea {attempt + 1}/{retries}): {e}")
            time.sleep(3) # Asteptam 3 secunde daca pica netul inainte sa reincercam

    print("Am depasit numarul maxim de incercari. Trecem mai departe (sau abandonam).")
    return []

def download_all_data():
    all_klines=[]

    # KuCoin returneaza maxim 1500 lumanari pe request.
    # 1500 ore inseamna 5.400.000 secunde. Folosim un pas sigur de 5.000.000 secunde.
    step=5000000

    current_end=END_TIME

    print(f"Incepem descarcarea datelor pentru {SYMBOL} pe {TIMEFRAME}...")
    print(f"Interval cerut: 01.01.2018 - 31.12.2025")

    while current_end>START_TIME:
        current_start=max(START_TIME,current_end-step)

        print(f"Descarcam perioada: {datetime.fromtimestamp(current_start)} -> {datetime.fromtimestamp(current_end)}")

        klines=fetch_kucoin_klines(SYMBOL, TIMEFRAME, current_start, current_end)

        if not klines:
            print("Nu s-au mai gasit date pentru aceasta perioada.")
            break
        all_klines.extend(klines)

        # Noul end_time va fi cel mai vechi timestamp din datele primite, minus 1 ora (3600 secunde)
        oldest_kline_time=int(klines[-1][0])
        current_end=oldest_kline_time-3600

        # Pauza pentru a nu lua ban de la API (Rate Limiting)
        time.sleep(1)

    print(f"S-au descarcat {len(all_klines)} randuri in total.")

    # Formatam datele intr-un DataFrame Pandas pentru a le salva frumos
    columns=['timestamp','open','close','high','low','volume','turnover']

    df=pd.DataFrame(all_klines,columns=columns)

    # Convertim timestamp in numeric, apoi sortam CRONOLOGIC (de la 2018 spre 2025)
    df['timestamp']=pd.to_numeric(df['timestamp'])
    df=df.sort_values('timestamp').reset_index(drop=True)

    # Filtram strict pentru a ne asigura ca nu avem date in afara intervalului dorit
    df=df[(df['timestamp']>=START_TIME)&(df['timestamp']<=END_TIME)]

    # Adaugam coloana cu data vizibila uman
    df['datetime']=pd.to_datetime(df['timestamp'],unit='s')

    #Salvam in fisierul CSV
    df.to_csv(OUTPUT_FILE,index=False)
    print(f"Fisierul a fost salvat cu succes in: {OUTPUT_FILE} ({len(df)} randuri finale)")



if __name__ == "__main__":
   download_all_data()
