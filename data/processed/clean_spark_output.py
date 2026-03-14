"""
==============================================================================
SCRIPT: Curățare Output Apache Spark
DESCRIERE:
Acest script caută fișierul real de date (cel care începe cu 'part-00000')
generat de motorul Apache Spark în folderul temporar. Apoi, îl mută la
nivelul directorului 'processed' și îl redenumește într-un format clar și
ușor de citit ('eth_features_final.csv').
La final, șterge automat folderul original Spark, inclusiv fișierele
inutile de tipul .crc și _SUCCESS.
==============================================================================
"""

import os
import shutil
import glob

# Căile către foldere (relative la script)
SPARK_FOLDER='/home/damian/Work/Trading/Risk_Score_ETH/eth-risk-score-analysis/data/processed/eth_features_spark'
FINAL_FILE="/home/damian/Work/Trading/Risk_Score_ETH/eth-risk-score-analysis/data/processed/eth_features_final.csv"

def clean_data():
    print("Incepem curatarea output-ului Spark...")
    # 1. Căutăm fișierul CSV real generat de Spark (cel cu nume lung)
    csv_files=glob.glob(os.path.join(SPARK_FOLDER,'part-*.csv'))

    if not csv_files:
        print("Eroare: Nu am găsit niciun fișier valid în folderul Spark.")
        return

    spark_csv=csv_files[0]
    # 2. Dacă există deja un fișier vechi 'eth_features_final.csv', îl ștergem
    if os.path.exists(FINAL_FILE):
        os.remove(FINAL_FILE)

    # 3. Mutăm și redenumim fișierul principal
    shutil.move(spark_csv,FINAL_FILE)
    print(f"✅ Fișierul a fost extras și redenumit cu succes în: {FINAL_FILE}")

    # 4. Ștergem folderul original cu restul fișierelor inutile (_SUCCESS, .crc)
    shutil.rmtree(SPARK_FOLDER)
    print("Folderul temporar Spark și fișierele .crc au fost șterse.")

if __name__=="__main__":
    clean_data()