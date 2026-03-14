
import os

os.environ["JAVA_HOME"]="/usr/lib/jvm/java-17-openjdk-amd64"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,log,lag,when,avg,stddev,abs as spark_abs,greatest
from pyspark.sql.window import Window

# Configurari de cai
INPUT_FILE='../../../data/raw/eth_usdt_1h_kucoin_2018_2025.csv'
OUTPUT_FOLDER='../../../data/processed/eth_features_spark'

def process_data():
    # 1. Initializam Apache Spark (Il facem sa foloseasca toate nucleele disponibile local)
    print("Initializam motorul Apache Spark")
    spark=SparkSession.builder.appName("ETH_Feature_Engineering").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") # Ascundem logurile inutile de la Spark

    # 2. Incarcam datele brute
    print(f"Incarcam datele din:{INPUT_FILE}")
    df=spark.read.csv(INPUT_FILE,header=True,inferSchema=True)

    # Ne asiguram ca datele sunt sortate perfect cronologic pentru calculele pe serii de timp
    w_chronological=Window.orderBy("timestamp")

    print("Calculam feature-urile matematice (Log Returns, Volume, etc.)...")

    # 3. FEATURE 1: Log Returns (Cel mai important pentru ML financiar)
    # Folosim formula ln(Close_t / Close_t-1)
    df=df.withColumn("log_return",log(col("close")/lag("close",1).over(w_chronological)))

    #4. Feature 2: :Log Volume(Calmam exploziile de volum)
    df=df.withColumn("log_volume",log(col("volume")+1))

    # ==============================================================================
    # FERESTRE DE TIMP PENTRU INDICATORI
    # ==============================================================================
    # Window pentru ultimele 14 ore (RSI)
    w_14=Window.orderBy("timestamp").rowsBetween(-13,0)
    # Window pentru ultimele 20 ore (Bollinger Bands)
    w_20=Window.orderBy("timestamp").rowsBetween(-19,0)
    # Ferestre pentru MACD (12, 26, 9)
    w_12=Window.orderBy("timestamp").rowsBetween(-11,0)
    w_26=Window.orderBy("timestamp").rowsBetween(-25,0)
    w_9=Window.orderBy("timestamp").rowsBetween(-8,0)


    print("Calculam Indicatorii Tehnici (Bollinger Bands & RSI)...")
    # 5. FEATURE 3: Bollinger Bands (20 perioade, 2 devieri standard)
    df=df.withColumn("sma_20",avg("close").over(w_20))
    df=df.withColumn("stddev_20",stddev("close").over(w_20))

    df=df.withColumn("bb_upper",col("sma_20")+(col("stddev_20")*2))
    df=df.withColumn("bb_lower",col("sma_20")-(col("stddev_20")*2))

    # Feature-ul real pt model: %B (Cat de sus/jos e pretul in interiorul benzilor)
    df=df.withColumn("bb_pct_b",(col("close")-col("bb_lower"))/(col("bb_upper")-col("bb_lower")))

    # 6. FEATURE 4: RSI Simplificat (14 perioade)
    # Pasul A: Diferenta de pret
    df=df.withColumn("price_diff",col("close")-lag("close",1).over(w_chronological))

    # Pasul B: Separam castigurile (gains) de pierderi (losses)
    df=df.withColumn("gain",when(col("price_diff")>0,col("price_diff")).otherwise(0))
    df=df.withColumn("loss",when(col("price_diff")<0,spark_abs(col("price_diff"))).otherwise(0))

    # Pasul C: Media castigurilor si pierderilor pe 14 ore
    df=df.withColumn("avg_gain",avg("gain").over(w_14))
    df=df.withColumn("avg_loss",avg("loss").over(w_14))

    # Pasul D: Calculul Final RSI
    df=df.withColumn("rs",col("avg_gain")/when(col("avg_loss")==0,0.0001).otherwise(col("avg_loss")))
    df=df.withColumn("rsi_14",100-(100/(1+col("rs"))))

    print("Calculam FEATURE 5: MACD (12, 26, 9)...")
    # Pasul A: Mediile mobile pe 12 si 26 de ore
    df=df.withColumn("sma_12",avg("close").over(w_12))
    df=df.withColumn("sma_26",avg("close").over(w_26))

    # Pasul B: Linia MACD (diferenta dintre medii)
    df=df.withColumn("macd",col("sma_12")-col("sma_26"))

    # Pasul C: Linia de Semnal (media MACD-ului pe 9 ore)
    df=df.withColumn("macd_signal",avg("macd").over(w_9))

    # Pasul D: Histograma MACD (foarte relevanta pentru LSTM ca sa vada accelerarea)
    df=df.withColumn("macd_hist",col("macd")-col("macd_signal"))

    print("Curatam tabelul final...")
    # 7. Pastram doar coloanele relevante pentru antrenament si eliminam randurile cu NULL
    # (Primele 20 de ore vor avea NULL pentru ca nu exista destul istoric pentru a calcula media pe 20 ore)
    final_features=[
        "timestamp","datetime","close",# Pastram close pentru referinta, dar modelul se va uita la restul
        "log_return","log_volume",
        "bb_pct_b","rsi_14",
        "macd","macd_hist"
    ]

    df_final=df.select(final_features).dropna()

    # 8. Salvam datele procesate
    # Spark salveaza in mod nativ sub forma de folder cu mai multe fisiere (distribuit).
    # Pentru comoditatea de a vizualiza, il fortam sa salveze intr-un singur fisier CSV
    print(f"Salvam datele procesate in {OUTPUT_FOLDER} ...")
    df_final.coalesce(1).write.mode("overwrite").csv(OUTPUT_FOLDER,header=True)
    print("✅ Procesarea cu Apache Spark a fost finalizata cu succes!")

    spark.stop()


if __name__ == "__main__":
    process_data()