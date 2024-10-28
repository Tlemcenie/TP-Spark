import pyspark.sql.functions as f # type: ignore
from pyspark.sql import SparkSession # type: ignore



def main():
    spark = SparkSession.builder \
        .appName("Wordcount") \
        .master("local[*]") \
        .getOrCreate()
    
    
    df = spark.read.csv( "/Users/benzenine/Documents/ESGI /Cours /Spark core/spark-handson/src/resources/exo1/data.csv",header=True)
    result = wordcount(df, 'text')

    result.write.mode("overwrite").partitionBy("count").parquet("data/exo1/output2")

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()