from pyspark.sql import SparkSession
from fr.hymaia.exo2.aggregate_functions import calculate_population_by_department

def main():
    spark = SparkSession.builder.appName("SparkAggregateJob").getOrCreate()

    # Charger le fichier généré par le job clean
    df_with_department = spark.read.parquet("data/exo2/clean_with_department")

    # Calculer la population par département
    df_population_by_department = calculate_population_by_department(df_with_department)

    # Sauvegarder le résultat dans un fichier CSV
    df_population_by_department.coalesce(1).write.mode("overwrite").csv("data/exo2/aggregate", header=True)

if __name__ == "__main__":
    main()
