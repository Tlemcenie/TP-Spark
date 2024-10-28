from pyspark.sql import SparkSession
from fr.hymaia.exo2.clean_functions import filter_adult_clients, join_city, extract_department, handle_corse_department

def main():
    spark = SparkSession.builder.appName("SparkCleanJob").getOrCreate()

    
    # Charger les fichiers clients et villes
    df_clients = spark.read.csv("src/resources/exo2/clients_bdd.csv", header=True, inferSchema=True)
    df_villes = spark.read.csv("src/resources/exo2/city_zipcode.csv", header=True, inferSchema=True)

    # Appliquer le filtre pour garder uniquement les clients majeurs
    df_adult_clients = filter_adult_clients(df_clients)
    # Effectuer la jointure pour ajouter la colonne 'city'
    df_clients_with_city = join_city(df_adult_clients, df_villes)
    # Extraire le département par défaut
    df_with_department = extract_department(df_clients_with_city)
    # Gérer les cas spécifiques de la Corse
    df_final = handle_corse_department(df_with_department)

    # Sauvegarder le DataFrame final avec la colonne 'departement'
    df_final.write.mode("overwrite").parquet("data/exo2/clean_with_department")

if __name__ == "__main__":
    main()
