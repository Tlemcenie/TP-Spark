import pytest
from pyspark.sql import SparkSession
from fr.hymaia.exo2.spark_clean_job import main as clean_job_main
import shutil
import os

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

def test_clean_job_integration(spark, tmp_path):
    # Définitions de chemins spécifiques
    clients_path = tmp_path / "src/resources/exo2/clients_bdd.csv"
    villes_path = tmp_path / "src/resources/exo2/city_zipcode.csv"
    output_path = tmp_path / "/Users/benzenine/Documents/ESGI /Cours /Spark core/spark-handson/data/exo2/clean_with_department"
    
    # Création des répertoires nécessaires
    os.makedirs(clients_path.parent, exist_ok=True)
    os.makedirs(villes_path.parent, exist_ok=True)
    os.makedirs(output_path.parent, exist_ok=True)

    # Données de test
    clients_data = [("Alice", 17, "75001"), ("Bob", 18, "69002")]
    villes_data = [("75001", "Paris"), ("69002", "Lyon")]

    # Écriture des fichiers de test
    spark.createDataFrame(clients_data, ["name", "age", "zip"]).write.csv(str(clients_path), header=True, mode="overwrite")
    spark.createDataFrame(villes_data, ["zip", "city"]).write.csv(str(villes_path), header=True, mode="overwrite")

    # Exécution du job principal
    clean_job_main()

    # Lecture et vérification des résultats
    result_df = spark.read.parquet(str(output_path))
    assert result_df.count() > 0
    assert "departement" in result_df.columns
    
    # Suppression conditionnelle des répertoires
    if os.path.exists(clients_path.parent):
        shutil.rmtree(clients_path.parent)
    if os.path.exists(villes_path.parent):
        shutil.rmtree(villes_path.parent)
    if os.path.exists(output_path.parent):
        shutil.rmtree(output_path.parent)
