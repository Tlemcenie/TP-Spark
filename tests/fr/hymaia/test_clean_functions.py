import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from fr.hymaia.exo2.clean_functions import filter_adult_clients, join_city, extract_department, handle_corse_department

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

def test_filter_adult_clients(spark):
    data = [("Alice", 17, "75001"), ("Bob", 18, "69002")]
    df = spark.createDataFrame(data, ["name", "age", "zip"])
    result_df = filter_adult_clients(df)
    
    # Vérification du schéma
    expected_schema = ["name", "age", "zip"]
    assert result_df.schema.fieldNames() == expected_schema
    
    # Vérification du contenu
    assert result_df.filter("age < 18").count() == 0
    assert result_df.count() == 1

def test_join_city(spark):
    clients_data = [("Alice", 25, "75001"), ("Bob", 18, "69002")]
    villes_data = [("75001", "Paris"), ("75002", "Lyon")]
    clients_df = spark.createDataFrame(clients_data, ["name", "age", "zip"])
    villes_df = spark.createDataFrame(villes_data, ["zip", "city"])

    result_df = join_city(clients_df, villes_df)
    
    # Vérification du schéma
    expected_schema =  ["zip", "name", "age", "city"]
    assert result_df.schema.fieldNames() == expected_schema

    # Vérification du contenu
    assert result_df.filter(col("city") == "Paris").count() == 1

def test_extract_department(spark):
    data = [("Alice", 25, "75001"), ("Bob", 18, "20100")]
    df = spark.createDataFrame(data, ["name", "age", "zip"])
    result_df = extract_department(df)
    
    # Vérification du schéma
    expected_schema = ["name", "age", "zip", "departement"]
    assert result_df.schema.fieldNames() == expected_schema

    # Vérification du contenu
    assert result_df.filter(col("departement") == "75").count() == 1

def test_handle_corse_department(spark):
    data = [("Alice", 25, "75001"), ("Bob", 18, "20100"), ("Charlie", 20, "20200")]
    df = spark.createDataFrame(data, ["name", "age", "zip"])
    df = extract_department(df)  # Ajoute la colonne 'departement' sans spécifier le département Corse
    result_df = handle_corse_department(df)
    
    # Vérification du schéma
    expected_schema = ["name", "age", "zip", "departement"]
    assert result_df.schema.fieldNames() == expected_schema

    # Vérification du contenu pour la Corse
    assert result_df.filter(col("departement") == "2A").count() == 1
    assert result_df.filter(col("departement") == "2B").count() == 1

def test_handle_corse_department_invalid_zip(spark):
    data = [("Alice", 25, "invalid_zip")]
    df = spark.createDataFrame(data, ["name", "age", "zip"])
    df = extract_department(df)  # Ajoute la colonne 'departement'
    result_df = handle_corse_department(df)
    
    # Vérification que les valeurs non numériques produisent 'None' pour 'departement'
    assert result_df.filter(col("departement").isNull()).count() == 1
