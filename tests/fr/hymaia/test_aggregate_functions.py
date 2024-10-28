import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType, IntegerType
from fr.hymaia.exo2.aggregate_functions import calculate_population_by_department

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

def test_calculate_population_by_department(spark):
    data = [("Alice", 25, "75"), ("Bob", 18, "75"), ("Charlie", 20, "2A")]
    df = spark.createDataFrame(data, ["name", "age", "departement"])
    result_df = calculate_population_by_department(df)
    
    # Vérification du schéma
    expected_schema = ["departement", "nb_people"]
    assert result_df.schema.fieldNames() == expected_schema
    
    # Vérification des types de données
    assert result_df.schema["departement"].dataType == StringType()
    assert result_df.schema["nb_people"].dataType == LongType()

    # Vérification du contenu
    assert result_df.filter(col("departement") == "75").first()["nb_people"] == 2
