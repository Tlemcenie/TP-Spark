import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from fr.hymaia.exo2.spark_aggregate_job import main as aggregate_job_main
import shutil

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

def test_aggregate_job_integration(spark, tmp_path):
    # Création du répertoire temporaire
    (tmp_path / "data/exo2").mkdir(parents=True, exist_ok=True)
    
    clean_with_department_path = tmp_path / "/Users/benzenine/Documents/ESGI /Cours /Spark core/spark-handson/data/exo2/clean_with_department"
    aggregate_output_path = tmp_path / "/Users/benzenine/Documents/ESGI /Cours /Spark core/spark-handson/data/exo2/aggregate"

    data = [("Alice", 25, "75"), ("Bob", 18, "75"), ("Charlie", 20, "2A")]
    spark.createDataFrame(data, ["name", "age", "departement"]).write.parquet(str(clean_with_department_path), mode="overwrite")

    aggregate_job_main()

    result_df = spark.read.csv(str(aggregate_output_path), header=True, inferSchema=True)
    assert result_df.count() > 0
    assert result_df.filter(col("departement") == "75").first()["nb_people"] == 2
    shutil.rmtree(str(tmp_path / "data"))
