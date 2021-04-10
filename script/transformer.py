
from pyspark.sql  import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,FloatType,IntegerType
from pyspark.sql import SQLContext
import sys


if __name__ == '__main__':
    bucket_root_path=sys.argv[1]
    spark = SparkSession.builder.appName("ETL").getOrCreate()
    sqlContext = SQLContext(spark)

    source_path = bucket_root_path + "data/WHO.csv"
    transformed_data_path = bucket_root_path + "/transformed-data/"

    df = spark.read.csv(source_path, header=True, sep=",")

    #simple transformation to get top most populated countries
    top_5_populated_countries = df.withColumn("Population", df["Population"].cast(IntegerType()))
    transformed_df = df.withColumn("Population", df["Population"].cast(IntegerType())).orderBy('Population',
                                                                                               ascending=False)
    # Register the DataFrame as a table.
    transformed_df.registerTempTable("who_table")
    # SQL can be run over DataFrames that have been registered as a table.
    results = sqlContext.sql("SELECT * FROM who_table LIMIT 5")

    results.write.format("parquet").save(transformed_data_path)
