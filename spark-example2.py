from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Example").getOrCreate()
spark.sparkContext.setLogLevel("OFF")

# Defines the DataFrame schema
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("idade", IntegerType(), True),
    StructField("cidade", StringType(), True)
])

data = [("Pedro", 31, "São Paulo"),
         ("Vinicius", 27, "Rio de Janeiro"),
         ("Kim", 29, "Seul")]

rdd = spark.sparkContext.parallelize(data)

# Creates a DataFrame from the defined RDD and Schema
df = spark.createDataFrame(rdd, schema)

df_filtrado = df.filter(df.idade > 28)
df_filtrado.show()

spark.stop()