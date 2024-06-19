from generate_data import generate_person, generate_sales, generate_employee

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("dataframe").getOrCreate()
    
dataframe = spark.read.csv("spark_notes/data_files/empolyee.csv",header=True)

dataframe.show()



# generate_employee(100, "spark_notes/data_files/empolyee.csv", type="csv")


# generate_person(100, "spark_notes/data_files/person.csv", type="csv")

# generate_sales(100, "spark_notes/data_files/sales", type="delta")
# generate_sales(100, "spark_notes/data_files/sales.csv", type="csv")