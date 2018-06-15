"""
Program normalizes data into batches consisting of 10% deviation from latest value point 
for more effective use in machine learning when scaled into uniform values.
"""
from os import listdir
from pyspark.sql import SparkSession
import csv
import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Normalized Batch Creator") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

data_files = listdir(r"./data")	
export_dir = "./Output/"

UB = 1.1 #Define upper and lower bound
LB = 0.9


for i in range(len(data_files)):
	count = 0
	check = True
	
	df = spark.read.csv("./data/" + data_files[i], header=True)
	df.createOrReplaceTempView("mytable")
	
	T = df.agg({"T": "max"}).collect()[0]["max(T)"]
	
	while(check == True):
		
		export_csv = export_dir + str(count) + "-" + data_files[i][:-5] + ".csv"
		open(export_csv, 'a')
		
		T_Value = float(spark.sql("SELECT * FROM mytable WHERE T = '" + T + "'").collect()[0]["O"])
		X = spark.sql("SELECT * FROM mytable WHERE O > " + str(T_Value * UB) + " OR O < " + str(T_Value * LB))
		
		if not X.rdd.isEmpty():
		
			X_Time = X.agg({"T": "max"}).collect()[0]["max(T)"]
			batch = spark.sql("SELECT * FROM mytable WHERE T <= '" + T + "' AND T >= '" + X_Time + "'").toPandas()
			
			batch.to_csv(export_csv)
			T = X_Time
			df = spark.sql("SELECT * FROM mytable WHERE T <= '" + X_Time + "'")
			df.createOrReplaceTempView("mytable")
			count += 1
		
		else:
			batch = df.toPandas()
			batch.to_csv(export_csv)
			check = False
			
			
			