//this is the code for cleaning nft data
//output: 
//	column1: time
//	column2: nft_sales

import org.apache.spark.sql.functions.{trim,ltrim,rtrim,col}

var nft = spark.read.option("inferSchema", "true").csv("nft_v1.csv")
nft=nft.withColumnRenamed("_c0","time")
nft=nft.withColumnRenamed("_c1","nft_sales")
nft.createOrReplaceTempView("nft")

nft=nft.withColumn("time", regexp_replace($"time", "\\s+", ""))
val df=nft.filter(nft.col("time")=!="DateTime")


