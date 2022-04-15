import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mp.feature.{VectorAssembler, StringIndexer, OneHotEncoderEstimator}
import org.apache.spark.ml.regression.{RandomForestRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegresionEvaluator


//Takes input file 
//This datasource should probably have it's first row column format modified so that slashes can be replaced with dashes. 
//This would allow the schema to infer the date
val df = spark.read.csv("cleanSource/output/out3.csv")

val newFrame =  df.withColumnRenamed("_c0", "dates").withColumnRenamed("_c1","tweets")

val df3 = newFrame.selectExpr("cast(dates as string) dates", "cast(tweets as int) tweets")
//convert dates to more standard format
val df4 = df3.withColumn("dates",date_format(to_date(col("dates"), "MM/dd/yy"), "yyyy-MM-dd"))

//TODO: join this data with other datasources to create one table and then graph them on a line graph to compare them
// We will also calculate the correlation between them as well (this will use sparks correlation function)
//So far all scala is being tested in the scala shell, this will eventually be moved to a working scala file.

//Some further data cleaning may be required for this source:
val df2 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("cleanSource/output/nftData.csv")
// make date readable
val df5 = df2.withColumn("DateTime", split(col("DateTime"),"\\s+").getItem(0).as("cleanDate"))
//One more datasource needs to be added as well (the ether price data) This is currently being cordinated with the group and should be completed soon.
val df6 = df5.withColumnRenamed("Sales (USD) (y)", "Sales")
df6.createOrReplaceTempView("df6")
spark.sql("SELECT DateTime, Sales FROM df6")
df4.createOrReplaceTempView("df4")
// spark.sql("SELECT")
spark.sql("SELECT * FROM df4 INNER JOIN df6 ON df4.dates = df6.DateTime")

val tweetsSales = spark.sql("SELECT * FROM df4 INNER JOIN df6 ON df4.dates = df6.DateTime")

val df9 = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("cleanSource/output/price.csv")

val df10 = df9.withColumn("Date", date_format(to_date(col("Date"), "MM/dd/yy"), "yyyy-MM-dd"))

df10.createOrReplaceTempView("df10")

tweetsSales.createOrReplaceTempView("merger")

val merged = spark.sql("SELECT * FROM df10 INNER JOIN merger ON df10.Date = merger.dates")

//merged has everything
