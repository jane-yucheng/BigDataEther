val df1 = spark.read.text("cleanSource/output3/part-r-00000")

val newFrame1 =df1.withColumn("dates", split(col("value"),"\\s+").getItem(0).as("cleanDate")).withColumn("tweets", split(col("value"),"\\s+").getItem(1).as("cleanDate"))

val df3 = newFrame1.selectExpr("cast(dates as string) dates", "cast(tweets as int) tweets")
//convert dates to more standard format
val df4 = df3.withColumn("dates",date_format(to_date(col("dates"), "yyyy/MM/d"), "yyyy-MM-dd"))

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
merged.createOrReplaceTempView("MainFrame")

val dailyAverages = spark.sql(" SELECT Date, (High+Low)/2 AS DailyAverage FROM MainFrame")

dailyAverages.createOrReplaceTempView("dailyAverages")

//Add the daily average for every column that can get one to the central dataframe
spark.sql("SELECT dailyAverages.DailyAverage, MainFrame.* FROM dailyAverages INNER JOIN MainFrame ON dailyAverages.Date = MainFrame.Date")
val newFrame = spark.sql("SELECT MainFrame.*, dailyAverages.DailyAverage FROm MainFrame LEFT JOIN dailyAverages ON MainFrame.Date = dailyAverages.Date ORDER BY Date")
newFrame.createOrReplaceTempView("newMain")

//What is the overall average Price
spark.sql("SELECT AVG(DailyAverage) FROM newMain").show()
//1322.0268310546874|


//order by average price per day
spark.sql("SELECT * from newMain ORDER BY DailyAverage DESC ").show()
//get tweets on days where the price is higherthan average
spark.sql("SELECT Date, DailyAverage, Tweets FROM newMain WHERE DailyAverage > 1322.0268310546874")
//use this to work with everything instead of just date, dailyaverage, tweets
//spark.sql("SELECT * FROM newMain WHERE DailyAverage > 1322.0268310546874").show()
// spark.sql("SELECT Date, DailyAverage, Tweets FROM newMain WHERE DailyAverage < 1322.0268310546874")
//average tweets for days with below average price
spark.sql("SELECT avg(tweets) From (SELECT Date, DailyAverage, Tweets from newMain WHERE DailyAverage < 1322.0268310536874)").show()
//+-----------------+
// |      avg(tweets)|
// +-----------------+
// |5615.977419354838|
// +-----------------+
//average tweets fro days with above average price
spark.sql("SELECT avg(tweets) From (SELECT Date, DailyAverage, Tweets from newMain WHERE DailyAverage > 1322.0268310536874)").show()
//+------------------+
// |       avg(tweets)|
// +------------------+
// |29960.284653465347|
// +------------------+
//So with this we can conclude that tweets are higher on high price days
//GET AVERAGE Sales(IN DOLLARS) 
spark.sql("SELECT AVG(Sales) FROM newMain").show()
// +--------------------+
// |          avg(Sales)|
// +--------------------+
// |2.6699232175615236E7|
// +--------------------+
//This is equal to 26699232.175615236
//GET AVERAGE TWEETS for days above average number of sales
spark.sql("SELECT AVG(tweets) FROM (SELECT Date, Sales, Tweets FROM newMain WHERE Sales > 26699232.175615236)").show()
// +------------------+
// |       avg(tweets)|
// +------------------+
// |35273.237704918036|
// +------------------+
//get average tweets for days with below average number of sales
spark.sql("SELECT AVG(tweets) FROM (SELECT Date, Sales, Tweets FROM newMain WHERE Sales < 26699232.175615236)").show()
// +-----------------+
// |      avg(tweets)|
// +-----------------+
// |8947.680769230768|
// +-----------------+sp

spark.sql("SELECT avg(Sales) From (SELECT Date, DailyAverage, Sales, Tweets from newMain WHERE DailyAverage < 1322.0268310536874)").show()
// +-----------------+
// |       avg(Sales)|
// +-----------------+
// |99062.80658064516|
// +-----------------+

spark.sql("SELECT avg(Sales) From (SELECT Date, DailyAverage, Sales, Tweets from newMain WHERE DailyAverage > 1322.0268310536874)").show()
// +------------------+
// |        avg(Sales)|
// +------------------+
// |6.75212742766089E7|
// +------------------+

spark.sql("SELECT AVG(Tweets) FROM newMain").show()
// +----------------+
// |     avg(Tweets)|
// +----------------+
// |15220.5673828125|
// +----------------+

spark.sql("SELECT AVG(Sales) FROM (SELECT Date, Sales, Tweets FROM newMain WHERE Tweets > 15220.5673828125)").show()
// +-----------------+
// |       avg(Sales)|
// +-----------------+
// |908342.9714133742|
// +-----------------+

spark.sql("SELECT AVG(Sales) FROM (SELECT Date, Sales, Tweets FROM newMain WHERE Tweets > 15220.5673828125)").show()
// +-------------------+
// |         avg(Sales)|
// +-------------------+
// |7.306645921486336E7|
// +-------------------+

spark.sql("SELECT AVG(Sales),AVG(DailyAverage) FROM (SELECT Date, Sales, Tweets, DailyAverage FROM newMain WHERE Tweets  15220.5673828125)").show()

spark.sql("SELECT AVG(Sales),AVG(DailyAverage) FROM (SELECT Date, Sales, Tweets, DailyAverage FROM newMain WHERE Tweets > 15220.5673828125)").show()
// +-------------------+-----------------+
// |         avg(Sales)|avg(DailyAverage)|
// +-------------------+-----------------+
// |7.306645921486336E7|2998.619289617485|
// +-------------------+-----------------+

spark.sql("SELECT AVG(Sales),AVG(DailyAverage) FROM (SELECT Date, Sales, Tweets, DailyAverage FROM newMain WHERE Tweets < 15220.5673828125)").show()
// +-----------------+-----------------+
// |       avg(Sales)|avg(DailyAverage)|
// +-----------------+-----------------+
// |908342.9714133742|389.4541261398178|
// +-----------------+-----------------+