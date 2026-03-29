// ==============================
// Load Data
// ==============================

val df = spark.read
  .option("header", "true")
  .csv("../../Outputs/preprocessing_phase2/phase2_5_3_transformed_data/transformed_data.csv")

val rdd = df.rdd

// ==============================
// Task 2: Hour Analysis
// ==============================

val hourCounts = rdd
  .map(row => (row.getAs[String]("Hour").toInt, 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, false)

println("Top Crime Hours:")
hourCounts.take(10).foreach(println)


// ==============================
// Task 2: Weekday Analysis
// ==============================

val weekdayCounts = rdd
  .map(row => (row.getAs[String]("Weekday").toInt, 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, false)

println("Top Crime Weekdays:")
weekdayCounts.take(10).foreach(println)


// ==============================
// Task 3: Crime Type Analysis
// ==============================

val crimeTypeCounts = rdd
  .map(row => (row.getAs[String]("Primary Type"), 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, false)

println("Top Crime Types:")
crimeTypeCounts.take(10).foreach(println)


// ==============================
// Task 3: District Analysis
// ==============================

val districtCounts = rdd
  .map(row => (row.getAs[String]("District"), 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, false)

println("Top Districts:")
districtCounts.take(10).foreach(println)