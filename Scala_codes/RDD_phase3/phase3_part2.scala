// Phase 3 – RDD Operations

// =======================
// Task 1: Data Loading & Setup
// =======================
val df = spark.read.option("header","true").csv("../../Outputs/preprocessing_phase2/phase2_5_3_transformed_data/transformed_data.csv")
val rdd = df.rdd

println("Total records: " + rdd.count())
println("First record: " + rdd.first())
rdd.take(5).foreach(println)

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

// =======================
// Task 4: Validation & Summary
// =======================

// Night crimes
val nightCrimes = rdd.filter(row =>
  row.getAs[String]("Hour").toInt >= 18 ||
  row.getAs[String]("Hour").toInt <= 5
)

println("Night crimes count: " + nightCrimes.count())


// Weekend crimes
val weekendCrimes = rdd.filter(row =>
  row.getAs[String]("Weekday").toInt == 6 ||
  row.getAs[String]("Weekday").toInt == 7
)

println("Weekend crimes count: " + weekendCrimes.count())


// Distinct crime types
val uniqueCrimeTypes = rdd.map(row => row.getAs[String]("Primary Type")).distinct()

println("Unique crime types: " + uniqueCrimeTypes.count())
uniqueCrimeTypes.take(10).foreach(println)


// Extra action
println("Collected crime types:")
uniqueCrimeTypes.collect().foreach(println)

