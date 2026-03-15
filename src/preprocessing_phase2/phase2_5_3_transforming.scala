// ================================================================
//  Phase 2 - Section 5.3: Data Transformation (Scala + Spark)
//  Author: Aljwh | IT462 Big Data | King Saud University
// ================================================================

// 1 Initialize Spark Session
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler}
import org.apache.spark.ml.Pipeline

val spark = SparkSession.builder()
  .appName("Phase 2 - 5.3 Data Transformation")
  .master("local[*]")
  .getOrCreate()

println("Spark session started successfully!")

// 2 Load Reduced Dataset (output from 5.2)
val inputPath = "C:/Users/Aljwh/OneDrive/Documents/IT_KSU/Courses/Level8/IT462 BD/Chicago_crime/Outputs/preprocessing_phase2/phase2_5_2_reduced_data/part-00000-04f9a25f-4065-49a3-a341-223f89d30c3f-c000.csv"

var df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(inputPath)

println(s"Loaded reduced dataset with ${df.count} records and ${df.columns.length} columns.")
df.show(5)

// ================================================================
//  5.3.1 TYPE CONVERSIONS
// ================================================================

df = df.withColumn("Date", to_timestamp(col("Date")))
df = df.withColumn("Hour",    hour(col("Date")))
df = df.withColumn("Weekday", dayofweek(col("Date")))
df = df.withColumn("Month",   month(col("Date")))

df = df.withColumn("Arrest_Int",   col("Arrest").cast("boolean").cast("int"))
df = df.withColumn("Domestic_Int", col("Domestic").cast("boolean").cast("int"))

df = df.withColumn("Latitude",       col("Latitude").cast("double"))
df = df.withColumn("Longitude",      col("Longitude").cast("double"))
df = df.withColumn("District",       col("District").cast("int"))
df = df.withColumn("Community Area", col("Community Area").cast("int"))

println("Type conversions completed.")
df.select("Date", "Hour", "Weekday", "Month", "Arrest_Int", "Domestic_Int").show(5)

// ================================================================
//  5.3.2 ENCODING FOR CATEGORICAL VARIABLES
// ================================================================

val primaryTypeIndexer = new StringIndexer()
  .setInputCol("Primary Type")
  .setOutputCol("PrimaryType_Index")
  .setHandleInvalid("keep")

val locationDescIndexer = new StringIndexer()
  .setInputCol("Location Description")
  .setOutputCol("LocationDesc_Index")
  .setHandleInvalid("keep")

val oheEncoder = new OneHotEncoder()
  .setInputCols(Array("PrimaryType_Index", "LocationDesc_Index"))
  .setOutputCols(Array("PrimaryType_OHE", "LocationDesc_OHE"))

val primaryModel  = primaryTypeIndexer.fit(df)
df = primaryModel.transform(df)

val locationModel = locationDescIndexer.fit(df)
df = locationModel.transform(df)

val oheModel = oheEncoder.fit(df)
df = oheModel.transform(df)

println("StringIndexer and OneHotEncoder applied.")
df.select("Primary Type", "PrimaryType_Index", "PrimaryType_OHE",
          "Location Description", "LocationDesc_Index").show(5)

// Target Encoding: PrimaryType_ArrestRate
val arrestRates = df.groupBy("Primary Type")
  .agg(avg("Arrest_Int").alias("PrimaryType_ArrestRate"))

df = df.join(arrestRates, Seq("Primary Type"), "left")

// ================================================================
//  5.3.3 FEATURE ENGINEERING
// ================================================================

val locationCrimeCounts = df.groupBy("Location Description")
  .agg(count("*").alias("Location_Crime_Count"))

df = df.join(locationCrimeCounts, Seq("Location Description"), "left")

df = df.withColumn("Is_Weekend",
  when(col("Weekday").isin(1, 7), 1).otherwise(0))

df = df.withColumn("Is_Night",
  when(col("Hour").between(20, 23) || col("Hour").between(0, 5), 1).otherwise(0))

df = df.withColumn("Time_Of_Day",
  when(col("Hour").between(0, 5),   "Night")
  .when(col("Hour").between(6, 11),  "Morning")
  .when(col("Hour").between(12, 17), "Afternoon")
  .otherwise("Evening"))

df = df.withColumn("Crime_Hour_Bin", floor(col("Hour") / 4))

df = df.withColumn("District_Community_Key",
  concat(col("District"), lit("_"), col("Community Area")))

df = df.withColumn("Is_Hotspot_Location",
  when(col("Location_Crime_Count") > 50, 1).otherwise(0))

println("Target encoding (PrimaryType_ArrestRate) applied.")
df.select("Primary Type", "PrimaryType_ArrestRate",
          "Location Description", "Location_Crime_Count",
          "Is_Hotspot_Location").show(5)

println("Feature engineering completed.")
df.select("Hour", "Is_Weekend", "Is_Night", "Time_Of_Day",
          "Crime_Hour_Bin", "District_Community_Key").show(5)

// ================================================================
//  5.3.4 SCALING / NORMALIZATION (MinMaxScaler)
// ================================================================

val featuresForScaling = Array(
  "Latitude", "Longitude", "Hour",
  "Location_Crime_Count", "PrimaryType_ArrestRate"
)

val assembler = new VectorAssembler()
  .setInputCols(featuresForScaling)
  .setOutputCol("features_vec")
  .setHandleInvalid("skip")

val scaler = new MinMaxScaler()
  .setInputCol("features_vec")
  .setOutputCol("features_scaled")

val scalingPipeline = new Pipeline().setStages(Array(assembler, scaler))
val scalingModel    = scalingPipeline.fit(df)
df = scalingModel.transform(df)

println("MinMaxScaler applied.")
df.select("Latitude", "Longitude", "Hour", "features_scaled").show(5)

// ================================================================
//  5.3.5 FINAL COLUMN ORDER - CSV (25 columns, no vector columns)
// ================================================================

val csvCols = Seq(
  "Primary Type", "Location Description", "Date", "Year",
  "Arrest", "Domestic", "District", "Community Area",
  "Latitude", "Longitude",
  "Hour", "Weekday", "Month",
  "Arrest_Int", "Domestic_Int",
  "Is_Weekend", "Is_Night", "Time_Of_Day", "Crime_Hour_Bin",
  "District_Community_Key",
  "Location_Crime_Count",
  "PrimaryType_ArrestRate",
  "Is_Hotspot_Location",
  "PrimaryType_Index", "LocationDesc_Index"
)

val dfFinal = df.select(csvCols.map(col): _*)

println("Final dataset:")
dfFinal.printSchema()
dfFinal.show(5)

// ================================================================
//  5.3.6 SAVE AS CSV
// ================================================================

val outputPath = "C:/Users/Aljwh/OneDrive/Documents/IT_KSU/Courses/Level8/IT462 BD/Chicago_crime/Outputs/preprocessing_phase2/phase2_5_3_transformed_data"

dfFinal
  .coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .csv(outputPath)

println(s"Transformed dataset saved (CSV) to: $outputPath")

spark.stop()
println("Step 5.3 Data Transformation completed successfully!")