// ================================================================
//  IT462 Big Data Systems - Phase 2: Data Preprocessing
//  Chicago Crime Pattern Analysis
//  Unified executable pipeline: Cleaning -> Reduction -> Transformation
//  Output: transformed_data.csv folder
// ================================================================

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler}
import org.apache.spark.ml.Pipeline

object DataPreprocessing {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Phase 2 - Data Preprocessing")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // ==============================
    // 0. Paths
    // ==============================
    // Change this path to the raw Chicago Crime CSV path on your device.
    val inputPath = if (args.length > 0) args(0)
      else "C:/Users/Aljwh/OneDrive/Documents/IT_KSU/Courses/Level8/IT462 BD/Chicago_crime/Dataset/Crimes_-_2001_to_Present_20260311.csv"

    // This output name is intentionally clear because Phase 4 reads transformed_data.csv.
    val outputPath = if (args.length > 1) args(1)
      else "C:/Users/Aljwh/OneDrive/Documents/IT_KSU/Courses/Level8/IT462 BD/Chicago_crime/Outputs/preprocessing_phase2/transformed_data.csv"

    println("Spark session started successfully.")
    println(s"Input path: $inputPath")
    println(s"Output path: $outputPath")

    // ==============================
    // 1. Load Raw Dataset
    // ==============================
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    val rawRows = rawDF.count()
    val rawCols = rawDF.columns.length

    println(s"Loaded raw dataset: $rawRows rows, $rawCols columns")

    // ==============================
    // 2. Data Cleaning
    // ==============================
    var dfCleaned = rawDF.na.drop("all")

    val missingLatLonBefore = rawDF.filter(col("Latitude").isNull || col("Longitude").isNull).count()
    val missingLocationBefore = rawDF.filter(col("Location Description").isNull).count()

    val importantCols = Seq("ID", "Date", "Primary Type", "Latitude", "Longitude")
    importantCols.foreach { c =>
      dfCleaned = dfCleaned.filter(col(c).isNotNull)
    }

    dfCleaned = dfCleaned.na.fill(Map(
      "Location Description" -> "UNKNOWN",
      "Block" -> "UNKNOWN",
      "Description" -> "UNKNOWN",
      "Ward" -> 0,
      "Community Area" -> 0,
      "Beat" -> 0,
      "District" -> 0,
      "X Coordinate" -> 0,
      "Y Coordinate" -> 0
    ))

    // Deduplicate by ID to make sure each incident appears once.
    val duplicateIDsBefore = rawDF.groupBy("ID").count().filter(col("count") > 1).count()
    dfCleaned = dfCleaned.dropDuplicates("ID")

    // Parse timestamp and remove invalid dates / invalid year range.
    dfCleaned = dfCleaned.withColumn("Date", to_timestamp(col("Date")))
      .filter(col("Date").isNotNull)
      .withColumn("Year", year(col("Date")))
      .filter(col("Year").between(2001, 2026))

    // Cast geographic columns and filter Chicago bounding box.
    dfCleaned = dfCleaned
      .withColumn("Latitude", col("Latitude").cast("double"))
      .withColumn("Longitude", col("Longitude").cast("double"))

    val invalidCoordinatesBefore = dfCleaned
      .filter(!(col("Latitude").between(41.0, 43.0) && col("Longitude").between(-89.0, -87.0)))
      .count()

    dfCleaned = dfCleaned
      .filter(col("Latitude").between(41.0, 43.0))
      .filter(col("Longitude").between(-89.0, -87.0))

    // Standardize important text columns.
    Seq("Primary Type", "Location Description", "Block").foreach { c =>
      dfCleaned = dfCleaned.withColumn(c, upper(trim(regexp_replace(col(c), "\\s+", " "))))
    }

    // Cast booleans.
    dfCleaned = dfCleaned
      .withColumn("Arrest", col("Arrest").cast("boolean"))
      .withColumn("Domestic", col("Domestic").cast("boolean"))

    // Cap Latitude and Longitude using 1st and 99th percentiles.
    val Array(latP01, latP99) = dfCleaned.stat.approxQuantile("Latitude", Array(0.01, 0.99), 0.001)
    val Array(lonP01, lonP99) = dfCleaned.stat.approxQuantile("Longitude", Array(0.01, 0.99), 0.001)

    dfCleaned = dfCleaned
      .withColumn("Latitude",
        when(col("Latitude") < latP01, latP01)
          .when(col("Latitude") > latP99, latP99)
          .otherwise(col("Latitude")))
      .withColumn("Longitude",
        when(col("Longitude") < lonP01, lonP01)
          .when(col("Longitude") > lonP99, lonP99)
          .otherwise(col("Longitude")))

    val cleanedRows = dfCleaned.count()

    println("\n============================================================")
    println("Cleaning Summary")
    println("Issue                              Before        After")
    println("============================================================")
    println(f"Total Rows                         $rawRows%12d  $cleanedRows%12d")
    println(f"Rows Removed                       ${rawRows - cleanedRows}%12d")
    println(f"Missing Lat/Lon                    $missingLatLonBefore%12d  ${0}%12d")
    println(f"Missing Location Description       $missingLocationBefore%12d  ${0}%12d")
    println(f"Duplicate IDs                      $duplicateIDsBefore%12d  ${0}%12d")
    println(f"Invalid Coordinates                $invalidCoordinatesBefore%12d  ${0}%12d")
    println("============================================================\n")

    // ==============================
    // 3. Data Reduction
    // ==============================
    val dfSampled = dfCleaned.sample(withReplacement = false, fraction = 0.05, seed = 42)

    val selectedCols = Seq(
      "Primary Type", "Location Description", "Date", "Arrest", "Domestic",
      "District", "Community Area", "Latitude", "Longitude", "Year"
    )

    var dfReduced = dfSampled.select(selectedCols.map(col): _*)

    val beforeRowsReduction = dfCleaned.count()
    val afterRowsReduction = dfReduced.count()
    val beforeColsReduction = dfCleaned.columns.length
    val afterColsReduction = dfReduced.columns.length

    val dfAgg = dfReduced
      .groupBy("District", "Community Area", "Primary Type")
      .agg(count("*").alias("Crime_Count_Agg"))
      .orderBy(desc("Crime_Count_Agg"))

    println("Aggregation summary for crime hotspots:")
    dfAgg.show(10, truncate = false)

    println("\n+---------+------------+---------+")
    println("| Metric  | Before     | After   |")
    println("+---------+------------+---------+")
    println(f"| Rows    | $beforeRowsReduction%10d | $afterRowsReduction%7d |")
    println(f"| Columns | $beforeColsReduction%10d | $afterColsReduction%7d |")
    println("+---------+------------+---------+\n")

    // ==============================
    // 4. Data Transformation
    // ==============================
    var df = dfReduced
      .withColumn("Date", to_timestamp(col("Date")))
      .withColumn("Hour", hour(col("Date")))
      .withColumn("Weekday", dayofweek(col("Date")))
      .withColumn("Month", month(col("Date")))
      .withColumn("Arrest_Int", col("Arrest").cast("boolean").cast("int"))
      .withColumn("Domestic_Int", col("Domestic").cast("boolean").cast("int"))
      .withColumn("Latitude", col("Latitude").cast("double"))
      .withColumn("Longitude", col("Longitude").cast("double"))
      .withColumn("District", col("District").cast("int"))
      .withColumn("Community Area", col("Community Area").cast("int"))

    println("Type conversions sample:")
    df.select("Date", "Hour", "Weekday", "Month", "Arrest_Int", "Domestic_Int").show(5, truncate = false)

    // Encoding categorical variables.
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

    val encodingPipeline = new Pipeline().setStages(Array(primaryTypeIndexer, locationDescIndexer, oheEncoder))
    val encodingModel = encodingPipeline.fit(df)
    df = encodingModel.transform(df)

    println("Encoding sample:")
    df.select("Primary Type", "PrimaryType_Index", "PrimaryType_OHE", "Location Description", "LocationDesc_Index").show(5, truncate = false)

    // Target encoding: average arrest rate per crime type.
    val arrestRates = df.groupBy("Primary Type")
      .agg(avg("Arrest_Int").alias("PrimaryType_ArrestRate"))
    df = df.join(arrestRates, Seq("Primary Type"), "left")

    // Location crime count and hotspot feature.
    val locationCrimeCounts = df.groupBy("Location Description")
      .agg(count("*").alias("Location_Crime_Count"))
    df = df.join(locationCrimeCounts, Seq("Location Description"), "left")

    df = df
      .withColumn("Is_Weekend", when(col("Weekday").isin(1, 7), 1).otherwise(0))
      .withColumn("Is_Night", when(col("Hour").between(20, 23) || col("Hour").between(0, 5), 1).otherwise(0))
      .withColumn("Time_Of_Day",
        when(col("Hour").between(0, 5), "Night")
          .when(col("Hour").between(6, 11), "Morning")
          .when(col("Hour").between(12, 17), "Afternoon")
          .otherwise("Evening"))
      .withColumn("Crime_Hour_Bin", floor(col("Hour") / 4))
      .withColumn("District_Community_Key", concat(col("District"), lit("_"), col("Community Area")))
      .withColumn("Is_Hotspot_Location", when(col("Location_Crime_Count") > 50, 1).otherwise(0))

    println("Target encoding and hotspot sample:")
    df.select("Primary Type", "PrimaryType_ArrestRate", "Location Description", "Location_Crime_Count", "Is_Hotspot_Location").show(5, truncate = false)

    println("Feature engineering sample:")
    df.select("Hour", "Is_Weekend", "Is_Night", "Time_Of_Day", "Crime_Hour_Bin", "District_Community_Key").show(5, truncate = false)

    // Scaling / Normalization.
    val featuresForScaling = Array(
      "Latitude", "Longitude", "Hour", "Location_Crime_Count", "PrimaryType_ArrestRate"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featuresForScaling)
      .setOutputCol("features_vec")
      .setHandleInvalid("skip")

    val scaler = new MinMaxScaler()
      .setInputCol("features_vec")
      .setOutputCol("features_scaled")

    val scalingPipeline = new Pipeline().setStages(Array(assembler, scaler))
    val scalingModel = scalingPipeline.fit(df)
    df = scalingModel.transform(df)

    println("Scaling sample:")
    df.select("Latitude", "Longitude", "Hour", "features_scaled").show(5, truncate = false)

    // ==============================
    // 5. Final Dataset and Export
    // ==============================
    val finalCols = Seq(
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
      "PrimaryType_Index", "LocationDesc_Index",
      "features_vec", "features_scaled"
    )

    val dfFinal = df.select(finalCols.map(col): _*)

    println("Final preprocessed dataset schema:")
    dfFinal.printSchema()

    println("Final preprocessed dataset sample:")
    dfFinal.show(5, truncate = false)

    dfFinal.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(outputPath)

    println(s"Final transformed data generated successfully at: $outputPath")
    println("This file/folder is the generated transformed_data.csv used in later phases.")

    spark.stop()
    println("Phase 2 Data Preprocessing completed successfully.")
  }
}
