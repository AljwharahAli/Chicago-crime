// ================================================================
//  Phase 2 - Section 5.1: Data Cleaning (Scala + Spark)
//  Author: Aljwh | IT462 Big Data | King Saud University
// ================================================================

// 1️⃣ Initialize Spark Session
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Phase 2 - 5.1 Data Cleaning")
  .master("local[*]")
  .getOrCreate()

println("✅ Spark session started successfully!")

// 2️⃣ Load Dataset
val filePath = "C:/Users/Aljwh/OneDrive/Documents/IT_KSU/Courses/Level8/IT462 BD/Chicago_crime/Dataset/Crimes_-_2001_to_Present_20260311.csv"

val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(filePath)

println(s"✅ Loaded dataset with ${df.count} records.")
df.show(5)

// 3️⃣ Remove Fully Empty Rows
var dfCleaned = df.na.drop("all")
println(s"➡️ After dropping empty rows: ${dfCleaned.count} records.")

// 4️⃣ Remove Rows with Missing Key Columns
val importantCols = Seq("ID", "Date", "Primary Type", "Latitude", "Longitude")
importantCols.foreach { c =>
  dfCleaned = dfCleaned.filter(col(c).isNotNull)
}

println(s"➡️ After removing nulls in key columns: ${dfCleaned.count} records.")

// 5️⃣ Fill Missing Text Values
dfCleaned = dfCleaned.na.fill(Map(
  "Location Description" -> "unknown",
  "Block" -> "unknown",
  "Description" -> "unknown"
))

println("✅ Missing string columns filled with 'unknown'.")

// 6️⃣ Remove Duplicate Rows
dfCleaned = dfCleaned.dropDuplicates()
println(s"✅ Removed duplicate rows. Remaining: ${dfCleaned.count} records.")

// 7️⃣ Convert ALL string columns to lowercase and clean spaces
val stringCols = dfCleaned.schema.fields
  .filter(_.dataType.simpleString == "string")
  .map(_.name)

stringCols.foreach { c =>
  dfCleaned = dfCleaned.withColumn(
    c,
    lower(trim(regexp_replace(col(c), "\\s+", " ")))
  )
}

println("✅ All string columns converted to lowercase and trimmed.")

// 8️⃣ Replace direction abbreviations in Block column
dfCleaned = dfCleaned.withColumn(
  "Block",
  regexp_replace(col("Block"), "\\bw\\b", "west")
)

dfCleaned = dfCleaned.withColumn(
  "Block",
  regexp_replace(col("Block"), "\\be\\b", "east")
)

dfCleaned = dfCleaned.withColumn(
  "Block",
  regexp_replace(col("Block"), "\\bn\\b", "north")
)

dfCleaned = dfCleaned.withColumn(
  "Block",
  regexp_replace(col("Block"), "\\bs\\b", "south")
)

println("✅ Street direction abbreviations expanded (W/E/N/S → west/east/north/south).")



// 1️⃣1️⃣ Verify Cleaning Result
dfCleaned.show(5)
dfCleaned.printSchema()

// 1️⃣2️⃣ Save Cleaned Dataset
val outputPath = "C:/Users/Aljwh/OneDrive/Documents/IT_KSU/Courses/Level8/IT462 BD/Chicago_crime/Outputs/preprocessing_phase2/phase2_5_1_cleaned_data"

dfCleaned
  .coalesce(1)   // merge output into one file
  .write
  .option("header", "true")
  .mode("overwrite")
  .csv(outputPath)

println(s"💾 Cleaned dataset saved successfully to: $outputPath")

// 1️⃣3️⃣ Finish
spark.stop()

println("✅ Step 5.1 Data Cleaning completed successfully! 🎉")