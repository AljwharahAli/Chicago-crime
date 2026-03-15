// ================================================================
//  Phase 2 - Section 5.2: Data Reduction (Scala + Spark)
//  Author: Aljwh | IT462 Big Data | King Saud University
// ================================================================

// 1️⃣ Initialize Spark Session
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Phase 2 - 5.2 Data Reduction")
  .master("local[*]")
  .getOrCreate()

println("✅ Spark session started successfully!")

// 2️⃣ Load Cleaned Dataset (output from 5.1)
val inputPath = "C:/Users/Aljwh/OneDrive/Documents/IT_KSU/Courses/Level8/IT462 BD/Chicago_crime/Outputs/preprocessing_phase2/phase2_5_1_cleaned_data/part-00000-6d24ef54-b2c4-429d-983c-90a166c4cbee-c000.csv"

val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(inputPath)

println(s"✅ Loaded cleaned dataset with ${df.count} records.")
df.show(5)

// ================================================================
//  5.2.1 SAMPLING
//  Apply 5% random sample to reduce dataset size for efficiency
// ================================================================

val dfSampled = df.sample(withReplacement = false, fraction = 0.05, seed = 42)
println(s"➡️ After 5% random sampling: ${dfSampled.count} records.")

// ================================================================
//  5.2.2 FEATURE SELECTION
//  Keep only columns relevant to crime hotspot analysis
// ================================================================

val selectedCols = Seq(
  "Primary Type",
  "Location Description",
  "Date",
  "Arrest",
  "Domestic",
  "District",
  "Community Area",
  "Latitude",
  "Longitude",
  "Year"
)

var dfReduced = dfSampled.select(selectedCols.map(col): _*)
println(s"➡️ After feature selection: ${dfReduced.columns.length} columns retained.")
dfReduced.show(5)

// ================================================================
//  5.2.3 AGGREGATION
//  Summarize crime counts by District, Community Area, Primary Type
//  to identify spatial crime patterns and hotspot concentrations.
//  NOTE: Aggregation result is used as a lookup/join — we do NOT
//  drop columns from dfReduced. The aggregated counts are joined
//  back to enrich each record.
// ================================================================

val dfAgg = dfReduced
  .groupBy("District", "Community Area", "Primary Type")
  .agg(count("*").alias("Crime_Count_Agg"))
  .orderBy(desc("Crime_Count_Agg"))

println("✅ Aggregation summary (top crime areas):")
dfAgg.show(10)

// ================================================================
//  5.2.4 QUANTITATIVE IMPACT
// ================================================================

val beforeRows    = df.count
val afterRows     = dfReduced.count
val beforeColumns = df.columns.length
val afterColumns  = dfReduced.columns.length

println("\n" + "=" * 50)
println(s"  Metric   | Before       | After")
println("=" * 50)
println(s"  Rows     | $beforeRows  | $afterRows")
println(s"  Columns  | $beforeColumns            | $afterColumns")
println("=" * 50 + "\n")

// ================================================================
//  5.2.5 SAVE REDUCED DATASET
// ================================================================

val outputPath = "C:/Users/Aljwh/OneDrive/Documents/IT_KSU/Courses/Level8/IT462 BD/Chicago_crime/Outputs/preprocessing_phase2/phase2_5_2_reduced_data"

dfReduced
  .coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .csv(outputPath)

println(s"💾 Reduced dataset saved successfully to: $outputPath")

// Finish
spark.stop()
println("✅ Step 5.2 Data Reduction completed successfully! 🎉")