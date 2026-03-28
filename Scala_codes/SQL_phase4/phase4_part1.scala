// ============================================================
// Phase 4 – SQL Operations in Spark
// Task 1: DataFrame & Temp View Setup
// ============================================================

val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("../../Outputs/preprocessing_phase2/phase2_5_3_transformed_data/transformed_data.csv")
  
println("=" * 60)
println("Schema:")
println("=" * 60)
df.printSchema()

println("=" * 60)
println("Sample Data (5 rows):")
println("=" * 60)
df.show(5, truncate = false)

println(s"Total Records: ${df.count()}")
println(s"Total Columns: ${df.columns.length}")
println(s"Columns: ${df.columns.mkString(", ")}")

df.createOrReplaceTempView("crimes")
println("Temp view 'crimes' created successfully.")

// ============================================================
// Phase 4 – Task 2: SQL Frequency Queries
// ============================================================

// Query 1: Number of Crimes per District
println("=" * 60)
println("Query 1: Number of Crimes per District")
println("=" * 60)

val query1 = spark.sql("""
  SELECT
    District,
    COUNT(*) AS Total_Crimes,
    SUM(Arrest_Int) AS Total_Arrests,
    ROUND(SUM(Arrest_Int) * 100.0 / COUNT(*), 2) AS Arrest_Rate_Pct
  FROM crimes
  WHERE District IS NOT NULL
  GROUP BY District
  HAVING COUNT(*) > 1000
  ORDER BY Total_Crimes DESC
""")

query1.show(20)

/*
Interpretation:
- Shows the distribution of crimes across Chicago districts.
- High-crime districts represent hotspots requiring more police resources.
- Arrest_Rate_Pct reveals law enforcement effectiveness per district.
- HAVING filters out districts with less than 1000 incidents.
*/

// Query 2: Most Common Crime Types with Arrest Rate
println("=" * 60)
println("Query 2: Most Common Crime Types with Arrest Rate")
println("=" * 60)

val query2 = spark.sql("""
  SELECT
    `Primary Type` AS Crime_Type,
    COUNT(*) AS Total_Crimes,
    SUM(Arrest_Int) AS Total_Arrests,
    ROUND(SUM(Arrest_Int) * 100.0 / COUNT(*), 2) AS Arrest_Rate_Pct,
    SUM(Domestic_Int) AS Domestic_Cases
  FROM crimes
  WHERE `Primary Type` IS NOT NULL
  GROUP BY `Primary Type`
  ORDER BY Total_Crimes DESC
  LIMIT 10
""")

query2.show(10, truncate = false)

/*
Interpretation:
- Highlights the most prevalent crime types such as THEFT and BATTERY.
- Low arrest rates indicate difficulty in prosecution or lack of evidence.
- Domestic_Cases shows the volume of domestic violence within each crime type.
- Helps prioritize police training and resource allocation.
*/