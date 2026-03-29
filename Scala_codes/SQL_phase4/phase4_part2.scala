// ============================================================
// Phase 4 – SQL Operations in Spark
// Task 1: DataFrame & Temp View Setup
// ============================================================

val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("C:/Users/ASUS/Downloads/transformed_data.csv")

df.createOrReplaceTempView("crimes")
println("Temp view 'crimes' created successfully.")
println(s"Total Records: ${df.count()}")

// ============================================================
// Task 3: Time-Based Queries
// ============================================================

// Query 3: Crime Distribution by Time of Day
println("=" * 60)
println("Query 3: Crime Distribution by Time of Day")
println("=" * 60)

val q3 = spark.sql("""
  SELECT Time_Of_Day, Is_Night, COUNT(*) AS Total_Crimes,
    SUM(Arrest_Int) AS Total_Arrests,
    ROUND(SUM(Arrest_Int) * 100.0 / COUNT(*), 2) AS Arrest_Rate_Pct,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS Pct_Of_All_Crimes
  FROM crimes
  GROUP BY Time_Of_Day, Is_Night
  ORDER BY Total_Crimes DESC
""")

q3.show()

/*
Interpretation:
- Afternoon hours recorded the highest crime volume (31.07% of all crimes).
- Night crimes carry the lowest arrest rate (20.36%), reflecting enforcement
  challenges during low-visibility hours.
- Preventive patrol resources should be concentrated during afternoon and
  evening hours for maximum impact.
*/

// Query 4: Weekend vs. Weekday Crime Patterns
println("=" * 60)
println("Query 4: Weekend vs. Weekday Crime Patterns")
println("=" * 60)

val q4 = spark.sql("""
  SELECT
    CASE WHEN Is_Weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS Day_Type,
    COUNT(*) AS Total_Crimes,
    SUM(Arrest_Int) AS Total_Arrests,
    ROUND(SUM(Arrest_Int) * 100.0 / COUNT(*), 2) AS Arrest_Rate_Pct,
    ROUND(AVG(Hour), 2) AS Avg_Crime_Hour,
    COUNT(DISTINCT `Primary Type`) AS Distinct_Crime_Types
  FROM crimes
  GROUP BY Is_Weekend
  ORDER BY Total_Crimes DESC
""")

q4.show()

/*
Interpretation:
- Weekdays account for ~72% of all crimes (302,097 cases) vs weekends (117,786).
- Weekend arrest rate is slightly lower (24.48% vs 25.29%), possibly due to
  reduced police staffing on weekends.
- Both day types cover nearly identical distinct crime categories (31 vs 32).
*/

// ============================================================
// Task 4: Advanced Query
// ============================================================

// Query 5: Top 3 Crime Types per District using RANK() Window Function
println("=" * 60)
println("Query 5: Top 3 Crime Types per District")
println("=" * 60)

val q5 = spark.sql("""
  SELECT District, Crime_Type, Total_Crimes, Total_Arrests,
    Arrest_Rate_Pct, Night_Crimes, Weekend_Crimes,
    ROUND(Night_Crimes * 100.0 / Total_Crimes, 2) AS Night_Crime_Pct,
    ROUND(Weekend_Crimes * 100.0 / Total_Crimes, 2) AS Weekend_Crime_Pct,
    district_rank
  FROM (
    SELECT
      District,
      `Primary Type` AS Crime_Type,
      COUNT(*) AS Total_Crimes,
      SUM(Arrest_Int) AS Total_Arrests,
      ROUND(SUM(Arrest_Int) * 100.0 / COUNT(*), 2) AS Arrest_Rate_Pct,
      SUM(Is_Night) AS Night_Crimes,
      SUM(Is_Weekend) AS Weekend_Crimes,
      RANK() OVER (PARTITION BY District ORDER BY COUNT(*) DESC) AS district_rank
    FROM crimes
    GROUP BY District, `Primary Type`
  ) ranked
  WHERE district_rank <= 3
  ORDER BY District, district_rank
""")

q5.show(60)

/*
Interpretation:
- Theft dominates in most districts, while Districts 11 and 15 show narcotics
  as the top crime type with near-perfect arrest rates (99.71% and 99.93%).
- Battery consistently appears as first or second ranked crime in every district.
- Night crime percentages are higher for battery and criminal damage (~40-51%)
  compared to theft (~17-30%).
- RANK() OVER (PARTITION BY District) enabled cross-district comparison in a
  single scalable query.
*/