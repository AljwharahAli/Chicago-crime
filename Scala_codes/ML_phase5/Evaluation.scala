// ── Spark KMeans Model Evaluation (Scala) ──────────────────────── 
import java.io.{FileInputStream, ObjectInputStream} 
import org.apache.spark.ml.clustering.{KMeans, KMeansModel} 
import org.apache.spark.ml.evaluation.ClusteringEvaluator 
import org.apache.spark.ml.feature.VectorAssembler 
import org.apache.spark.ml.linalg.{Vector, Vectors} 
// 1. Load data 
val rawData = spark.read.option("header","true") 
  .option("inferSchema","true").csv("C:/Users/ASUS/Downloads/transformed_data.csv") 
  
// 2. Prepare features (5 numeric features matching model) 
val featureCols = Array("PrimaryType_Index","LocationDesc_Index", 
                        "Hour","District","Month") 
val assembler = new VectorAssembler() 
  .setInputCols(featureCols).setOutputCol("features") 
  .setHandleInvalid("skip") 
val data = assembler.transform(rawData) 
  
// 3. Load saved model (Java serialized) 
val fis = new FileInputStream("kmeansModel.ser") 
val ois = new ObjectInputStream(fis) 
val model = ois.readObject().asInstanceOf[KMeansModel] 
ois.close() 
  
// 4. Apply model 
val predictions = model.transform(data) 
  
// 5. Silhouette Score 
val evaluator = new ClusteringEvaluator() 
val silhouette = evaluator.evaluate(predictions) 
  
// 6. WSSSE (manual — computeCost removed in Spark 3.x) 
def computeWSSSE(pred: DataFrame, 
                 centers: Array[Vector]): Double = 
  pred.select("features","prediction").rdd.map { row => 
    val f = row.getAs[Vector]("features") 
    val c = row.getInt(1) 
    Vectors.sqdist(f, centers(c)) 
  }.sum() 
val wssse = computeWSSSE(predictions, model.clusterCenters) 
  
// 7. Baseline model (k=2) 
val baselineFitted = new KMeans().setK(2).setSeed(42) 
  .setFeaturesCol("features").fit(data) 
val baselinePred = baselineFitted.transform(data) 
val baselineSilhouette = evaluator.evaluate(baselinePred) 
val baselineWssse = computeWSSSE(baselinePred, 
                                 baselineFitted.clusterCenters) 