import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import scala.math._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}
import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}
import scala.collection.mutable.HashMap

object Himanshu_Purandare_Task1 {
	def main(args: Array[String]) {
	  var start = System.nanoTime()
	  var test_data = "/home/test/SampleApp/testing_small.csv"
	  var txtFileRatings = "/home/test/SampleApp/ratings.csv"
	    if (args.length != 2) {
      txtFileRatings = "ratings.csv"
      test_data = "testing_small.csv"
    }
    else {
      txtFileRatings = args(0)
      test_data = args(1)
    }
	  val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
      val sc = new SparkContext(conf)
      var test_dataLines = sc.textFile(test_data , 2).cache()
      var txtFileLinesRatings = sc.textFile(txtFileRatings , 2).cache()
      /*test_dataLines = test_dataLines.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      txtFileLinesRatings = txtFileLinesRatings.mapPartitionsWithIndex{ (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      * 
      */
      var test_first = test_dataLines.first()
      var rate_first = txtFileLinesRatings.first()
      test_dataLines = test_dataLines.filter(f=>f!=test_first)
      txtFileLinesRatings = txtFileLinesRatings.filter(f=>f!=rate_first)
      //println("test_dataLines: "+test_dataLines.count+"  txtFileLinesRatings: "+txtFileLinesRatings.count)
      
      var ratings_Data = txtFileLinesRatings.flatMap(x => createDataMapForRatings(x))
      var test_dataMap = test_dataLines.flatMap(x => createDataMapFortestData(x))
      var test_test = ratings_Data.join(test_dataMap)
      var actual_test = test_test.flatMap(f=>createActualTestData(f._1 ._1 ,f._1 ._2 ,f._2 ._1 ))
      //actual_test.foreach(f=>println(f))
      val TestRating = actual_test.map(f => Rating(f._1 ._1 , f._1 ._2 ,f._2 ))
      var actual_train = ratings_Data.subtractByKey(actual_test)
      var ColdStart_User = actual_train.groupBy(f=>f._1 ._1 ).collect.toMap
      var ColdStart_Movie = actual_train.groupBy(f=>f._1 ._2 ).collect.toMap
      //println("count = "+actual_train.count)
      //println("actual_train: "+actual_train.count)
      
      val ratings = actual_train.map(f => Rating(f._1 ._1 , f._1 ._2 ,f._2 ))
      //println("ratings: "+ratings.count)
      
      val rank = 10
      val numIterations = 10
      val model = ALS.train(ratings, rank, numIterations, 0.06)
      val usersProducts = TestRating.map { case Rating(user, product, rate) =>
  (user, product)
}
	  //println("Size of usersProducts: ", usersProducts.count)
      val predictions_answer = model.predict(usersProducts)
      val predictions_intermediate = predictions_answer.map { case Rating(user, product, rate) =>((user, product), rate)}
	  
	  //val predictions = predictions_answer.map { case Rating(user, product, rate) =>((user, product), rate)}
	  val subtracted = actual_test.subtractByKey(predictions_intermediate)
	  val absentPredictions = subtracted.keys.flatMap(f=>getAbsentPredictions(f._1 ,f._2 ,ColdStart_User, ColdStart_Movie))
	  val predictions = predictions_intermediate.union(absentPredictions)
      //predictions.foreach(f=>println(f))
      val ratesAndPreds = TestRating.map { case Rating(user, product, rate) =>
  ((user, product), rate)
}.join(predictions)



		//ratesAndPreds.foreach(f=>println(f))

	  val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
  val err = Math.abs(r1 - r2)
  err * err
}.mean()
	var fin_list = Map[String,Int]()
	for (ii<-ratesAndPreds.collect) {
	  var err = Math.abs(ii._2 ._1 - ii._2 ._2 )
	  if (err >= 0 && err < 1) {
	    //println("Inside 1")
	    fin_list += ">=0 and <1" -> (fin_list.getOrElse(">=0 and <1", 0) + 1)
	  }
	  else if (err >= 1 && err < 2){
	    //println("Inside 2")
	    fin_list += ">=1 and <2" -> (fin_list.getOrElse(">=1 and <2", 0) + 1)
	  }
	  else if (err >= 2 && err < 3) {
	    //println("Inside 3")
	    fin_list += ">=2 and <3" -> (fin_list.getOrElse(">=2 and <3", 0) + 1)
	  }
	  else if (err >= 3 && err < 4){
	    //println("Inside 4")
	    fin_list += ">=3 and <4" -> (fin_list.getOrElse(">=3 and <4", 0) + 1)
	  }
	  else if (err >= 4){
	    //println("Inside 5")
	    fin_list += ">=4" -> (fin_list.getOrElse(">=4", 0) + 1)
	  }
	}
	//println(fin_list.size)
	
	
	var FileName = "Himanshu_Purandare_result_task1.txt"
    val pw = new PrintWriter(new File(FileName))
	pw.write("UserId,MovieId,Pred_Rating\n")
	var count = 1
	//var sz = predictions.count
	predictions.toArray.sortBy(t=>(t._1._1.toInt, t._1 ._2 )).foreach(f=>{if (count == 1) {pw.write(f._1._1 +","+f._1 ._2 +","+f._2); count=0}else{pw.write("\n"+f._1._1 +","+f._1 ._2 +","+f._2)}})
	pw.close 
	var end = ((System.nanoTime()-start)/1e9d)
	println(">=0 and <1: "+fin_list(">=0 and <1"))
	println(">=1 and <2: "+fin_list(">=1 and <2"))
	println(">=2 and <3: "+fin_list(">=2 and <3"))
	println(">=3 and <4: "+fin_list(">=3 and <4"))
	println(">=4: "+fin_list(">=4"))
	val RMSE = sqrt(MSE)
	println("RMSE = " + RMSE.toDouble)
	println("The total execution time taken is "+end+" sec.")
	}
	
	def createActualTestData(it1: Int, it2: Int, it3: Double): Map[(Int, Int), Double] = {
	  val dataMap = Map[(Int, Int), Double]((it1 -> it2) -> it3)
	return dataMap
	}
	
	def getAbsentPredictions(User: Int, Movie:Int, ColdStart_User:Map[Int,Iterable[((Int, Int), Double)]], ColdStart_Movie: Map[Int,Iterable[((Int, Int), Double)]]):scala.collection.mutable.Map[(Int,Int),Double] = {
	  var dataMap = scala.collection.mutable.Map[(Int, Int), Double]()
	  if (ColdStart_User.contains(User) && (!ColdStart_Movie.contains(Movie))) {
	  var iterab = ColdStart_User(User)
	      var SumofVal:Double = 0
	      for (ii <- iterab) {
	        SumofVal += ii._2 
	      }
	      var sizeOfUserList:Double = iterab.size
	      dataMap += (User,Movie) -> (SumofVal/sizeOfUserList)
	  }
	  else if ((!ColdStart_User.contains(User)) && ColdStart_Movie.contains(Movie)) {
	    var iterab = ColdStart_Movie(Movie)
	      var SumofVal:Double = 0
	      for (ii <- iterab) {
	        SumofVal += ii._2 
	      }
	      var sizeOfUserList:Double = iterab.size
	      dataMap += (User,Movie) -> (SumofVal/sizeOfUserList)
	  }
	  else if ((!ColdStart_User.contains(User)) && (!ColdStart_Movie.contains(Movie))) {
	    dataMap += (User,Movie) -> (2.5)
	  }
	  return dataMap
	}
	
	def createDataMapForRatings(data:String): Map[(Int, Int), Double] = {
	 val array = data.split(",")
	 val dataMap = Map[(Int, Int), Double]((array(0).toInt -> array(1).toInt) -> array(2).toDouble)
	return dataMap
	}
	
	def createDataMapFortestData(data:String): Map[(Int, Int), Double] = {
	  val array = data.split(",")
	  val dataMap = Map[(Int, Int), Double]((array(0).toInt -> array(1).toInt) -> 1.00)
	  return dataMap
	}
}