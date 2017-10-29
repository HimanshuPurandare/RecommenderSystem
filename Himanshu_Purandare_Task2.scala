import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import scala.math._

object Himanshu_Purandare_Task2 {
	def main(args: Array[String]) {
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
      var test_dataLines = sc.textFile(test_data , 4).cache()
      var txtFileLinesRatings = sc.textFile(txtFileRatings , 4).cache()
      var test_first = test_dataLines.first
      var rate_first = txtFileLinesRatings.first
      test_dataLines = test_dataLines.filter(f=>f!=test_first)
      txtFileLinesRatings = txtFileLinesRatings.filter(f=>f!=rate_first)
      var ratings_Data = txtFileLinesRatings.flatMap(x => createDataMapForRatings(x))
      var test_dataMap = test_dataLines.flatMap(x => createDataMapFortestData(x))
      var test_test = ratings_Data.join(test_dataMap)
      var actual_test = test_test.flatMap(f=>createActualTestData(f._1 ._1 ,f._1 ._2 ,f._2 ._1 ))
      var actual_train = ratings_Data.subtractByKey(actual_test)
      var solveColdStart = actual_train.groupBy(f=>f._1 ._1 ).collect.toMap
      var MoviesMappedUsers = actual_train.flatMap(f=>createMoviesMappedUsers(f._1 ._1 ,f._1 ._2 ,f._2 ))
      var groupMoviesMappedUsers = MoviesMappedUsers.groupByKey()
      var AverageMapped = groupMoviesMappedUsers.mapPartitions(iterator => getAverage(iterator).iterator)
      var AverageMappedMap = AverageMapped.collect.toMap
      var joinedAverage = groupMoviesMappedUsers.join(AverageMapped)
      var mappedSubtractedAverage = joinedAverage.mapPartitions(iterator=> subtractAverage(iterator).iterator)
      mappedSubtractedAverage.persist()
      var collectmappedSubtractedAverage = mappedSubtractedAverage.collect.toMap
      var collectactual_train = actual_train.collect.toMap
      var predictions = actual_test.keys.mapPartitions(iterator=>getPredictions(iterator, collectmappedSubtractedAverage, collectactual_train, solveColdStart, AverageMappedMap).iterator)
      
      val ratesAndPreds = actual_test.join(predictions)
      val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
  val err = Math.abs(r1 - r2)
  err * err
}.mean()
      //predictions.foreach(f=>println(f))
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
      var FileName = "Himanshu_Purandare_result_task2.txt"
    val pw = new PrintWriter(new File(FileName))
	pw.write("UserId,MovieId,Pred_Rating\n")
	var count = 1
	//var sz = predictions.count
	predictions.toArray.sortBy(t=>(t._1._1, t._1 ._2 )).foreach(f=>{if (count == 1) {pw.write(f._1._1 +","+f._1 ._2 +","+f._2); count=0}else{pw.write("\n"+f._1._1 +","+f._1 ._2 +","+f._2)}})
	pw.close 
	println(">=0 and <1: "+fin_list(">=0 and <1"))
	println(">=1 and <2: "+fin_list(">=1 and <2"))
	println(">=2 and <3: "+fin_list(">=2 and <3"))
	println(">=3 and <4: "+fin_list(">=3 and <4"))
	println(">=4: "+fin_list(">=4"))
	val RMSE = sqrt(MSE)
	println("RMSE = " + RMSE.toDouble)
	}
	
	def createDataMapForRatings(data:String): scala.collection.mutable.Map[(Int, Int), Double] = {
	 val array = data.split(",")
	 val dataMap = scala.collection.mutable.Map[(Int, Int), Double]((array(0).toInt -> array(1).toInt) -> array(2).toDouble)
	return dataMap
	}
	
	def createDataMapFortestData(data:String): scala.collection.mutable.Map[(Int, Int), Double] = {
	  val array = data.split(",")
	  val dataMap = scala.collection.mutable.Map[(Int, Int), Double]((array(0).toInt -> array(1).toInt) -> 1.00)
	  return dataMap
	}
	
	def createActualTestData(it1: Int, it2: Int, it3: Double): scala.collection.mutable.Map[(Int, Int), Double] = {
	  val dataMap = scala.collection.mutable.Map[(Int, Int), Double]((it1 -> it2) -> it3)
	return dataMap
	}
	
	def createMoviesMappedUsers(UserId: Int, MovieId: Int, Rating: Double): scala.collection.mutable.Map[Int, (Int, Double)] = {
	  val dataMap = scala.collection.mutable.Map[Int, (Int, Double)](MovieId -> (UserId -> Rating))
	return dataMap
	}
	
	def getAverage(iter:Iterator[(Int, Iterable[(Int, Double)])]):scala.collection.mutable.Map[Int, Double] = {
	  var tot:Double = 0.0
	  var dataMap = scala.collection.mutable.Map[Int,Double]()
	  for (x <- iter) {
	    tot = 0.0
	    for (dd <- x._2 ) {
	      tot += dd._2 
	    }
	    dataMap += x._1 -> (tot/x._2 .size)  
	  }
	  return dataMap
	}
	
	def subtractAverage(iter: Iterator[(Int, (Iterable[(Int, Double)], Double))]):scala.collection.mutable.Map[Int, scala.collection.mutable.Map[Int, Double]] = { 
	  var dataMap = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[Int, Double]]()
		for (x <- iter) {
		  var average = x._2 ._2 
		  var temp = scala.collection.mutable.Map[Int, Double]()
		  for (dd <- x._2 ._1 ) {
		    temp += (dd._1 -> (dd._2 - average))   
		  }
		  dataMap += x._1 -> temp
		}
		return dataMap
	}
	
	def getPredictions(iter:Iterator[(Int, Int)], table: Map[Int,scala.collection.mutable.Map[Int,Double]], rate:Map[(Int, Int),Double], solveColdStart: Map[Int,Iterable[((Int, Int),Double)]], AverageMapped:Map[Int,Double]) : scala.collection.mutable.Map[(Int, Int), Double]= {
	  var dataMap = scala.collection.mutable.Map[(Int, Int), Double]()
	  for (x <- iter) {
	    var PearsonCoeff = scala.collection.mutable.Map[(Int, Int), Double]()
	    var User = x._1 
	    var Movie = x._2
	    if (table.contains(Movie)) {
	    var vect1 = table(Movie)
	    var flag:Boolean = false 
	    for (dd <- table.keys) {
	      if (dd != Movie) {
		      var vect2 = table(dd)
		      if (vect2.keySet.exists(_ == User)) {
		        flag = true
		        var Numerator:Double = 0
		        for (ss <- vect1.keys) {
		          if (vect2.keySet.exists(_ == ss)) {
		            Numerator += (vect1(ss) * vect2(ss))
		          }
		        }
		        var Den1:Double = 0
		        var Den2:Double = 0
		        for (ss <- vect1.keys) {
		          Den1 += (vect1(ss) * vect1(ss))
		        }
		        for (ss <- vect2.keys) {
		          Den2 += (vect2(ss) * vect2(ss))
		        }
		        var Denominator = sqrt((Den1 * Den2))
		        if (Denominator == 0){
		          PearsonCoeff += (dd, Movie) -> (0.000001)
		        }
		        else {
		        var PearCoeff = (Numerator/Denominator)
		        var rho:Double = 2.5 //Case Amplification
		        PearCoeff = PearCoeff * Math.pow(Math.abs(PearCoeff), rho-1) //W_dash = PearCoeff * (|PearCoeff|^(rho-1))
		        PearsonCoeff += (dd, Movie) -> PearCoeff
		        
		        }
		      }
	      }
	    }
	    if (!flag) {
	      dataMap += (User,Movie) -> AverageMapped(Movie)
	    }
	    else {
	    var newNeighbour = PearsonCoeff.toSeq.sortBy(_._2).reverse.take(20)  //Neighbourhood = 20
	    var Numer:Double = 0
	    var Denom:Double = 0
	    for (ii <- newNeighbour) {
	      Numer += rate((User,ii._1 ._1)) * ii._2 
	      Denom += abs(ii._2) 
	    }
	    var predict = (Numer/Denom)
	    if (predict < 0.0) {
	      predict = AverageMapped(Movie)
	    } 
	    dataMap += (User,Movie) -> predict
	    }
	  }
	    else {
	      //Solve Cold Start
	      if (solveColdStart.contains(User)){
	      var iterab = solveColdStart(User)
	      var SumofVal:Double = 0
	      for (ii <- iterab) {
	        SumofVal += ii._2 
	      }
	      var sizeOfUserList:Double = iterab.size
	      dataMap += (User,Movie) -> (SumofVal/sizeOfUserList)
	      }
	      else{
	        dataMap += (User,Movie) -> (2.5)
	      }
	    }
	    
	    
	  }
	  return dataMap	  
	}
}
	
	