
import org.apache.spark.rdd.RDD  
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext  
import org.apache.spark.SparkContext._
import scala.io.Source
import scala.collection.mutable.Set  // variable set


object topKdoc {
    def main(args: Array[String]) {
      // Get the path
      val stopwordPath = "/home/hadoop/sparkapp/src/main/scala/datafiles/stopwords/stopwords.txt"
      val querywordPath = "/home/hadoop/sparkapp/src/main/scala/datafiles/query/query.txt"
      val inputFilePath = "/home/hadoop/sparkapp/src/main/scala/datafiles/input/*.txt"
      
      // Initialize set
      val stopSet:Set[String] = Set()
      val querySet:Set[String] = Set()
      
      val conf = new SparkConf().setAppName("Top K").setMaster("local[2]")
      val sc = new SparkContext(conf)
      
      // Read files
      val stopword = sc.textFile(stopwordPath).cache()
//      stopword.foreach(println)
      val queryword = sc.textFile(querywordPath).cache()
//      queryword.foreach(println)
      val inputFiles = sc.wholeTextFiles(inputFilePath, 1).cache()
      
      // Get stopword & queryword
      for (item <- stopword.collect){
        stopSet.add(item)
      }
      for (item <- queryword.collect){
        querySet.add(item)
      }      
//      // Another way to read stopword
//      val readFile = Source.fromFile(stopwordPath)
//      val lines = readFile.getLines
//      for (line <- lines){
//        stopSet.add(line)
//      }
//      println(stopSet)
       
      // Split 
      val inputFileSplit = inputFiles.flatMapValues(line => line.replaceAll("[^a-zA-Z0-9']"," ").toLowerCase.split(" "))
//      // print for debugging
//      inputFileSplit.take(100).foreach(println)
//      inputFileSplit.take(100).foreach(e => {
//        val (k, v) = e
//        println(k+":"+v.trim())
//      })
//      println(inputFileSplit)
      
      // Generate new pairs
      val inputFileNewSplit = inputFileSplit.map({case(flag, word) => word+"@"+flag.split("/")(9)})
//      inputFileNewSplit.take(100).foreach(println)
      
      // Fliter out stopwords
      val inputFileFilter = inputFileNewSplit.filter(x => stopSet.contains(x.split("@")(0))==false)
      
      ///////// Stage1 - Compute frequency of every word in a document  /////////
      val inputFileS1 = inputFileFilter.map(x => (x, 1))
      val s1 = inputFileS1.reduceByKey((x, y) => (x + y))
      
      ///////// Stage2 - Compute TF-IDF of every word  /////////
      val s2 = s1.map({case(x,y) => (x.split("@")(0), x.split("@")(1)+"="+y)})
      val s2_1 = s2.groupByKey
      val s2_2 = s2_1.map{case(k,v) =>
       var sum:Int = 0
       val set1:Set[String] = Set()
       val set2:Set[String] = Set()
       // For each word in each doc
       v.foreach{x =>
         sum = sum + 1
         set1.add(x)
       }
       set1.foreach{x =>
         set2.add((x.split("=")(0)+"="+((1.0+Math.log(x.split("=")(1).toDouble))*Math.log(8.0/sum))))
       }  
       (k,set2)
      }     
     val s2_3 = s2_2.flatMap{case(k, v) => v.map(x => (k+"@"+x.split("=")(0), x.split("=")(1)))}

     ///////// Stage3 - Compute normalized TF-IDF of every word w.r.t.a document  /////////
     val s3 = s2_3.map({case(x,y) => (x.split("@")(1), x.split("@")(0)+"="+y)})
//     s3.foreach(println)
//     val s3_1 = s3.groupByKey.mapValues(_.toList)
     val s3_1 = s3.groupByKey
//     s3_1.collect().foreach(println)
     val s3_2 = s3_1.map{case(k,v) =>
       var sum:Double = 0.0
       val set3:Set[String] = Set()
       val set4:Set[String] = Set()
       v.foreach{x =>
         sum = sum + (x.split("=")(1).toDouble)*(x.split("=")(1).toDouble)
         set3.add(x)
       }
       set3.foreach{x =>
         set4.add((x.split("=")(0)+"="+x.split("=")(1).toDouble/Math.sqrt(sum)))
       }  
       (k,set4)
      }
     val s3_3 = s3_2.flatMap{case(k,v) => v.map(x=>(x.split("=")(0)+"@"+k, x.split("=")(1).toDouble))}
    
     ///////// Stage4 - Compute the relevance of every document w.r.t. a query /////////
     val s4 = s3_3.filter({case(k,v) => querySet.contains(k.split("@")(0))})
     val s4_1 = s4.map{case(k,v) => (k.split("@")(1),v)}.reduceByKey((x, y) => (x + y))
    
     ///////// Stage5 - Sort documents by their relevance to the query ///////// 
     val s5 = s4_1.map{case(k,v) => (v,k)}
     val s5_1 = s5.sortByKey(false).take(3).map{case(k,v) => (v,k)}
     val s5_2 = sc.parallelize(s5_1.toSeq, 1)
//    s5_2.collect().foreach(println)
    
//     ///////// Stage6 - Output files /////////
     s1.saveAsTextFile("file:///home/hadoop/sparkapp/src/main/scala/datafiles/output1/")
     s2_3.saveAsTextFile("file:///home/hadoop/sparkapp/src/main/scala/datafiles/output2/")
     s3_3.saveAsTextFile("file:///home/hadoop/sparkapp/src/main/scala/datafiles/output3/")
     s4_1.saveAsTextFile("file:///home/hadoop/sparkapp/src/main/scala/datafiles/output4/")
     s5_2.saveAsTextFile("file:///home/hadoop/sparkapp/src/main/scala/datafiles/output5/")
     }
}