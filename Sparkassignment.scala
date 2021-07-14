import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.functions.conv
import org.apache.spark.sql
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
//import org.apache.spark.sqlContext.implicits._
import org.apache.spark.sql._


import org.apache.spark.sql.types._

import org.apache.spark.sql.{DataFrame, SparkSession}

object Sparkassignment {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val sparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Sparkassignment-Training")
      .getOrCreate;



//
//    val x = sparkSession.sparkContext.parallelize(Array("b", "a", "c"))
//    val y = x.map(z => (z,1))
//    println(x.collect().mkString(", "))
//    println(y.collect().mkString(", "))

//
//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3))
//    val y = x.filter(n => n%2 == 1)
//    println(x.collect().mkString(", "))
//    println(y.collect().mkString(", "))


//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3))
//    val y = x.flatMap(n => Array(n, n*100, 42))
//    println(x.collect().mkString(", "))
//    println(y.collect().mkString(", "))


//    val x = sparkSession.sparkContext.parallelize(Array("John", "Fred", "Anna", "James"))
//    val y = x.groupBy(w => w.charAt(0))
//    println(y.collect().mkString(", "))

//
//    val x = sparkSession.sparkContext.parallelize(
//      Array(('B',5),('B',4),('A',3),('A',2),('A',1)))
//    val y = x.groupByKey()
//    println(x.collect().mkString(", "))
//    println(y.collect().mkString(", "))


//    val words = Array("one", "two", "two", "three", "three", "three")
//    val wordPairsRDD = sparkSession.sparkContext.parallelize(words).map(word => (word, 1))
//    val wordCountsWithReduce = wordPairsRDD
//      .reduceByKey(_ + _)
//      .collect()
//
//    val wordCountsWithGroup = wordPairsRDD
//      .groupByKey()
//      .map(t => (t._1, t._2.sum))
//      .collect()
//
//   wordCountsWithReduce.foreach(println)
//    wordCountsWithGroup.foreach(println)


//
//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3), 2)
//    def f(i:Iterator[Int])={ (i.sum,42).productIterator }
//    val y = x.mapPartitions(f)
//    // glom() flattens elements on the same partition
//    println("Before partition")
//       println(x.collect().mkString(","))
//
//    println("after partition")
//
//    println(y.collect().mkString(","))



//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3), 2)
//    def f(partitionIndex:Int, i:Iterator[Int]) = {
//      (partitionIndex, i.sum).productIterator
//    }
//    val y = x.mapPartitionsWithIndex(f)
//    // glom() flattens elements on the same partition
//    println(x.collect().mkString(","))
//   println(y.collect().mkString(","))

//
//
//    val x = sparkSession.sparkContext.parallelize(Array(1, 2, 3, 4, 5))
//    val y = x.sample(false, 0.4)
//    // omitting seed will yield different output
//    println(y.collect().mkString(", "))


//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3), 2)
//    val y = sparkSession.sparkContext.parallelize(Array(3,4), 1)
//    val z = x.union(y)
//  println(z.collect().mkString(","))


//    val x = sparkSession.sparkContext.parallelize(Array(("a", 1), ("b", 2)))
//    val y = sparkSession.sparkContext.parallelize(Array(("a", 3), ("a", 4), ("b", 5)))
//    val z = x.join(y)
//    println(z.collect().mkString(", "))



//        val x = sparkSession.sparkContext.parallelize(Array(1,2,3,3,4))
//    val y = x.distinct()
//    println(y.collect().mkString(", "))

//
//    val x = sparkSession.sparkContext.parallelize(Array(1, 2, 3, 4, 5), 3)
//    val y = x.coalesce(2)
//     println(x.collect().mkString("  "))
//  println(y.collect().mkString(","))


//    val x = sparkSession.sparkContext.parallelize(
//      Array("John", "Fred", "Anna", "James"))
//    val y = x.keyBy(w => w.charAt(0))
//    println(y.collect().mkString(", "))


//    import org.apache.spark.Partitioner
//    val x = sparkSession.sparkContext.parallelize(Array(('J',"James"),('F',"Fred"),
//      ('A',"Anna"),('J',"John")), 3)
//    val y = x.partitionBy(new Partitioner() {
//      val numPartitions = 2
//      def getPartition(k:Any) = {
//        if (k.asInstanceOf[Char] < 'H') 0 else 1
//      }
//    })
//    println(y.collect().mkString(","))
//
//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3))
//    val y = x.map(n=>n*n)
//    val z = x.zip(y)
//    println(z.collect().mkString(", "))


//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3,4))
//    val y = x.reduce((a,b) => a+b)
//    println(x.collect.mkString(", "))
//    println(y)


//
//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3), 2)
//    val y = x.getNumPartitions
//    val xOut = x.glom().collect()
//    println(y)

//
//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3), 2)
//    val y = x
//    println(x.collect().mkString(","))
//    println(y.collect().mkString(","))


//    parkSession.sparkContext


//        println(xout)


//    xout.foreach(x=>println(x))
//        println("Y")
//        yout.foreach(x=>println(x))



    //    val data1=sparkSession.sparkContext.parallelize(Seq(("sun",1),("mon",2),("tue",3), ("wed",4),("thus",5)))
//
//
////
//    data1.foreach(println)
//
//    val dataRDD = sparkSession.read.csv("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/test.csv").rdd
//
//
//    val dataRDD = sparkSession.sparkContext.textFile("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/test.csv")
//
//
//    dataRDD.foreach(println)
//
//    val dataRDDJson = sparkSession.read.json("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/test.json").rdd
//
//    dataRDDJson.foreach(println)
//
//    val words=sparkSession.sparkContext.parallelize(Seq("sun", "rises", "in", "the", "east", "and", "sets", "in"))
//    val wordPair = words.map(w => (w.charAt(0), w))
//    wordPair.foreach(println)
////
//
//
//    val dataRDD2 = sparkSession.sparkContext.textFile("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/test.csv")
//    val mapFile = dataRDD2.flatMap(lines => lines.split(",")).filter(value => value=="spark")
//    println(mapFile.count())
//
//
//
    val data = Seq(
      Row(8, "bat"),
      Row(64, "mouse"),
      Row(-27, "horse")
    )
//
    val schema = StructType(
      List(
        StructField("number", IntegerType, true),
        StructField("word", StringType, true)
      )
    )

    val dfTest = sparkSession.createDataFrame( sparkSession.sparkContext.parallelize(data), schema )


//
//

//    val df = sparkSession.read.options(Map("header"->"true")).csv("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/test.csv")

//    val Schema = StructType(Array(
//      StructField("Name",StringType,true),
//      StructField("Dept",StringType,true)
//    ))
//
//    val df_schema = sparkSession.read.schema(Schema).options(Map("header"->"true"))
//      .csv("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/test.csv")
//      df_schema.show(false)
//      df_schema.printSchema()


//
//    val Schema = StructType(Array(
//      StructField("Name",StringType,true),
//      StructField("Dept",StringType,true)
//    ))
//
//    val df_schema = sparkSession.read.schema(Schema).options(Map("header"->"true"))
//      .csv("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/test.csv")
//    df_schema.show(false)
//    df_schema.printSchema()
//
//    df_schema.write.format("csv").mode("overwrite").option("header", "false").save("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/Test_test_GraphDb/")
//
////    df_schema.explain()

//    val x=sparkSession.sparkContext.parallelize(Array(1,2,3),2)
//    def f(i:Iterator[Int])=
//    {
//      (i.sum,42).productIterator
//
//    }

//    val y=x.mapPartitions(f)
//
//    val xout=x.collect()
//    val yout=y.collect()
//
//    println("X")
//
//    xout.foreach(x=>println(x))
//    println("Y")
//    yout.foreach(x=>println(x))


//    val x = sparkSession.sparkContext.parallelize(Array(1, 2, 3, 4, 5))
//
//    val y = x.sample(false, 0.4)
//
//
//
//        y.foreach(x=>println(x))
//    //

//
//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3))
//    val y = x.map(n=>n*n)
//    val z = x.zip(y)
//    println(z.collect().mkString(", "))

//
//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3), 2)
//    val y = x.partitions.size
//    val xOut = x.glom().collect()
//    println(y)

//
//    val x =sparkSession.sparkContext.parallelize(Array(1,2,3), 2)
//    val y = x.collect()
//     println(x.collect().mkString(","))


//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3,4))
//    val y = x.reduce((a,b) => a+b)
//    println(x.collect.mkString(", "))
//    println(y)



//    def seqOp = (data:(Array[Int], Int), item:Int) =>
//      (data._1 :+ item, data._2 + item)
//    def combOp = (d1:(Array[Int], Int), d2:(Array[Int], Int)) =>
//      (d1._1.union(d2._1), d1._2 + d2._2)
//    val x = sparkSession.sparkContext.parallelize(Array(1,2,3,4))
//    val y = x.aggregate((Array[Int](), 0))(seqOp, combOp)
//    println(y)


//val x =sparkSession.sparkContext.parallelize(Array(2,4,1))
//    val y = x.max
//    println(x.collect().mkString(", "))
//    println(y)


//    val x = sparkSession.sparkContext.parallelize(Array(2,4,1))
//    val y = x.sum
//    println(x.collect().mkString(", "))
//    println(y)


//    val x =sparkSession.sparkContext.parallelize(Array(2,4,1))
//    val y = x.mean
//    println(x.collect().mkString(", "))
//    println(y)


//    val x = sparkSession.sparkContext.parallelize(Array(2,4,1))
//    val y = x.stdev
//    println(x.collect().mkString(", "))
//    println(y)


    val x = sparkSession.sparkContext.parallelize(Array(('J',"James"),('F',"Fred"),
      ('A',"Anna"),('J',"John")))
    val y = x.countByKey()
    println(y)


//    df_schema.write.format("csv").mode("overwrite").option("header", "false")
//      .save("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/Test_test_GraphDb/")
//
//
//
//    val dataRDD = sparkSession.sparkContext.textFile("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/test.csv")



    //df_schema.write.mode("overwrite").insertInto("prd_inv_tmp2.item_sample_spark_1")


    //    val dataDFson = sparkSession.read.json("/Users/z002w76/IdeaProjects/Sparkassignment-Training/src/resources/test.json").toDF()
//
    // dataDFson.printSchema()










    sys.exit(0)


    import sparkSession.implicits._

    val inputDF:DataFrame = sparkSession.read.option("inferSchema", true).option("header", true).csv("/Users/z002w76/Documents/spark_assignment.csv")


    val Hostname:DataFrame = inputDF.withColumn("hostname", split(col("url"), "/")(2))


    //getting hostname from url


    Hostname.limit(3).show(false)

    //Filter based on job title manager

    val manager=Hostname.filter(lower(col("job_title")).rlike("manager"))


    println(manager.count())

    // To get the salary frequency

    val Salary_FreqNo=inputDF.withColumn("FreqNo", when(col("payments_frequency")==="Often",4)
      when(col("payments_frequency")==="Once",1)
      when(col("payments_frequency")==="Monthly",12)
      when(col("payments_frequency")==="Seldom",2)
      when(col("payments_frequency")==="Never",0)
      when(col("payments_frequency")==="Daily",365)
      when(col("payments_frequency")==="Yearly",1)
      when(col("payments_frequency")==="Weekly",52))


    val Salary_FreqNo_Double=Salary_FreqNo.withColumn("SalDouble",(substring_index(col("salary"),"$",-1))
      .cast(sql.types.DoubleType))

    //Total salary

    val TotalSalary=Salary_FreqNo_Double.withColumn("Total_Salary",(col("SalDouble") * col("FreqNo")))

    //Max salary

    val MaxGender_Salary=TotalSalary.groupBy("gender").agg(max("Total_Salary"))


    MaxGender_Salary.show()

    // hex to RGB conversion

    val hexact=inputDF.withColumn("hexNo",(substring_index(col("hex_colour"),"#",-1)))

    val hexact_to_R=hexact.withColumn("R",conv(substring(col("hexNo"),1,2),16,10))

    val hexact_RG=hexact_to_R.withColumn("G",conv(substring(col("hexNo"),3,2),16,10))


    val hexact_to_RGB=hexact_RG.withColumn("B",conv(substring(col("hexNo"),5,2),16,10))

    val distinct_freq=hexact_to_RGB.dropDuplicates("hexNo", "hex_colour","R","G","B").select("hexNo", "hex_colour","R","G","B")


    distinct_freq.limit(3).show(false)


  }

}
