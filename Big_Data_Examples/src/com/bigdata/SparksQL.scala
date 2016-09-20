package com.bigdata

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*copying to HDFS (using linux command line)
hadoop fs -put departments.json /user/cloudera/scalaspark
* 
* 
*/
object SparksQL {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("wordCount")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //loading the jsonfile
    val departmentsJson = sqlContext.jsonFile("/user/cloudera/scalaspark/departments.json")
    //regestering the tmptable
    departmentsJson.registerTempTable("departmentsTable")
    //query department data
    val departmentsData = sqlContext.sql("select * from departmentsTable")
    departmentsData.collect().foreach(println)
  }
}