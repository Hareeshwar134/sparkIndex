package sparkindexPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.time.format.DateTimeFormatter

object indexObj {
  def main(args:Array[String]):Unit={
  
  val conf = new SparkConf().setAppName("First").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
		val spark=SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val df=spark.read.format("csv").load("file:///E://data//txnsheader")
		df.show()
		val indexdf=addcolumnIndex(spark,df)
		indexdf.show()
}
 
  def addcolumnIndex(spark: SparkSession,df: DataFrame)={
    spark.sqlContext.createDataFrame(
        df.rdd.zipWithIndex.map{
          case (row,index)=>Row.fromSeq(row.toSeq :+ index)
        },
        StructType(df.schema :+ StructField("index", LongType, false)))
        
  }
}