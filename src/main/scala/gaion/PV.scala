package gaion

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{ZoneId, ZonedDateTime}

import com.cloudera.sparkts.{DateTimeIndex, MinuteFrequency, TimeSeriesRDD, UniformDateTimeIndex}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

//case class PV(time:String,key:String,ct: Double );

object RunModel {
  val starttime="2017-01-01 00:00:00"
  val endtime= "2017-05-31 23:55:00"
  val predictedN=288
  val outputTableName=""
  val modelName="holtwinters"
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val hiveColumnName=List("time","key","ct")

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf= new SparkConf().setAppName("timeserial").setMaster("local")
    val sc= new SparkContext(conf)
    val sqlContext=new SQLContext(sc)

    import sqlContext.implicits._

    //create dataframe
    val trainData=getData(sc,sqlContext,"src/main/resource/data.csv")
    //val vertifyData=getData(sc,sqlContext,"src/main/resource/data2.csv")
    trainData.show()
    //vertifyData.show()


    //create DateTimeIndex
    val zone = ZoneId.systemDefault()
    var dtIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, zone),
      ZonedDateTime.of(2017, 5, 31, 23, 55, 0, 0, zone),
      new MinuteFrequency(5)
    )

    //create TimeSeriesRDD
    val trainTsrdd=TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex,trainData,hiveColumnName(0),hiveColumnName(1), hiveColumnName(2))
    trainTsrdd.foreach(println(_))

    //cache
    trainTsrdd.cache()

    //add absent value "linear", "nearest", "next", or "previous"
    val filledTrainTsrdd=trainTsrdd.fill("linear")

    //create model
    val timeSeriesModel= new TimeSeriesModel(predictedN)

    //train model
    val forcast=modelName match {
      case "arima"=>{
        println("begin train")
        val (forecast,coefficients)=timeSeriesModel.arimaModelTrain(filledTrainTsrdd)
        forecast

      }
      case "holtwinters"=>{
        //季节性参数（12或者4）
        val period=288*30
        //holtWinters选择模型：additive（加法模型）、Multiplicative（乘法模型）
        val holtWintersModelType="Multiplicative"
        val (forecast,sse)=timeSeriesModel.holtWintersModelTrain(filledTrainTsrdd,period,holtWintersModelType)
        forecast
      }
      case _=>throw new UnsupportedOperationException("Currently only supports 'ariam' and 'holtwinters")
    }

    val time=timeSeriesModel.productStartDatePredictDate(predictedN,endtime,endtime)

    forcast.map{
      row=>
        val key=row._1
        val values=row._2.toArray.mkString(",")
        (key,values)
    }.flatMap(row=>row._2.split(","))saveAsTextFile("src/main/resource/30Multiplicative")
  }


  def getTrainData(sqlContext:SQLContext):DataFrame={
    val data=sqlContext.sql(
      s"""
         |select time, 'key' as  key, ct from tmp_music.tmp_lyp_nginx_result_ct2 where time between ${starttime} and ${endtime}
       """.stripMargin)
    data
  }

  def getData(sparkContext: SparkContext,sqlContext:SQLContext,path:String):DataFrame={
    val data=sparkContext.textFile(path).map(line=>line.split(",")).map{
      line=>
        val time =sdf.parse(line(0))
        val timestamp= new Timestamp(time.getTime)
        Row(timestamp,line(1),line(2).toDouble)
    }

    val field=Seq(
      StructField(hiveColumnName(0), TimestampType, true),
      StructField(hiveColumnName(1), StringType, true),
      StructField(hiveColumnName(2), DoubleType, true)
    )
    val schema=StructType(field)
    val zonedDateDataDf=sqlContext.createDataFrame(data,schema)
    zonedDateDataDf
  }
}
