package gaion

import java.text.SimpleDateFormat
import java.time.{ZoneId, ZonedDateTime}

import com.cloudera.sparkts._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes

object UsdPredictionModel {
  val starttime="2018-01-01 00:00:00"
  val endtime= "2018-06-30 00:00:00"
  val predictedN=31
  val outputTableName=""
  //val modelName="holtwinters"
  val modelName="arima"
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val hiveColumnName=List("ACPT_DATE","key","DCLR_USD_PRC")

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local")
      .appName("Ts HotWinter")
      .getOrCreate()

    //create dataframe
    val orgData= spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\vuduc\\Desktop\\war\\train_key.csv")

    //getData(sc,sqlContext,"C:\\Users\\vuduc\\Desktop\\war\\train_key.csv")
    //val vertifyData=getData(sc,sqlContext,"src/main/resource/data2.csv")
    orgData.printSchema()
    val trainData = orgData.withColumn(hiveColumnName(2), orgData.col(hiveColumnName(2)).cast(DataTypes.DoubleType))
    trainData.show()
    //vertifyData.show()

    //create DateTimeIndex
    val zone = ZoneId.systemDefault()
    var dtIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, zone),
      ZonedDateTime.of(2018, 6, 30, 0, 0, 0, 0, zone),
      new DayFrequency(1)
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
        //Seasonal parameters (12 or 4)
        val period=7
        // holtWinters selection model: additive (additive model), Multiplicative (multiplication model)
        val holtWintersModelType="Multiplicative"
        //val holtWintersModelType="additive"
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
    }.flatMap(row=>row._2.split(","))saveAsTextFile("C:\\Users\\vuduc\\Desktop\\war\\30_Multiplicative_" +
      new SimpleDateFormat("yyyy-MM-dd hh-mm-ss").format(System.currentTimeMillis()))

    spark.stop()
  }
}
