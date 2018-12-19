package gaion.timeseries

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import java.util.Calendar

import com.cloudera.sparkts.{DateTimeIndex, DayFrequency, TimeSeriesRDD, UniformDateTimeIndex}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class TsPredictor (data: DataFrame,
                   timeSeriesCol: String, valueCol: String,
                   startTime: Timestamp, endTime: Timestamp,
                   keyVal: String
                ) {
  val keyCol = "_ts_gen_key"

  //add absent value "linear", "nearest", "next", or "previous"
  private def tsPrepareData(missingValueOpt: String) = {
    val trainData = data.withColumn(keyCol, functions.lit(keyVal))
      .withColumn(valueCol, data.col(valueCol).cast(DataTypes.DoubleType))
    trainData.show()
    trainData.printSchema()

    //create DateTimeIndex
    val startDate = Calendar.getInstance
    startDate.setTimeInMillis(startTime.getTime)
    val endDate = Calendar.getInstance
    endDate.setTimeInMillis(endTime.getTime)

    val zone = ZoneId.systemDefault()
    var dtIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(startDate.get(Calendar.YEAR), startDate.get(Calendar.MONTH) + 1, startDate.get(Calendar.DAY_OF_MONTH), 0, 0, 0, 0, zone),
      ZonedDateTime.of(endDate.get(Calendar.YEAR), endDate.get(Calendar.MONTH) + 1, endDate.get(Calendar.DAY_OF_MONTH), 0, 0, 0, 0, zone),
      new DayFrequency(1)
    )

    //create TimeSeriesRDD
    val trainTsrdd=TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex,trainData,timeSeriesCol,keyCol, valueCol)
    trainTsrdd.foreach(println(_))

    //cache
    trainTsrdd.cache()

    //add absent value "linear", "nearest", "next", or "previous"
    val filledTrainTsrdd=trainTsrdd.fill(missingValueOpt)
    filledTrainTsrdd
  }

  def tsHotWinterPredict(holtWintersModelType: String, predictStep: Integer, period: Integer): (RDD[(String,org.apache.spark.mllib.linalg.Vector)],RDD[(String,Double)]) = {
    println(startTime + " " + endTime + " " + predictStep)

    //add absent value "linear", "nearest", "next", or "previous"
    val filledTrainTsrdd = tsPrepareData("linear")

    //create model
    val timeSeriesModel= new TimeSeriesModel(predictStep)

    //train model
    // holtWinters selection model: additive (additive model), Multiplicative (multiplication model)
    val (forecast,sse) = timeSeriesModel.holtWintersModelTrain(filledTrainTsrdd,period,holtWintersModelType)
    (forecast,sse)
  }

  def tsArimaPredict(predictStep: Integer): (RDD[(String,org.apache.spark.mllib.linalg.Vector)],RDD[(String,Coefficient)]) = {
    println(startTime + " " + endTime + " " + predictStep)

    //add absent value "linear", "nearest", "next", or "previous"
    val filledTrainTsrdd = tsPrepareData("linear")

    //create model
    val timeSeriesModel= new TimeSeriesModel(predictStep)

    //train model
    // holtWinters selection model: additive (additive model), Multiplicative (multiplication model)
    val (forecast,coefficients)=timeSeriesModel.arimaModelTrain(filledTrainTsrdd)
    (forecast,coefficients)
  }
}