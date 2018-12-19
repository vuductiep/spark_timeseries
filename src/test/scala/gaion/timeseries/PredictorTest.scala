import java.sql.Timestamp

import gaion.timeseries.TsPredictor
import org.apache.spark.sql.SparkSession

object PredictorTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("Ts HotWinter")
      .getOrCreate()

    //create dataframe
    val orgData= spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\vuduc\\Desktop\\war\\train_key.csv")

    val pred = new TsPredictor(orgData, "ACPT_DATE","DCLR_USD_PRC",
      new Timestamp(1514732400000L),
      new Timestamp(1527087600000L),
      "nipa"
    )

    val predRes = pred.tsHotWinterPredict("multiplicative", 31, 7)
    spark.stop()
  }
}
