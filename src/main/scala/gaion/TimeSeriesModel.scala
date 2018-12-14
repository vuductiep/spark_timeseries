package gaion

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cloudera.sparkts.TimeSeriesRDD
import com.cloudera.sparkts.models.{ARIMA, HoltWinters}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable.ArrayBuffer

class TimeSeriesModel extends Serializable{

  //Predict the next N values
  private var predictedN=1
  //Stored table name
  private var outputTableName="timeseries_output"

  def this(predictedN:Int){
    this()
    this.predictedN=predictedN
  }

  case class Coefficient(coefficients: String,p: String,d: String,q:String,logLikelihoodCSS:String,arc:String)
  /**
    * Arima model:
    * Output its p, d, q parameters
    * Output predicted N values of its prediction
    * @param trainTsrdd
    */
  def arimaModelTrain(trainTsrdd:TimeSeriesRDD[String]): (RDD[(String,org.apache.spark.mllib.linalg.Vector)],RDD[(String,Coefficient)])={
    val predictedN=this.predictedN

    //train model
    val arimaAndVectorRdd=trainTsrdd.map{line=>
      line match {
        case (key,denseVector)=>
          (key,ARIMA.autoFit(denseVector),denseVector)
      }
    }

    /** Parameter output: actual value of p, d, q and its coefficient value, maximum likelihood estimation value, aic value **/
    val coefficients=arimaAndVectorRdd.map{line=>
      line match{
        case (key,arimaModel,denseVector)=>{
          val coefficients=arimaModel.coefficients.mkString(",")
          val p=arimaModel.p.toString
          val d=arimaModel.d.toString
          val q=arimaModel.q.toString
          val logLikelihoodCSS=arimaModel.logLikelihoodCSS(denseVector).toString
          val arc=arimaModel.approxAIC(denseVector).toString
          (key,Coefficient(coefficients,p,d,q,logLikelihoodCSS,arc))
        }
      }
    }

    //print coefficients
    coefficients.collect().map{f=>
      val key=f._1
      val coefficients=f._2
      println(key+" coefficients:"+coefficients.coefficients+"=>"+"(p="+coefficients.p+",d="+coefficients.d+",q="+coefficients.q+")"
        +"logLikelihoodCSS:"+coefficients.logLikelihoodCSS+" arc:"+coefficients.arc)
    }

    //predict
    val forecast= arimaAndVectorRdd.map{
      row=>
        val key=row._1
        val model=row._2
        val denseVector=row._3
        (key,model.forecast(denseVector,predictedN))
    }

    //print predict
    val forecastValue=forecast.map{
      _ match{
        case (key,value)=>{
          val partArray=value.toArray.mkString(",").split(",")
          var forecastArrayBuffer=new ArrayBuffer[Double]()
          var i=partArray.length-predictedN
          while(i<partArray.length){
            forecastArrayBuffer+=partArray(i).toDouble
            i=i+1
          }
          (key,Vectors.dense(forecastArrayBuffer.toArray))
        }
      }
    }
    println("Arima forecast of next "+predictedN+" observations:")
    forecastValue.foreach(println)

    //return forecastValue & coefficients
    (forecastValue,coefficients)
  }



  /**
    * Implement the HoltWinters model
    * @param trainTsrdd
    */
  def holtWintersModelTrain(trainTsrdd:TimeSeriesRDD[String],period:Int,holtWintersModelType:String): (RDD[(String,org.apache.spark.mllib.linalg.Vector)],RDD[(String,Double)]) ={

    //set parms
    val predictedN=this.predictedN

    //create model
    val holtWintersAndVectorRdd=trainTsrdd.map{
      row=>
        val key=row._1
        val denseVector=row._2
        //ts: Vector, period: Int, modelType: String = "additive", method: String = "BOBYQA"
        val model=HoltWinters.fitModel(denseVector,period,holtWintersModelType)
        (key,model,denseVector)
    }

    //create dist vector
    val predictedArrayBuffer=new ArrayBuffer[Double]()
    var i=0
    while(i<predictedN){
      predictedArrayBuffer+=i
      i=i+1
    }
    val predictedVectors=Vectors.dense(predictedArrayBuffer.toArray)

    //predict
    val forecast=holtWintersAndVectorRdd.map{
      row=>
        val key=row._1
        val model=row._2
        val denseVector=row._3
        val forcaset=model.forecast(denseVector,predictedVectors)
        (key,forcaset)
    }

    //print predict
    println("HoltWinters forecast of next "+predictedN+" observations:")
    forecast.foreach(println)

    //sse- to get sum of squared errors
    val sse=holtWintersAndVectorRdd.map{
      row=>
        val key=row._1
        val model=row._2
        val vector=row._3
        (key,model.sse(vector))
    }
    return (forecast,sse)
  }



  /**
    * Batch generation date (specific to minute seconds) for saving
    * The format is:：yyyy-MM-dd HH:mm:ss
    * @param predictedN
    * @param startTime
    * @param endTime
    */
  def productStartDatePredictDate(predictedN:Int,startTime:String,endTime:String): ArrayBuffer[String] ={
    //形成开始start到预测predicted的日期
    var dateArrayBuffer=new ArrayBuffer[String]()
    val dateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val cal1 = Calendar.getInstance()
    val cal2 = Calendar.getInstance()
    val st=dateFormat.parse(startTime)
    val et=dateFormat.parse(endTime)
    //设置训练数据中开始和结束日期
    cal1.set(st.getYear,st.getMonth,st.getDay,st.getHours,st.getMinutes,st.getSeconds)
    cal2.set(et.getYear,et.getMonth,et.getDay,et.getHours,et.getMinutes,et.getSeconds)
    //间隔差
    val minuteDiff=(cal2.getTimeInMillis-cal1.getTimeInMillis)/ (1000 * 60 * 5)+predictedN

    var iMinuteDiff = 0;
    while(iMinuteDiff<=minuteDiff){
      cal1.add(Calendar.MINUTE,5)
      //保存日期
      dateArrayBuffer+=dateFormat.format(cal1.getTime)
      iMinuteDiff=iMinuteDiff+1;

    }
    dateArrayBuffer
  }
}
