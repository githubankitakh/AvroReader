package avro_consumer



import avro_consumer.properties.{sfOptions, _}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.concurrent.duration._
import org.apache.spark.sql.functions._



object AdvanceshipmentData {



  val spark = SparkSession
    .builder
    .appName("AdvanceshipmentDataconsumer")
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  spark.udf.register("deserialize", (bytes: Array[Byte]) => kafkaAvroDeserializer.deserializeAvroData(bytes))




  def readkafka():DataFrame= {
       spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BROKERS)
      .option("subscribe", kafkatopic)
      .option("includeHeaders", "true")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss","false")
      .option("maxOffsetsPerTrigger","50000")
      .load()


  }

  // FUNCTION USED TO TRANSFORM THE STREAMING DATA FROM  JSON TO TABULAR FORMAT //

  def kafkatransformations ():DataFrame ={

    val df = readkafka()


    // USED TO FETCH OUT THE VALUE AND HEADERS FROM THE KAFKA MESSAGE BODY //

    val valueDataFrame = df.selectExpr( "deserialize(value) AS message","CAST(headers.value as STRING)")
      .select(from_json(col("message"), sparkSchema.dataType).alias("avro_data"),col("value").as("header"))
      .select(col("avro_data.*"),col("header").as("subject"))


    // USED TO EXPLODE ARRYAS AND CREATE CUSTOM COLUMNS//

    val Flattened = valueDataFrame
      .withColumn("hazmatClasses",explode_outer(col("hazmatClasses")))
      .withColumn("events",explode_outer(col("subscription.events")))
      .withColumn("CREATEDATE",current_timestamp())
      .withColumn("UPDATEDATE",current_timestamp())


    // USED TO FILTER OUT THE SPECIFIC SCHEMA/SUBJECT FROM THE  MESSAGES //

    val Filtering = Flattened.filter($"subject".like(("[%com.pb.fdr.delivery.PackageAdvancedShipmentNotice%]") ))




    Filtering.select(             // SELECT STATEMENT TO FETCH THE LISTED COLUMNS //
      col("asnDateTime"),
      col("trackingId"),
      col("baseEvs"),
      col("merchantId"),
      col("classOfService"),
      col("lastMileClassOfService"),
      col("expectedInductFacilityId"),
      col("shipFromFacilityId"),
      col("sortCode"),
      col("containerId"),
      col("mawb"),
      col("bol"),
      col("referenceNumber"),
      col("addRef1"),
      col("addRef2"),
      col("shipToAddress.name").as("shipToAddress_name"),
      col("shipToAddress.attention").as("shipToAddress_attention"),
      col("shipToAddress.address1").as("shipToAddress_address1"),
      col("shipToAddress.address2").as("shipToAddress_address2"),
      col("shipToAddress.city").as("shipToAddress_city"),
      col("shipToAddress.stateOrProvince").as("shipToAddress_stateOrProvince"),
      col("shipToAddress.country").as("shipToAddress_country"),
      col("shipToAddress.isResidential").as("shipToAddress_isResidential"),
      col("weight.measurmentValue").as("measurmentValue_weight"),
      col("weight.unitOfMeasure").as("unitOfMeasure_weight"),
      col("dimensions.length.measurmentValue").as("measurmentValue_length"),
      col("dimensions.length.unitOfMeasure").as("unitOfMeasure_length"),
      col("dimensions.width.measurmentValue").as("measurmentValue_width"),
      col("dimensions.width.unitOfMeasure").as("unitOfMeasure_width"),
      col("dimensions.height.measurmentValue").as("measurmentValue_height"),
      col("dimensions.height.unitOfMeasure").as("unitOfMeasure_height"),
      col("dimensions.girth.measurmentValue").as("measurmentValue_girth"),
      col("dimensions.girth.unitOfMeasure").as("unitOfMeasure_girth"),
      col("dimensions.isRectangular"),
      col("hazmatClasses"),
      col("subscription.emailAddress"),
      col("events"),
      col("subscription.phoneNumber"),
      col("subscription.sendEmail"),
      col("subscription.sendSms"),
      col("CREATEDATE"),
      col("UPDATEDATE"),
      col("subject")

    )

  }


  // FUNCTION USED TO WRITE DATA IN SNOWFLAKE JDBC CONNECTOR IN STREAMING FASHION//

  def writesnowflake(snowflakecredentials:Map[String,String]) = {

    val Snowflake_ready_data = kafkatransformations()

    Snowflake_ready_data
      .writeStream
      .option("checkpointLocation", "checkpoints") // TO KEEP ACCOUNT OF OFFSETS

      .foreachBatch { (batch:DataFrame,_:Long)=>
        batch.write
          .format ("snowflake")
          .options (snowflakecredentials)
          .option ("dbtable", snowflaketable)
          .mode (SaveMode.Append)
          .save()
      }

      .trigger(Trigger.ProcessingTime(10.seconds)) // TO TRIGGER THE MICROBATCHES FOR STREAMING DATA
      .start()
      .awaitTermination()
  }






  def main(args: Array[String]): Unit = {

    writesnowflake(snowflakecredentials = sfOptions)

    }



}
