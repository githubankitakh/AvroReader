package avro_consumer

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters

object properties {

  //  TO HOLD ALL THE OBJECTS AND VARIABLES USED IN STREAMING FUNCTIONS //

  val kafkatopic = "fdr-delivery-int-package"

  val Schemaregistrytopic = "com.pb.fdr.delivery.PackageAdvancedShipmentNotice" //"com.pb.fdr.delivery.PackageVoidInformation"//"com.pb.fdr.delivery.ConfirmShipCreateInformation"

  val BROKERS = "b-3.kafka-usea1-dev.i8gspf.c5.kafka.us-east-1.amazonaws.com:9092,b-4.kafka-usea1-dev.i8gspf.c5.kafka.us-east-1.amazonaws.com:9092,b-2.kafka-usea1-dev.i8gspf.c5.kafka.us-east-1.amazonaws.com:9092"//

  val schemaRegistryUrl = "https://schemaregistry-msk-usea1-prod.fdr.pitneycloud.com/"

  val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)

  val kafkaAvroDeserializer = new AvroDeserialize(schemaRegistryClient)

  val avroSchema  = schemaRegistryClient.getLatestSchemaMetadata(Schemaregistrytopic).getSchema


  var sparkSchema = SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))

  val snowflaketable = ""  //no table exist for this job yet so create one before running

  var sfOptions = Map(
    "sfURL" -> "https://pitneybowes.us-east-1.snowflakecomputing.com/",
    "sfAccount" -> "pitneybowes",
    "sfUser" -> "FDRREPLICATION_ETL_USER",
    "sfPassword" -> "DEDjjfr@43dEjDd",
    "sfDatabase" -> "DIA_ML_ANALYSIS_DB_DEV",
    "sfSchema" -> "FDRDWH_TEMP",
    "sfRole" -> "DIA_ML_ANALYSIS_DEV_LOAD_ROLE",
    "sfWarehouse" -> "FDR_GLOBAL_WH"
    //  "continue_on_error" -> "on"
  )



}
