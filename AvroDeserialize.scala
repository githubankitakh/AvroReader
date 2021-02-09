package avro_consumer

  import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
  import io.confluent.kafka.serializers.KafkaAvroDeserializer
  import org.apache.avro.generic.GenericRecord

  class AvroDeserialize(client: CachedSchemaRegistryClient) extends KafkaAvroDeserializer{



    this.schemaRegistry = client   // OBJECT OF CURRENT CLASS TRYING TO INVOKE THE MEMBER FUNCTION AND PASSING CLIENT ARUEMENT TO IT //

    def deserializeAvroData(bytes: Array[Byte]): String = {
      val genericRecord = deserialize(bytes).asInstanceOf[GenericRecord]
      genericRecord.toString
    }


}
