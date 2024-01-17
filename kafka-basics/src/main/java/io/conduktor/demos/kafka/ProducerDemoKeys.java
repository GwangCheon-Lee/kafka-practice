package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

		private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

		public static void main(String[] args) {
				log.info("I am a Kafka Producer!");

				//	create Producer Properties
				Properties properties = new Properties();

				//	set Producer Properties
				properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
				properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

				properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");
				properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

				// create the Producer
				KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

				for (int j = 0; j < 10; j++) {
						for (int i = 0; i < 10; i++) {

								String topic = "demo_java";
								String key = "id_" + i;
								String value = "hello world" + i;
								// create Producer Record
								ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

								// send data
								producer.send(producerRecord, (metadata, e) -> {
										// executes every time a record successfully sent or an exception is thrown
										if (e == null) {
												//	the record was successfully sent
												log.info("Key: " + key + " | Partition: " + metadata.partition());

										} else {
												log.error("Error while producing", e);
										}
								});
						}
						try {
								Thread.sleep(500);
						} catch (InterruptedException e) {
								throw new RuntimeException(e);
						}
				}


				// tell the producer to send all data and block util done -- synchronous
				producer.flush();

				// flush and close the producer
				producer.close();
		}

}
