package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

		private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

		public static void main(String[] args) {
				log.info("I am a Kafka Producer!");

				//	create Producer Properties
				Properties properties = new Properties();

				//	set Producer Properties
				properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
				properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

				// create the Producer
				KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

				// create Producer Record
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

				// send data
				producer.send(producerRecord);

				// tell the producer to send all data and block util done -- synchronous
				producer.flush();

				// flush and close the producer
				producer.close();
		}

}
