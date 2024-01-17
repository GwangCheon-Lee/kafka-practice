package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

		private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

		public static void main(String[] args) {
				log.info("I am a Kafka Consumer!");

				String groupId = "my-java-application";
				String topic = "demo_java";

				//	create Consumer Properties
				Properties properties = new Properties();

				//	set Consumer Properties
				properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

				// create Consumer configs
				properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
				properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
				properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
				properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

				// create a consumer
				try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
						consumer.subscribe(List.of(topic));

						// subscribe to a topic
						while (true) {
								log.info("Polling");

								ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

								for (ConsumerRecord<String, String> record : records) {
										log.info("Key: " + record.key() + ", Value: " + record.value());
										log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
								}
						}


						// poll for data
				} catch (Exception e) {
						log.error("Consumer Error", e);
				}


		}

}
