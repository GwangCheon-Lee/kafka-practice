package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
		private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

		public static void main(String[] args) throws InterruptedException {
				log.info("I am a Kafka Producer!");

				String bootstrapServers = "localhost:9092";

				//	create Producer Properties
				Properties properties = new Properties();
				properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
				properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

				// create the Producer
				KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

				String topic = "wikimedia.recentchange";

				WikimediaChangeHandler eventHandler = new WikimediaChangeHandler(producer, topic);
				String url = "https://stream.wikimedia.org/v2/stream/recentchange";
				EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
				EventSource eventSource = builder.build();

				// start the producer in another thread
				eventSource.start();

				// we produce for 10 minutes and black the program until then
				TimeUnit.MINUTES.sleep(10);
		}

}
