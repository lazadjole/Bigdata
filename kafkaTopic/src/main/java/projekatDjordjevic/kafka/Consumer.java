package projekatDjordjevic.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class Consumer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String bootstrapServers="127.0.0.1:9092";
		Properties properties=new Properties();
		properties.setProperty("bootstrap.servers", bootstrapServers);
		properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
		properties.setProperty("auto.offset.reset", "earliest");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
		KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);
	    String topic = "networkConnection";
		consumer.subscribe(Collections.singleton(topic));
		
		
        long pollTimeOut = 1000;
        long waitTime = 30 * 1000;  
        long numberOfMsgsReceived = 0;
	    while (waitTime > 0) {

	    	ConsumerRecords<String, String> msg = consumer.poll(Duration.ofMillis(pollTimeOut));
            if (msg.count() == 0) {
                  System.out.println("No messages after 1 second wait.");
            } else {
                 System.out.println("Read " + msg.count() + " messages");
                numberOfMsgsReceived += msg.count();

                
                Iterator<ConsumerRecord<String, String>> iter = msg.iterator();
                while (iter.hasNext()) {
                    ConsumerRecord<String, String> record = iter.next();
                    System.out.println("Consuming " + record.toString());
                    System.out.println(record.value());

                }
            }
            waitTime = waitTime - 1000; 
        }
        consumer.close();
		
	}

}
