package projekatDjordjevic.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {

	public static void main(String[] args) throws IOException {
		String bootstrapServers="127.0.0.1:9092";
		Properties properties=new Properties();
		properties.setProperty("bootstrap.servers", bootstrapServers);
		properties.setProperty("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
		
	    String topic = "networkConnection";
        String fileName = "resource/KDDTrain.csv";
        KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);
        File f = new File(fileName);
        FileReader fr = new FileReader(f);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();
        while (line != null) {

       
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, line);

            producer.send(rec);
            producer.flush();
            System.out.println("Sent message: " + line);
            line = reader.readLine();

        } 
        producer.close();
        System.out.println("All done.");

        System.exit(1);
        

	
	}

}
