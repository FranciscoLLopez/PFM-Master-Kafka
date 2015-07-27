package kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class TestProducer {
	public static void main(String[] args) {
		long counter = 0;
		File file = new File("/home/cloudera/workspace-scala/PFM-Master-Kafka/ds/SUMMIT_10k.csv");
		BufferedReader reader = null;

		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "kafka.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);
		while (counter < 1000000000) {
			try {
				reader = new BufferedReader(new FileReader(file));
				String text = null;

				while ((text = reader.readLine()) != null) {

					KeyedMessage<String, String> data = new KeyedMessage<String, String>(
							"test", Double.toString(counter), text);
					counter++;
					if (counter%500 ==0 ){	
						pause(5000);
						System.out.println("envio " + counter);
						
					}
					producer.send(data);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (reader != null) {
						reader.close();
					}
				} catch (IOException e) {
				}
			}
		}
		producer.close();
	}
	
	
	private static void pause(long timeMilliseconds){
		try {
		    Thread.sleep(timeMilliseconds);              //1000 milliseconds is one second.
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		
	}
}