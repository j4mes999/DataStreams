/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafkastreams;


import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 *
 * @author root
 */
public class KafkaProducer {
    
    public static void main(String[] args){
        
        String topicName = "LuisTopic";
        String value = "Hello World from java prodcuer to broker kafka 3";
        String key = "key3";
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
          Producer<String, String> producer;
        producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
	
	  ProducerRecord<String, String> record = new ProducerRecord<>(topicName,key,value);
	  producer.send(record);	       
      producer.close();
	  
	  System.out.println("SimpleProducer Completed.");
    } 
    
}

