/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafkastreams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

/**
 *
 * @author root
 */
public class KafkaConsumerStream {

    public static final int GROUPSNUM = 10;
    public static final int HASHFUNCXGROUP =3;
    public static final int POWER = 32;
    public static final int MAX = 4999;
    public static final int HASHCONSTANT = 123;

    public static void main(String[] args) {

        Properties settings = new Properties();
// Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-first-streams-application");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "zookeeper:2181");

// Create an instance of StreamsConfig from the Properties instance
        StreamsConfig config = new StreamsConfig(settings);
        KStreamBuilder builder = new KStreamBuilder();

        Serde<String> stringSerde = Serdes.String();
        final long sum = 0;
        KStream<String, Long> myStream = builder.stream(stringSerde, Serdes.Long(), "LuisTopic");
        myStream.foreach(new ForeachAction<String, Long>() {

            public Random rand = new Random();

            public double maxR = 0;
            public int countElements = 0;

            @Override
            public void apply(String key, Long value) {
                Map<Integer, List<Integer>> hashValues = new HashMap<>();
                List<Double> averageXgroup;
                averageXgroup = new ArrayList<>();
                int a;
                int b;
                List<Integer> bVal;
                int countZerosXGroup = 0;
                countElements++;
                for (int i = 1; i <= GROUPSNUM; i++) {
                    countZerosXGroup = 0;

                    for (int j = 1; j <= HASHFUNCXGROUP; j++) {
                        //a = (3*(rand.nextInt(2500)+1));
                        do {
                            a = (int) (Integer.MAX_VALUE * Math.random());
                            //a = (int) (HASHCONSTANT * rand.nextInt(99));
                        } while (a % 2 == 0);

                        do {
                            //b=(3*(rand.nextInt(2500)+1));
                            b = (int) (Integer.MAX_VALUE * Math.random());
                            //b = (int) (HASHCONSTANT * rand.nextInt(99));
                        } while (b % 2 == 0 || contains(hashValues, a, b));

                        bVal = hashValues.get(a);
                        if (bVal == null) {
                            bVal = new ArrayList<>();
                            hashValues.put(a, bVal);
                        }
                        bVal.add(b);
                        //System.out.println("Processing: a: "+a+" b: "+b+" value: "+value);
                        countZerosXGroup = countZerosXGroup + countZeroes(Long.toBinaryString(applyHashFunction(a, b, value)));
                    }

                    double avrgGroup = (double) countZerosXGroup / HASHFUNCXGROUP;
                    //System.out.println("avrg: "+avrgGroup);
                    averageXgroup.add(avrgGroup);
                }
                
                //System.out.println("AvrgGroupBeforeMEdian: "+averageXgroup);
                double R = findMedian(averageXgroup);
                //System.out.println("R: "+R);
                if (R > maxR) {
                    maxR = R;
                }

                if (countElements == MAX) {
                    countElements = 0;
                    writeAnswerIntoTopic(maxR);
                    System.out.println("answer: " + Math.pow(2, maxR));
                    //printHasMap(hashValues);
                }
            }

            private boolean contains(Map<Integer, List<Integer>> hashValues, int a, int b) {
                List<Integer> list = hashValues.get(a);
                return (list != null && list.contains(b));
            }

            private long applyHashFunction(int a, int b, Long value) {

                return (long) ((a * value + b) % Math.pow(2, POWER));

            }

            private int countZeroes(String binString) {
                int stringIndex = binString.length();

                int sum = 0;
                //System.out.println("Processing: "+binString+" stringINdex: "+stringIndex);
                if (stringIndex > 1) {

                    while (binString.charAt(stringIndex - 1) == '0') {
                        sum++;
                        stringIndex--;
                    }
                }
                return sum;
            }

            private double findMedian(List<Double> averageXgroup) {
                Collections.sort(averageXgroup);
                //if x mod 2 = 0 is an even number
                int size = averageXgroup.size();

                if (size % 2 == 0) {

                    return ((double) (averageXgroup.get(size / 2) + averageXgroup.get((size / 2) + 1)) / 2);
                } else {
                    return ((double) averageXgroup.get((size + 1) / 2));
                }

            }

            private void printHasMap(Map<Integer, List<Integer>> hashValues) {
                hashValues.keySet().forEach((key) -> {
                    System.out.println(key + " : " + hashValues.get(key).toString());
                });
            }

            private void writeAnswerIntoTopic(double maxR) {
                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                Producer<String, String> producer;
                producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
                
                double answer = Math.pow(2, maxR);
                String topicResponse = Double.toString(answer)+" Groups: "+GROUPSNUM+" GroupSize:"+HASHFUNCXGROUP;
                ProducerRecord<String, String> record = new ProducerRecord<>("FlajoletMartinResult","key",topicResponse);
                producer.send(record);	       
                producer.close();
            }
        });

        //myStream.mapValues(p -> s);
        //System.out.println("Stream value"+s);
        KafkaStreams stream = new KafkaStreams(builder, config);
        //stream.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
        //    throw new UnsupportedOperationException("Error in stream"+e.getMessage()); //To change body of generated methods, choose Tools | Templates.
        //   });
        System.out.println("Streaming started !");
        stream.start();

    }

}
