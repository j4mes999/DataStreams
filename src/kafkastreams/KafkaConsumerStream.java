/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafkastreams;


import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import model.Group;
import model.HashFunction;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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

    public static final int GROUPSNUM = 20;
    public static final int HASHFUNCXGROUP = 40;

    public static final int MAX = 2500000;
    public static final int HASHCONSTANT = 123;

    public static void main(String[] args) {

        // First of all generate the HashFunctions
        List<Group> hashFunctions = generateFunctions();
        printAll(hashFunctions);
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "FlajoletMartin");
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
            public Calendar startTime;
            @Override
            public void apply(String key, Long value) {
                
                List<Double> averageXgroup;
                int countZeroes;
                countElements++;
                if(countElements == 1){
                    startTime = Calendar.getInstance();
                }
                    
                for (Group g : hashFunctions) {
                    for (HashFunction hf : g.getFunctions()) {
                        countZeroes = countZeroes(Long.toBinaryString(hf.getHashValue(value)));
                        //System.out.println("countZero: "+countZeroes+" ele: "+Long.toBinaryString(hf.getHashValue(value)));
                        if(countZeroes > hf.getMaxR()){
                            hf.setMaxR(countZeroes);
                            //System.out.println("New max:"+countZeroes);
                        }
                            
                    }
                }

                if (countElements == MAX) {
                    countElements = 0;
                    
                    long executionTime = Calendar.getInstance().getTimeInMillis() - startTime.getTimeInMillis();
                    averageXgroup = calculateAvrg(hashFunctions);
                    double R = findMedian(averageXgroup);
                    writeAnswerIntoTopic(R,executionTime);
                    System.out.println("answer: " + R);
                    //printHasMap(hashValues);
                }
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
                printGroup(averageXgroup);
                //if x mod 2 = 0 is an even number
                int size = averageXgroup.size();

                if (size % 2 == 0) {

                    return ((double) (averageXgroup.get(size / 2) + averageXgroup.get((size / 2) + 1)) / 2);
                } else {
                    return ((double) averageXgroup.get((size + 1) / 2));
                }

            }

        

            private void writeAnswerIntoTopic(double maxR, long executionTime) {
                Properties props = new Properties();
                props.put("bootstrap.servers", "localhost:9092");
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                Producer<String, String> producer;
                producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

                long totalTime = executionTime/1000;
                String topicResponse = Double.toString(maxR) + " Groups: " + GROUPSNUM + " GroupSize:" + HASHFUNCXGROUP+" Pow: "+HashFunction.POWER
                                        +" Time: "+totalTime;
                ProducerRecord<String, String> record = new ProducerRecord<>("FlajoletMartinResult", "key", topicResponse);
                producer.send(record);
                producer.close();
            }

            private List<Double> calculateAvrg(List<Group> hashFunctions) {
               List<Double> list = new ArrayList<>();
               double avrg;
               for(Group g: hashFunctions){
                   avrg = 0;
                   for(HashFunction hf : g.getFunctions()){
                       avrg += Math.pow(2,hf.getMaxR() );
                       System.out.println("Max R: "+hf.getMaxR());
                   }
                   System.out.println("Avrg group "+avrg/HASHFUNCXGROUP);
                   list.add(avrg/HASHFUNCXGROUP);
               }
               return list;
            }

            private void printGroup(List<Double> averageXgroup) {
                 System.out.println("Group");
                for(double d: averageXgroup){
                    System.out.print(d+" : ");
                }
            }
        });

        
        KafkaStreams stream = new KafkaStreams(builder, config);
        System.out.println("Streaming started !");
        stream.start();

    }

    private static List<Group> generateFunctions() {

        List<Group> groups = new ArrayList<>();
        Group g;
        HashFunction hf;
        int a, b;
        Map<Integer, List<Integer>> hashValues = new HashMap<>();
        List<Integer> bVal;
        Random rnd = new Random();
        for (int i = 0; i < GROUPSNUM; i++) {
            g = new Group();
            for (int j = 0; j < HASHFUNCXGROUP; j++) {
                do {
                    a = (int) (Integer.MAX_VALUE * Math.random());
                    //a = (int) (11 * rnd.nextInt(127));
                } while(false);//(false); //(a % 2 == 0);

                do {
                    b = (int)(Integer.MAX_VALUE * Math.random());
                    //b = (int) (23 * rnd.nextInt(127));
                } while (contains(hashValues, a, b));//(false) (a % 2 == 0 || contains(hashValues, a, b));
                bVal = hashValues.get(a);
                if (bVal == null) {
                    bVal = new ArrayList<>();
                    hashValues.put(a, bVal);
                }
                bVal.add(b);
                
                hf = new HashFunction(a, b);
                g.addFunction(hf);
            }
            groups.add(g);
        }
        return groups;
    }

    private static boolean contains(Map<Integer, List<Integer>> hashValues, int a, int b) {
        List<Integer> list = hashValues.get(a);
        return (list != null && list.contains(b));
    }

    private static void printAll(List<Group> hashFunctions) {
        
        Random rnd = new Random();
        hashFunctions.forEach((g) -> {
            System.out.println("Group "+g.getSize());
            g.getFunctions().forEach((hf) -> {
                System.out.println(hf.getA()+" - "+hf.getB()+" - "+hf.getMaxR());
            });
        });
    }

}
