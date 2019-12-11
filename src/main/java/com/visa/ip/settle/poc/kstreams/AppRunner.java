package com.visa.ip.settle.poc.kstreams;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class AppRunner {

    Logger logger = LoggerFactory.getLogger(AppRunner.class);
    Random random = new Random();
    List<String> keys = new ArrayList<String>();
    KafkaProducer<String,String> producer;
    Map<String,Object> producerProperties;
    Properties kStreamsProperties;
    final public static long WINDOW_SIZE_DAYS = 1;
    final public static long ADVANCE_HOURS = 1;


    public void setup(){
        for(int i = 0; i< 100; i++){
            keys.add(RandomStringUtils.randomAlphanumeric(10));
        }

        producerProperties = new HashMap<String, Object>();
        // TODO : set producerProperties
        producerProperties.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        producerProperties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerProperties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producer = new KafkaProducer<>(producerProperties);

        producer = new KafkaProducer<String, String>(producerProperties);


        kStreamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreamsdedupdemo");
        kStreamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kStreamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kStreamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    public void run() throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(3);

        // random publisher to mock a change stream from MongoDB
        List<Callable<String>> publishers = new ArrayList<Callable<String>>();
        for(int i=0; i< 3; i++){
            publishers.add(() -> {
                int i1 = 0;
                while(i1 < 1000000) {
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("KStreamsTopic", keys.get(random.nextInt(keys.size() - 1)), "value");
                    producer.send(producerRecord);
                    try {
                        Thread.sleep(1000L + new Long(random.nextInt(9000)));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    i1++;
                }
                return "Finished publishing 1000000 records";
            });
        }
        executorService.invokeAll(publishers);


        executorService.shutdown();
        while (!executorService.isTerminated()) {
        }
        System.out.println("Finished all threads writing");

        System.out.println("Dedup wroker starting....");


        // kafka stream worker to consume and dedup
        final StreamsBuilder dedupTopologyBuilder = new StreamsBuilder();
        KStream<String, String> source = dedupTopologyBuilder.stream("KStreamsTopic");
        source
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(WINDOW_SIZE_DAYS)).advanceBy(Duration.ofHours(ADVANCE_HOURS))) // we are windowing the operations
                .reduce((value1,value2) -> value1.length() >= value2.length() ? value1 : value2)    // the comparison can be timestamped here instead
                .toStream()
                .to("KStreamsTopic_deduped");  // output to deduped stream

        final Topology dedupTopology = dedupTopologyBuilder.build();
        System.out.println(dedupTopology.describe());
        System.out.println("Now starting, ctrl+c kill me after a while");





        // start the dedup topology
        final KafkaStreams streams = new KafkaStreams(dedupTopology, kStreamsProperties);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);


    }

}
