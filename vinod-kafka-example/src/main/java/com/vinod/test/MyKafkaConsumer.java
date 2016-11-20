

package com.vinod.test;


import java.util.Properties;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Test class to send and receive message using kafka broker.
 * 
 * @author vinodkariyathungalkumaran
 *
 */
public class MyKafkaConsumer {
    private kafka.javaapi.consumer.ConsumerConnector consumer;

    /**
     * Method to create the kafka producer and sending message
     * 
     */
    public void testProducer() {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
        KeyedMessage<String, String> message = new KeyedMessage<String, String>("order", "Sending Customer Order, please process");
        producer.send(message);
    }

    /**
     * Method to create consumer and consume message
     */
    public void testConsumer() {
        String topic = "order";
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "vinod");
        props.put("zookeeper.session.timeout.ms", "5000");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        props.put("serializerClass", "kafka.serializer.StringEncoder");
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
        for (final KafkaStream stream : streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                String message = new String(it.next().message());
                System.out.println("Message from order Topic: " + message);
            }
        }
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    public static void main(String[] args) {
        MyKafkaConsumer mykafkaConsumer = new MyKafkaConsumer();
        mykafkaConsumer.testProducer();
        mykafkaConsumer.testConsumer();
    }

}


