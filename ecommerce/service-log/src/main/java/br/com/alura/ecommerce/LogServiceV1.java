package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogServiceV1 {

    public static void main(String[] args) {

        var logServiceV1 = new LogServiceV1();
        var service = new KafkaService(
                LogServiceV1.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logServiceV1::parse,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("---------------------------------------------");
        System.out.println("Logging!");
        System.out.println("Topic: " + record.topic());
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }

}
