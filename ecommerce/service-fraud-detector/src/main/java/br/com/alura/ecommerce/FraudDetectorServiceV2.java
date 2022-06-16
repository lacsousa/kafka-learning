package br.com.alura.ecommerce;

import com.sun.jdi.ObjectReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;

public class FraudDetectorServiceV2 {

    public static void main(String[] args) {

        var fraudDetectorServiceV2 = new FraudDetectorServiceV2();
        try(var kafkaService = new KafkaService<>(
                FraudDetectorServiceV2.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorServiceV2::parse,
                Order.class,
                Map.of())) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {

        System.out.println("---------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            //ignoring
            e.printStackTrace();
        }
        System.out.println("Order processed !");
    }

}
