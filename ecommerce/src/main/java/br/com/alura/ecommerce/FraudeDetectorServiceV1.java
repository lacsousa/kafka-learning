package br.com.alura.ecommerce;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class FraudeDetectorServiceV1 {

  public static void main(String[] args) {

    var consumer = new KafkaConsumer<String, String>(properties());
    consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
    var records = consumer.poll(Duration.ofMillis(100));
    if (records.isEmpty()) {
      System.out.println("Não foram encontrados registros...");
      return;
    }

    for (var record : records) {
      System.out.println("---------------------------------------------------------");
      System.out.println("Processando nova ordem, verificando possíveis fraudes ...");
      System.out.println(record.key());
      System.out.println(record.value());
      System.out.println(record.partition());
      System.out.println(record.offset());
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println("Ordem processada ...");
    }

  }

  private static Properties properties() {
    var properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    //precisa passar um GroupId
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudeDetectorServiceV1.class.getSimpleName());
    return properties;
  }
}
