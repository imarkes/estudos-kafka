package imos.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaService {
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;

    KafkaService(String topic, ConsumerFunction parse) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(configs());
        // consome mensagens do topico especificado
        consumer.subscribe(Collections.singletonList(topic));
    }

    void run() {
        while (true) {
            // verifica a cada x segundos se existe novas mensagens
            var registros = consumer.poll(Duration.ofMillis(100));
            if (!registros.isEmpty()) {
                System.out.println("Total de: " + registros.count() + " encontrados");
                for (var registro : registros) {
                    parse.consume(registro);
                }
            }

        }
    }

    private static Properties configs() {
        var prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailServices.class.getSimpleName());
        return prop;
    }
}
