package imos.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(configs());
        // consome mensagens de qualquer topico que começa com ECOMMERCE
        consumer.subscribe(Pattern.compile("ECOMMERCE*"));
        while (true) {
            // verifica a cada x segundos se existe novas mensagens
            var registros = consumer.poll(Duration.ofMillis(100));
            if (!registros.isEmpty()) {
                System.out.println("Total de: " + registros.count() + " encontrados");
                for (var registro : registros) {
                    System.out.println("LOG: " + registro.topic());
                    System.out.println(registro.key());
                    System.out.println(registro.value());
                    System.out.println(registro.partition());
                    System.out.println(registro.offset());
                }
            }

        }
    }

    private static Properties configs() {
        var prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
        return prop;
    }
}
