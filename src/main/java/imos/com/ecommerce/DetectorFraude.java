package imos.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DetectorFraude {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(configs());
        // consome mensagens do topico especificado
        consumer.subscribe(Collections.singletonList("ECOMMERCE_PEDIDOS"));
        while (true) {
            // verifica a cada x segundos se existe novas mensagens
            var registros = consumer.poll(Duration.ofMillis(100));
            if (!registros.isEmpty()) {
                System.out.println("Total de: " + registros.count() + " encontrados");
                for (var registro : registros) {
                    System.out.println("Processando novos registros:");
                    System.out.println(registro.key());
                    System.out.println(registro.value());
                    System.out.println(registro.partition());
                    System.out.println(registro.offset());
                    try {
                        Thread.sleep(100,0);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Registros processados!");
                }
            }

        }
    }

    private static Properties configs() {
        var prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, DetectorFraude.class.getSimpleName());
        prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // Evita duplicação
        return prop;
    }
}
