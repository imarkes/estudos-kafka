package imos.com.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class novoPedido {
    //Cria topico e produz mensagem.

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(configs());
        var key = UUID.randomUUID().toString();
        var value = key + "nome, Joa"; //Id, chave, valor (mensagens)
        // Envia as mensagens para o topico
        var topico = new ProducerRecord<String, String>("ECOMMERCE_PEDIDOS", key, value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println("Enviado com Sucesso " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timesTamp " + data.timestamp());
        };
        var email = "Email-06 ";
        var emailRecord = new ProducerRecord<>("ECOMMERCE_EMAIL", key, email);
        producer.send(topico, callback).get();
        producer.send(emailRecord, callback).get();

    }

    private static Properties configs() {
        // Faz a conexão e serializacão dos dados.
        var prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return prop;
    }
}
