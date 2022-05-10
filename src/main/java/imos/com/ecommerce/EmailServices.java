package imos.com.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class EmailServices {
    public static void main(String[] args) {

        var emailService = new EmailServices();
        var service = new KafkaService("ECOMMERCE_EMAIL",
                emailService::parse);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> registro) {
        System.out.println("Processando novos emails:");
        System.out.println(registro.key());
        System.out.println(registro.value());
        System.out.println(registro.partition());
        System.out.println(registro.offset());
        try {
            Thread.sleep(100,00);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email Enviado com Sucesso!");

    }
}



