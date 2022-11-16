package com.epam.homework;

import com.epam.homework.config.IntegrationTestConfig;
import com.epam.homework.service.KafkaConsumer;
import com.epam.homework.service.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.concurrent.ListenableFuture;

@SpringBootTest
@ActiveProfiles("test")
@ContextConfiguration(classes = {IntegrationTestConfig.class, KafkaProducer.class, KafkaConsumer.class})
class KafkaIntegrationTest {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Value("${kafka.topic.name}")
    private String topic;

    @Test
    void send_Data_To_Topic_Test() throws Exception {
        String data = "Sending data to Kafka";

        ListenableFuture<SendResult<String, String>> response = kafkaProducer.send(topic, data);
        ProducerRecord<String, String> producerRecord = response.get().getProducerRecord();

        Assertions.assertEquals(producerRecord.topic(), topic);
        Assertions.assertEquals(producerRecord.value(), data);
    }
}
