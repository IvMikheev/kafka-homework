package com.epam.homework.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.concurrent.ListenableFuture;

@Slf4j
@Component
@ActiveProfiles("test")
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public ListenableFuture<SendResult<String, String>> send(String topic, String data) {
        log.info("Sending data: '{}' to topic: '{}'", data, topic);
        return kafkaTemplate.send(topic, data);
    }
}
