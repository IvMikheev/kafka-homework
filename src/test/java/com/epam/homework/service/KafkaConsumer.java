package com.epam.homework.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@Getter
@Component
@ActiveProfiles("test")
public class KafkaConsumer {

    @KafkaListener(topics = "${kafka.topic.name}")
    public void receive(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
        // at most once
        acknowledgment.acknowledge();
        log.info("Received data: '{}' in topic: '{}'", consumerRecord.value(), consumerRecord.topic());
    }
}
