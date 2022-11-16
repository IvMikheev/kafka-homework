package com.epam.homework.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutputConsumer {

    @KafkaListener(topics = "${kafka-topics.output.name}", groupId = "output")
    public void consume(ConsumerRecord<String, Double> consumerRecord) {
        log.info("Consumer consumed vehicle with id: '{}' and traveled distance: {}",
                consumerRecord.key(), consumerRecord.value()
        );
    }
}
