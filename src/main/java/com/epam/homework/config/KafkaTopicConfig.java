package com.epam.homework.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka-topics.input.name}")
    private String inputTopic;

    @Value("${kafka-topics.output.name}")
    private String outputTopic;

    @Value("${kafka-topics.input.partitions}")
    private Integer numPartitions;

    @Value("${kafka-topics.input.replication-factor}")
    private Short replicationFactor;

    @Bean
    public KafkaAdmin kafkaAdmin(KafkaProperties kafkaProperties) {
        return new KafkaAdmin(kafkaProperties.buildAdminProperties());
    }

    @Bean
    public NewTopic input() {
        return new NewTopic(inputTopic, numPartitions, replicationFactor);
    }

    @Bean
    public NewTopic output() {
        return new NewTopic(outputTopic, numPartitions, replicationFactor);
    }
}
