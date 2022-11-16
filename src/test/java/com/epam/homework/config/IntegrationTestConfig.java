package com.epam.homework.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
@Profile("test")
public class IntegrationTestConfig {

    private static final String SEPARATOR = "://";

    @Value("${kafka.image}")
    private String kafkaImage;

    @Value("${kafka.topic.name}")
    private String topic;

    @Value("${kafka.topic.partitions}")
    private Integer numPartitions;

    @Value("${kafka.topic.replication-factor}")
    private Short replicationFactor;

    @Bean
    public Network network() {
        return Network.newNetwork();
    }

    @Bean
    public KafkaContainer kafkaContainer() {
        KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse(kafkaImage))
                .withNetwork(network())
                .withEmbeddedZookeeper();

        kafkaContainer.start();

        return kafkaContainer;
    }

    @Bean
    public NewTopic topicExample() {
        return TopicBuilder.name(topic)
                .partitions(numPartitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        String host = StringUtils.substringAfter(kafkaContainer().getBootstrapServers(), SEPARATOR);

        Map<String, Object> properties = new HashMap<>();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, host);

        return new KafkaAdmin(properties);
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        String host = StringUtils.substringAfter(kafkaContainer().getBootstrapServers(), SEPARATOR);

        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        String host = StringUtils.substringAfter(kafkaContainer().getBootstrapServers(), SEPARATOR);

        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
