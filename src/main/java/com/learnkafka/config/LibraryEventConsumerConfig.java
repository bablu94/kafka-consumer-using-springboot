package com.learnkafka.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@EnableKafka
public class LibraryEventConsumerConfig{

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?,?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configure,
            ConsumerFactory<Object, Object> kafkaConsumerFactory){
         ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
         configure.configure(factory, kafkaConsumerFactory);
         //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
         return factory;
    }
}
