package org.severstal.data.rmq;

import org.severstal.data.config.RabbitMQConfig;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableRabbit
@Configuration
public class RabbitMQConfiguration {
    @Autowired
    private RabbitMQConfig cfg;

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory =
                new CachingConnectionFactory(this.cfg.getHostname());
        connectionFactory.setUsername(this.cfg.getUsername());
        connectionFactory.setPassword(this.cfg.getPassword());
        return connectionFactory;
    }

    @Bean
    public AmqpAdmin amqpAdmin() {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory());

        return rabbitAdmin;
    }

    @Bean
    public RabbitTemplate rabbitTemplate() {
        return new RabbitTemplate(connectionFactory());
    }

    @Bean
    public Queue dataQueue() {
        return new Queue("data-queue");
    }

    @Bean
    public FanoutExchange fanoutExchange() {
        return new FanoutExchange("data-exchange");
    }

    @Bean
    public Binding binding() {
        return BindingBuilder.bind(dataQueue()).to(fanoutExchange());
    }
}