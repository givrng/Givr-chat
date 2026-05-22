package com.givr.chat.config;

import com.givr.chat.Dto.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import tools.jackson.databind.ObjectMapper;

@Configuration
public class RedisConfig {
    @Value("${spring.data.redis.host}")
    private String host;
    @Value("${spring.data.redis.port}")
    private int port;
    @Value("${spring.data.redis.password}")
    private String password;


    @Bean
    @Primary
    public ReactiveRedisConnectionFactory reactiveRedisConnectionFactory(){
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
        configuration.setHostName(host);
        configuration.setPort(port);
        if(password!=null)
            configuration.setPassword(password);
        return new LettuceConnectionFactory(configuration);
    }
    @Bean
    public ObjectMapper objectMapper(){
        return new ObjectMapper();
    }

    @Bean
    public ReactiveRedisTemplate<String, Object> reactiveRedisTemplate(ReactiveRedisConnectionFactory factory, ObjectMapper mapper){
        JacksonJsonRedisSerializer<Object> jsonRedisSerializer = new JacksonJsonRedisSerializer<>(mapper, Object.class);

        RedisSerializationContext<String, Object> context = RedisSerializationContext.<String, Object> newSerializationContext()
                .key(RedisSerializer.string())
                .value(jsonRedisSerializer)
                .hashKey(RedisSerializer.string())
                .hashValue(jsonRedisSerializer)
                .build();
        return new ReactiveRedisTemplate<>(factory, context);
    }
    @Bean
    ReactiveStringRedisTemplate reactiveStringRedisTemplate(ReactiveRedisConnectionFactory factory) {
        return new ReactiveStringRedisTemplate(factory);
    }
}
