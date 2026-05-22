package com.givr.chat.config;

import com.givr.chat.handlers.GivrSocketHandler;
import com.givr.chat.handlers.JwtHandshakeService;
import com.givr.chat.jwt.JwtUtil;
import com.givr.chat.redis.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.WebSocketService;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.reactive.socket.server.upgrade.ReactorNettyRequestUpgradeStrategy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class WebSocketConfig implements WebFluxConfigurer {
    @Autowired
    private JwtUtil jwtUtil;
    @Value("${allowed.origins}")
    private List<String> allowedOrigins;

    @Autowired
    private RedisService redisService;
    public WebSocketHandlerAdapter handlerAdapter(){
        return new WebSocketHandlerAdapter();
    }

//    @Bean
//    public HandshakeWebSocketService handshakeWebSocketService(){
//        return new JwtHandshakeService(jwtUtil, redisService);
//    }
    @Bean
    public HandlerMapping handlerMapping(GivrSocketHandler handler){
        Map<String, WebSocketHandler> map = new HashMap<>();
        map.put("/chat", handler);
        int order = -1;
        return new SimpleUrlHandlerMapping(map, order);
    }
    @Bean
    public WebSocketHandlerAdapter webSocketHandlerAdapter(JwtHandshakeService handshakeService){
        return new WebSocketHandlerAdapter(handshakeService);
    }

}
