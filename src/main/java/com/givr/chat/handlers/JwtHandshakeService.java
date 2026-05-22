package com.givr.chat.handlers;

import com.givr.chat.jwt.JwtUtil;
import com.givr.chat.redis.RedisService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.web.server.Cookie;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Predicate;

@RequiredArgsConstructor
@Service
public class JwtHandshakeService extends HandshakeWebSocketService {
    public static final String SECURITY_CONTEXT_KEY = "AUTH_USER_SECURITY_CONTEXT";
    public static final String AUTH_USER_EMAIL = "AUTH_USER_EMAIL";
    private final JwtUtil jwtUtil;
    private final RedisService redisService;

    @PostConstruct
    public void init(){
        setSessionAttributePredicate(attr->true);
    }

    @Override
    public Mono<Void> handleRequest(ServerWebExchange exchange, WebSocketHandler handler) {
        HttpCookie cookie = exchange.getRequest().getCookies().getFirst("AccessToken");
        if(cookie != null){
            String accessToken = cookie.getValue();
            if(jwtUtil.isTokenExpired(accessToken)){
                exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
                return exchange.getResponse().setComplete();
            }

        }else{
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }
        return super.handleRequest(exchange, handler);
    }
}
