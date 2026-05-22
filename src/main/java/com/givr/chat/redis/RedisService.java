package com.givr.chat.redis;

import com.givr.chat.Dto.Message;
import com.givr.chat.Dto.MessageEvent;

import com.givr.chat.handlers.SessionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class RedisService {
    @Autowired
    private ReactiveRedisTemplate<String, Object> redisTemplate;
    @Autowired
    private ObjectMapper mapper;

    private static final Map<Long, RecordId> projectMsgPointer = new ConcurrentHashMap<>();

    public static String getMessageOffset(Long projectId){
        var record = projectMsgPointer.computeIfAbsent(projectId, k->null);
        if(record == null)
            return null;
        return record.getValue();
    }
    // Websocket checks before accepting connection
    public Mono<Boolean> isMember(String userId, Long projectId){
        return redisTemplate.opsForSet()
                .isMember(String.format("user:%s", userId), String.valueOf(projectId));
    }

    public Flux<Long> getUserProjects(String userId){
        return redisTemplate.opsForSet()
                .members("user:"+userId)
                .map(String::valueOf)
                .map(Long::parseLong);
    }

    public void writeUserProjectPointers(SessionContext sessionContext, Map<String, String> projectPointers){
        Map<String, String> payload = new ConcurrentHashMap<>();
        payload.put("userId", sessionContext.getUserId());
        payload.put("role", sessionContext.getRole().name());
        payload.putAll(projectPointers);

        redisTemplate.opsForStream()
                .add(StreamRecords.newRecord()
                        .ofMap(payload)
                        .withStreamKey("user_project_pointers"))
                .then();
    }


    public Mono<Void> publishMessage(SessionContext ctx, MessageEvent payload){
        Message chatPayload = payload.getPayload();
        chatPayload.setSentBy(ctx.getUserId());
        chatPayload.setUsername(ctx.getEmail());
        chatPayload.setRole(ctx.getRole());

        Map<String, String> stringMap = Map.of(
                "projectId", chatPayload.getProjectId().toString(),
                "content", chatPayload.getContent(),
                "sentBy", chatPayload.getSentBy(),
                "username", chatPayload.getUsername(),
                "role", chatPayload.getRole().toString(),
                "sentAt", chatPayload.getSentAt().toString()
        );

        Mono<RecordId> recordId = redisTemplate.opsForStream().add(
                StreamRecords
                    .newRecord()
                        .ofMap(stringMap)
                    .withStreamKey("stream:messages")
        ).doOnNext(r-> projectMsgPointer.put(chatPayload.getProjectId(), r)
        ).doOnNext(r->chatPayload.setMsgId(r.getValue()));

        return recordId.then();
    };


}
