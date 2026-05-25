package com.givr.chat.handlers;

import com.givr.chat.Dto.Message;
import com.givr.chat.Dto.MessageEvent;
import com.givr.chat.enums.AccountType;
import com.givr.chat.enums.PacketType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
@Getter
@EqualsAndHashCode()
public class SessionContext {
    private final WebSocketSession session;
    private final String userId;
    private final String email;
    private final AccountType role;
    @Getter
    // projectId <------> pointer
    private final Map<String, String> group_pointers = new ConcurrentHashMap<>();
    @Getter
    private final Map<Long, Integer> groupUnreadCount = new ConcurrentHashMap<>();
    private final Set<Long> joinedProjects = ConcurrentHashMap.newKeySet();

    private final Sinks.Many<Message> outbound = Sinks.many().unicast().onBackpressureBuffer();

    private static final ObjectMapper mapper = new ObjectMapper();
    public Mono<Void> send(String payload){
        return session.send(Mono.just(session.textMessage(payload)));
    }

    public Mono<Void> sendUnreadCount(Long projectId){
        Message payload = new Message(projectId, PacketType.unread_update, String.valueOf(getUnreadCount(projectId)));
        return session.send(Mono.just(session.textMessage(mapper.writeValueAsString(payload))));
    }

    public SessionContext(WebSocketSession session, GivrUserAuth userAuth){
        this.session = session;
        this.userId = userAuth.userId();
        this.email = userAuth.email();
        this.role = userAuth.accountType();
    }

    public void increaseUnreadCount(Long projectId){
        int count = groupUnreadCount.computeIfAbsent(projectId, k->0) + 1;
        groupUnreadCount.put(projectId, count);
    }
    public int getUnreadCount(Long projectId){
        return groupUnreadCount.get(projectId);
    }

    public void resetUnreadCount(Long projectId){
        groupUnreadCount.put(projectId, 0);
    }

    public void addGroupMsgPointer(Long projectId, String pointer){
        if(pointer==null)
            return;

        group_pointers.putIfAbsent(String.valueOf(projectId), pointer);
    }

    public String getGroupMsgPointer(Long projectId){
        return group_pointers.get(String.valueOf(projectId));
    }


}
