package com.givr.chat.handlers;

import com.givr.chat.Dto.MessageEvent;
import com.givr.chat.enums.PacketType;
import com.givr.chat.jwt.JwtUtil;
import com.givr.chat.redis.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpCookie;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@Service
public class GivrSocketHandler implements WebSocketHandler {
    @Autowired
    private RedisService redisService;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private JwtUtil util;
    // Users that are on connected to the app but have not opened a group
    private final Map<Long, Map<String, SessionContext>> inactiveUser = new ConcurrentHashMap<>();
    // users that have opened a group and interacting
    private final Map<Long, Map<String, SessionContext>> activeUsers = new ConcurrentHashMap<>();
    @Override
    public Mono<Void> handle(WebSocketSession session) {

        return session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .flatMap(payload-> processPayload(payload, session))
                .doFinally((signalType)-> cleanUpSocket(session))
                .then();
    }

    /**
     * Registers a session to a group and writes to MVC server to inform it to listen when a group
     * becomes active*/
    public void registerGroup(Long groupId, SessionContext session){
        Map<String, SessionContext> sessions = inactiveUser.computeIfAbsent(groupId, k->new ConcurrentHashMap<>());
        sessions.put(session.getSession().getId(), session);
        inactiveUser.put(groupId, sessions);

        Map<String, SessionContext> activeSessions = activeUsers.computeIfAbsent(groupId, k->new ConcurrentHashMap<>());

        if(activeSessions.isEmpty())
            activeUsers.put(groupId, activeSessions);
    }


    public SessionContext createContext(WebSocketSession session){
        HttpCookie cookie = session.getHandshakeInfo().getCookies().getFirst("AccessToken");

        if(cookie!=null){
            GivrUserAuth userAuth = util.authenticate(cookie.getValue());
            return new SessionContext(session, userAuth);
        }
        return null;
    }

    public Mono<Void> deny(WebSocketSession session, Long projectId){
        return session.send(
                Mono.just(session.textMessage("""
                       {
                       "type":"Error",
                       "code":"UNAUTHORIZED",
                       "projectId":%d
                       }
                        """.formatted(projectId)))
        );
    }

    public Mono<Void> processPayload(String payload, WebSocketSession session){
        return deserialize(payload).flatMap(msg-> switch (msg.getType()){
                case Connected -> handleConnected(session);
                case Group_Opened -> handleJoin(session, msg);
                case Message -> handleMessage(session, msg);
                case Group_Closed -> handleLeave(session, msg);
            }
        )
                .doOnError(Throwable::printStackTrace)
                .onErrorResume(e-> {
                    e.printStackTrace();
                    return Mono.empty();
                });
    }

    public Mono<Void> handleMessage (WebSocketSession session, MessageEvent msg){
        Long projectId = msg.getPayload().getProjectId();
        msg.getPayload().setSentAt(LocalDateTime.now());
        msg.getPayload().setType(PacketType.chat_message);
        Map<String, SessionContext> contexts = activeUsers.get(projectId);
        SessionContext context = contexts.get(session.getId());

        System.out.println("Handling message broadcast");
        if(context==null)
            return deny(session, projectId);

        Collection<SessionContext> members = contexts.values();
        if(members.isEmpty())
            return Mono.empty();
        System.out.println("Active users");
        System.out.println(activeUsers.get(projectId).size());
        Mono<Void> publishMessage = redisService.publishMessage(context, msg)
                .then( Mono.defer(()->Flux.fromIterable(members)
                        .flatMap(member-> member.send(serialize(msg.getPayload())))
                        .doOnNext(System.err::println)
                        .then()));

        System.out.println("In active users");
        System.out.println(inactiveUser.get(projectId).size());
        Mono<Void> publishUnreadCount = Flux.fromIterable(inactiveUser.get(projectId).values())
                .doOnNext(ses->ses.increaseUnreadCount(projectId))
                .doOnNext(ses->ses.addGroupMsgPointer(projectId, msg.getPayload().getMsgId()))
                .flatMap(ses->ses.sendUnreadCount(projectId))
                .doOnError(System.err::println)
                .then();

        return publishMessage.then(publishUnreadCount);
    }

    // Move from inactive user to active user
    public Mono<Void> handleJoin (WebSocketSession session, MessageEvent msg){
        return Mono.fromCallable(()->{
            Long projectId = msg.getPayload().getProjectId();
            Map<String, SessionContext> sessionContextMap = inactiveUser.get(projectId);

            if(sessionContextMap == null){
                // Perhaps user was not registered to a project at the time of connection
                try{
                    handleConnected(session);
                    sessionContextMap = inactiveUser.get(projectId);
                } catch (RuntimeException e) {
                    return null;
                }
            }

            SessionContext sessionContext = sessionContextMap.get(session.getId());

            if(sessionContext == null) {
                deny(session, projectId);
                return null;
            }

            sessionContext.addGroupMsgPointer(projectId, "");
            sessionContext.resetUnreadCount(projectId);

            activeUsers.get(projectId).put(session.getId(), sessionContext);
            inactiveUser.get(projectId).remove(session.getId());
            return null;
        });
    }

    // Move from active user to inactive user
    public Mono<Void> handleLeave (WebSocketSession session, MessageEvent msg){
        return Mono.fromCallable(()->{
            Long projectId = msg.getPayload().getProjectId();
            Map<String, SessionContext> sessionContextMap = activeUsers.get(projectId);
            if(sessionContextMap == null) {
                deny(session, projectId);
                return null;
            }
            SessionContext sessionContext = sessionContextMap.get(session.getId());

            if(sessionContext == null) {
                deny(session, projectId);
                return null;
            }

            inactiveUser.get(projectId).put(session.getId(), sessionContext);

            activeUsers.get(projectId).remove(session.getId());
            return null;
        });
    }

    public Mono<Void> handleConnected(WebSocketSession session){
        SessionContext sessionContext = createContext(session);
        System.out.println("Session created "+sessionContext.getUserId());
        return redisService.getUserProjects(sessionContext.getUserId())
                .doOnNext(projectId->{
                    sessionContext.getJoinedProjects().add(projectId);
                    registerGroup(projectId, sessionContext);
                }).then();
    }
    // If a session is found in an inactive group, we do not need to deal with the pointer
    // If the session is found in an active group we use the groupId to request for the latest offset from redis
    // When it is done, we call the save offset method from redis
    public void cleanUpSocket(WebSocketSession session) {

        System.out.println("Clean up has started");

        SessionContext sessionContext = removeSession(inactiveUser, session, true);

        if (sessionContext == null) {
            sessionContext = removeSession(activeUsers, session, false);
        }

        if (sessionContext != null) {
            redisService.writeUserProjectPointers(
                    sessionContext,
                    sessionContext.getGroup_pointers()
            );
        }

        System.out.println("Clean up has ended");
    }

    private SessionContext removeSession(
            Map<Long, Map<String, SessionContext>> source,
            WebSocketSession session,
            boolean updateOffset
    ) {

        for (Map.Entry<Long, Map<String, SessionContext>> entry : source.entrySet()) {

            Long groupId = entry.getKey();
            Map<String, SessionContext> sessions = entry.getValue();

            if (sessions == null) {
                continue;
            }

            SessionContext ctx = sessions.remove(session.getId());

            if (ctx != null) {

                if (updateOffset) {
                    ctx.addGroupMsgPointer(
                            groupId,
                            RedisService.getMessageOffset(groupId)
                    );
                }

                return ctx;
            }
        }

        return null;
    }

    public Mono<MessageEvent> deserialize(String payload){
        return Mono.fromCallable(()->objectMapper.readValue(payload, MessageEvent.class));
    }

    public String serialize(Object object){
        return objectMapper.writeValueAsString(object);
    }
}
