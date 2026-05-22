package com.givr.chat.Dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.givr.chat.enums.AccountType;
import com.givr.chat.enums.PacketType;
import lombok.Data;
import lombok.ToString;

import java.time.LocalDateTime;

@Data
@ToString
public class Message {
    // Set by user
    private String msgId;
    private Long projectId;

    private String content;
    // Set by server
    private String sentBy;
    private PacketType type;
    private String username;
    @JsonIgnore
    private AccountType role;
    private LocalDateTime sentAt;

    public Message(Long projectId, PacketType type, String content){
        this.projectId = projectId;
        this.type = type;
        this.content = content;
    }
}
