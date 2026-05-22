package com.givr.chat.Dto;

import lombok.Data;
import org.springframework.data.redis.connection.stream.RecordId;


@Data
public class WriteAcknowledgement {
    private String username;
    private RecordId ackId;
}
