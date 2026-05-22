package com.givr.chat.Dto;

import com.givr.chat.enums.ClientEvent;
import lombok.Data;
import lombok.ToString;

@Data
public class MessageEvent {
    private ClientEvent type;
    private Message payload;
}
