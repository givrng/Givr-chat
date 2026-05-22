package com.givr.chat.handlers;

import com.givr.chat.enums.AccountType;

public record GivrUserAuth (String email, String userId, AccountType accountType){
}
