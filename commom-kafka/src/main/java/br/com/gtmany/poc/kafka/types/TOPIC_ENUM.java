package br.com.gtmany.poc.kafka.types;

import java.util.prefs.AbstractPreferences;

public enum TOPIC_ENUM {
    ECOMMERCE_NEW_ORDER,
    ECOMMERCE_SEND_EMAIL,
    ECOMMERCE_NEW_USER,
    ECOMMERCE_USER_GENERATE_READING_REPORT,
    ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS,
    ECOMMERCE_DEAD_LETTER;
}
