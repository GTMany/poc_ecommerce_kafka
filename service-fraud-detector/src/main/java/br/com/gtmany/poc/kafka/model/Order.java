package br.com.gtmany.poc.kafka.model;

import java.math.BigDecimal;

public class Order {
    private final String userId, orderId;
    private final BigDecimal amount;

    public Order(String userId, String orderId, BigDecimal amount) {
        this.userId = userId;
        this.orderId = orderId;
        this.amount = amount;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getUserId() {
        return userId;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
