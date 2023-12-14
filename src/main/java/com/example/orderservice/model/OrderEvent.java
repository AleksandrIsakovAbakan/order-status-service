package com.example.orderservice.model;

import lombok.Data;

@Data
public class OrderEvent {

    private String product;

    private Integer quantity;
}
