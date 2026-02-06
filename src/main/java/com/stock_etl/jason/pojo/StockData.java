package com.stock_etl.jason.pojo;

public class StockData {
    public String name;
    public String price;

    public StockData() {
    }

    public void setData(String name, String price){
        this.name = name;
        this.price = price;
    }

    @Override
    public String toString() {
        return "{\"name\":\"" + name + "\", \"price\":\"" + price + "\"}";
    }
}