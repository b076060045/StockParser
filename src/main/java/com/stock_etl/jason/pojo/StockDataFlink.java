package com.stock_etl.jason.pojo;

import java.sql.Time;

public class StockDataFlink {
    public String name;
    public Double price;
    public Time time;

    public StockDataFlink() {
    }

    public void setData(String name, Double price, Time time){
        this.name = name;
        this.price = price;
        this.time = time;
    }

    @Override
    public String toString() {
        return "{\"name\":\"" + name + "\", \"price\":\"" + price + "\", \"time\":\"" + time + "\"}";
    }
}
