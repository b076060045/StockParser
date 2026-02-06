package com.stock_etl.jason.util.webparser;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.stock_etl.jason.main;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


//多例模式
public class StockParser {
    private String symbol;
    private String url;
    private Connection conn;

    private static Map<String, StockParser> instanceMap = new ConcurrentHashMap<>();

    public void setSymbol(String symbol){
        this.symbol = symbol;
    }

    public void setUrl(String url){
        this.url = url;
    }

    private StockParser(String symbol){
        this.symbol = symbol;
        this.url = "https://tw.stock.yahoo.com/quote/" + symbol;
        this.conn = Jsoup.connect(this.url)
                    .userAgent("Mozilla/5.0");
    }

    public static StockParser getInstance(String symbol) {
        return instanceMap.computeIfAbsent(symbol, s -> {
            System.out.println("建立新的 Parser 給: " + s);
            return new StockParser(symbol); // ✅ 這裡是類別內部，可以直接 new private 建構子
        });
    }
    public String getSymbol() {
        return this.symbol;
    }
    
    public Map<String, String> parser_price() throws IOException {
        Document doc = this.conn.get();
        String name = doc.selectFirst("h1[class*='C($c-link-text)']").text();
        String price = doc.selectFirst("span[class*='Fz(32px)'][class*='Fw(b)']").text();
        return Map.of("name", name, "price", price);
    }
};
