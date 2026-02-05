package com.stock_etl.jason.util.webparser;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;

public class StockParser {
    private Connection conn;
    
    public StockParser(String url){
        this.conn = Jsoup.connect(url).userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");
    }

    public String parser_price() throws IOException {
        Document document = this.conn.get();
        return document.selectFirst("span[class*='Fz(32px)'][class*='Fw(b)']").text();
    }
}
