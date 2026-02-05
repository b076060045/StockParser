package com.stock_etl.jason;
import com.stock_etl.jason.util.kafka.Producer;
import io.github.cdimascio.dotenv.Dotenv;
import com.stock_etl.jason.util.webparser.StockParser;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.lang.Runnable;

import com.stock_etl.jason.pojo.StockData;

public class main {
    
    public static void main(String[] args) {

        SendPrice.setStockSymbol("2330.TW");
        
        // 創建定時任務執行器
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        
        // 設置定期任務：每30秒執行一次
        scheduler.scheduleAtFixedRate(new SendPrice(), 0, 30, TimeUnit.SECONDS); // 初始延遲0秒，之後每30秒執行一次
        
        // 添加關閉鉤子，確保程式優雅關閉
        Runtime.getRuntime().addShutdownHook(new Thread(new Close())) ;
        
        System.out.println("股價爬取任務已啟動，每30秒執行一次...");
        System.out.println("按 Ctrl+C 停止程式");
    }
}

class SendPrice implements Runnable{
    private static Producer producer = new Producer("localhost:29092");
    private static String stockSymbol = "0050.TW";
    private static StockParser parser = new StockParser("https://tw.stock.yahoo.com/quote/" + stockSymbol);
    @Override
    public void run() {
        try {
            System.out.println("開始執行股價爬取任務: " + java.time.LocalDateTime.now());
            
            String price = SendPrice.parser.parser_price();
            producer.send("stock", stockSymbol, new StockData(stockSymbol, price).toString());
            producer.producer.flush();
            
            System.out.println("股價資料已發送: " + stockSymbol + " = " + price);
            
        } catch (Exception e) {
            System.err.println("執行任務時發生錯誤: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static Producer getProducer() {
        return producer;
    }

    public static void setStockSymbol(String stockSymbol) {
        SendPrice.stockSymbol = stockSymbol;
        SendPrice.parser = new StockParser("https://tw.stock.yahoo.com/quote/" + stockSymbol);
    }
}

class Close implements Runnable{
    @Override
    public void run() {
        System.out.println("close");     
    }
}  
