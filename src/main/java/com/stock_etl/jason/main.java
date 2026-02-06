package com.stock_etl.jason;

import com.stock_etl.jason.util.webparser.StockParser;
import com.stock_etl.jason.pojo.StockData;
import com.stock_etl.jason.util.kafka.Producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;

public class main { // 建議類別名稱大寫 Main
    public static void main(String[] args) {
        Send send = new Send();
        send.ChooseStock();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(); 
        Runnable task = () -> {
            send.SendInformation();
        };
        scheduler.scheduleAtFixedRate(task, 0, 10, TimeUnit.SECONDS);
    }
}


class Send{

    private List<String> name;

    public void ChooseStock(){

        Scanner scanner = new Scanner(System.in);
        name = new ArrayList<String>();
        System.out.println("=== 股票資料抓取系統 ===");
        System.out.println("請輸入股票代碼 (例如: 2330.TW)，輸入 'done' 結束並開始傳送：");

        // 1. 收集使用者輸入
        while (true) {
            System.out.print("請輸入代碼: ");
            String input = scanner.nextLine().trim().toUpperCase();

            if (input.equalsIgnoreCase("done")) {
                break;
            }

            if (input.isEmpty()) {
                System.out.println("[錯誤] 輸入不能為空。");
                continue;
            }

            name.add(input); // 將輸入的代碼加入 List
        }
    }
    public void SendInformation(){
        try {
            // 2. 檢查是否有輸入股票，有的話才啟動 Producer
            if (!name.isEmpty()) {
                Producer producer = new Producer("localhost:29092");
                StockData stockData = new StockData();

                System.out.println("\n開始抓取資料並發送至 Kafka...");

                for (String s : name) {
                    try {
                        StockParser parser = StockParser.getInstance(s);
                        Map<String, String> price = parser.parser_price();
                        
                        System.out.println("股票: " + s + " | 價格: " + price.get("price") + " | 名稱: " + price.get("name"));
                        
                        stockData.setData(price.get("name"), price.get("price"));
                        String data = stockData.toString();
                        
                        producer.send("stock", s, data);
                    } catch (Exception e) {
                        System.out.println("[錯誤] 處理股票 " + s + " 時發生問題: " + e.getMessage());
                    }
                }
                producer.close();
                System.out.println("所有資料發送完畢。");
            } else {
                System.out.println("未輸入任何股票，程式結束。");
            }

        } catch (Exception e) {
            System.out.println("發生非預期錯誤: " + e.getMessage());
            e.printStackTrace(); 
        }
    }
}