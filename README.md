# Jason ETL - 股票資料串流處理系統

一個基於 Java 的即時股票資料抓取與處理系統，使用 Kafka、Flink 和 PostgreSQL 進行資料串流處理。

## 系統架構

```
股票網站爬蟲 → Kafka Producer → Kafka → Flink Consumer → PostgreSQL
     ↓              ↓              ↓         ↓           ↓
  Yahoo Finance   StockParser   Kafka Topic  KafkaToDb   資料庫
```

## 功能特色

- **即時股票資料抓取**：從 Yahoo Finance 爬取台股即時價格
- **串流資料處理**：使用 Apache Flink 進行即時資料處理與計算
- **訊息佇列**：Apache Kafka 作為資料傳輸中介
- **資料持久化**：PostgreSQL 儲存處理後的資料
- **定時任務**：每 10 秒自動抓取一次股票資料
- **互動式介面**：可動態輸入要監控的股票代碼

## 技術棧

- **Java 17**
- **Apache Kafka 3.6.1** - 訊息佇列
- **Apache Flink 1.18.0** - 串流處理
- **PostgreSQL 42.7.1** - 資料庫
- **Jsoup 1.17.2** - 網頁爬蟲
- **Maven** - 專案管理

## 專案結構

```
src/main/java/com/stock_etl/jason/
├── main.java                    # 主程式入口
├── pojo/
│   ├── StockData.java          # 股票資料 POJO
│   └── StockDataFlink.java     # Flink 股票資料格式
└── util/
    ├── kafka/
    │   └── Producer.java       # Kafka 生產者
    ├── flink/
    │   └── KafkaToDb.java      # Flink 消費者處理
    └── webparser/
        ├── StockParser.java    # 股票爬蟲解析器
        └── SS.java            # 股票設定物件
```

## 環境需求

- Java 17+
- Maven 3.6+
- Apache Kafka (localhost:29092)
- PostgreSQL 資料庫
- 網路連線 (用於爬取 Yahoo Finance)

## 安裝與執行

### 1. 啟動必要服務

```bash
# 啟動 Kafka
docker-compose up -d kafka

# 啟動 PostgreSQL
docker-compose up -d postgresql
```

### 2. 編譯專案

```bash
mvn clean compile
```

### 3. 執行程式

```bash
# 啟動股票資料抓取與傳送
mvn exec:java -Dexec.mainClass="com.stock_etl.jason.main"

# 啟動 Flink 資料處理
mvn clean compile exec:exec -Dexec.executable="java" -Dexec.args="-classpath %classpath --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED com.stock_etl.jason.util.flink.KafkaToDb"
```

## 使用方式

1. **啟動主程式**後，系統會提示輸入股票代碼
2. **輸入股票代碼**（例如：2330.TW、0050.TW）
3. 輸入 `done` 結束輸入並開始資料抓取
4. 系統會**每 10 秒**自動抓取一次所有輸入股票的即時價格
5. 資料會透過 Kafka 傳送給 Flink 進行處理
6. 處理結果會儲存到 PostgreSQL 資料庫

## 資料流程

1. **StockParser** 從 Yahoo Finance 爬取股票資料
2. **Producer** 將資料發送到 Kafka Topic "stock"
3. **KafkaToDb** 從 Kafka 消費資料並進行處理
4. Flink 進行 10 秒時間窗格的價格平均計算
5. 處理結果儲存到 PostgreSQL 資料庫

## 設定說明

### Kafka 設定
- Bootstrap Server: `localhost:29092`
- Topic: `stock`

### PostgreSQL 設定
- 需要建立對應的資料表儲存股票資料
- 連線設定可透過環境變數或 `.env` 檔案設定

## 注意事項

- 確保 Kafka 和 PostgreSQL 服務已正確啟動
- 股票代碼格式請使用 Yahoo Finance 的格式（如：2330.TW）
- 網路連線穩定性會影響爬蟲效果
- 建議在生產環境中加入錯誤重試機制和監控

## 開發說明

### 新增股票解析功能
修改 `StockParser.java` 中的 `parser_price()` 方法來適應不同的網頁結構。

### 自訂 Flink 處理邏輯
修改 `KafkaToDb.java` 中的處理函數來實現不同的業務邏輯。

### 資料庫結構
根據 `StockData` 和 `StockDataFlink` 的結構設計對應的資料表。
