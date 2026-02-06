package com.stock_etl.jason.util.flink;

// Flink 核心基礎
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

// Window 與時間相關
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

// Kafka 相關
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import com.stock_etl.jason.pojo.StockData;
import com.stock_etl.jason.pojo.StockDataFlink;

// 其他工具
import org.apache.flink.util.Collector;
import java.lang.Iterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;

public class KafkaToDb {
    
    public KafkaToDb() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        // 1. 初始化環境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 建立 Kafka Source (新版 API)
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:29092") // 建議先用 9092，除非你確定 Docker 映射是 29092
                .setTopics("stock")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // --- 重點修正：刪掉 env.addSource(source)，改用下面的 fromSource ---

        // 3. 將 Source 轉為 DataStream
        DataStream<String> stream = env.fromSource(
                source, 
                WatermarkStrategy.noWatermarks(), 
                "Kafka Source"
        );

        DataStream<StockDataFlink> stockStream = stream.map(s -> {
            try {
                JsonNode node = mapper.readTree(s);
                StockDataFlink data = new StockDataFlink();
                data.name = node.get("name").asText();
                // 處理價格字串，去掉逗號並轉成 double
                data.price = Double.parseDouble(node.get("price").asText().replace(",", ""));
                return data;
            } catch (Exception e) {
                System.err.println("跳過錯誤資料: " + s);
                return null; 
            }
        }).filter(Objects::nonNull); // 過濾掉解析失敗的資料

        // 把kafka的資料根據股票代碼分
        stockStream.keyBy(s -> s.name)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10))).aggregate(new WordCountAggregator(), new ProcessWindowFunction<Long, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) {
                Long count = elements.iterator().next();
                // 這裡可以自定義輸出的格式，把 Key 加上去
                out.collect("Key: " + key + ", Count: " + count + " (Window: " + context.window().getEnd() + ")");
            }
        }).print();

        stockStream.keyBy(s -> s.name)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10))).aggregate(new PriceAverageAggregator(), new ProcessWindowFunction<Double, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Double> elements, Collector<String> out) {
                Double count = elements.iterator().next();
                // 這裡可以自定義輸出的格式，把 Key 加上去
                out.collect("Key: " + key + ", Price_avg: " + count + " (Window: " + context.window().getEnd() + ")");
            }
        }).print();

        // 5. 啟動任務
        env.execute("Flink Kafka ETL Test");
    }

    public static void main(String[] args) {
        try {
            new KafkaToDb();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class UpperCaseMap implements MapFunction<String, String> {
    @Override
    public String map(String value) throws Exception {
        return value;
    }
}

class LowerCaseMap implements MapFunction<String, String> {
    @Override
    public String map(String value) throws Exception {
        return value.toLowerCase();
    }
}

class MyKey implements KeySelector<String, String> {
    @Override
    public String getKey(String value) throws Exception {
        return value;
    }
}

// 1. 修改 WordCountAggregator，輸出單純的 Long
class WordCountAggregator implements AggregateFunction<StockDataFlink, Long, Long> {
    @Override public Long createAccumulator() { return 0L; }
    @Override public Long add(StockDataFlink value, Long acc) { return acc + 1L; }
    @Override public Long getResult(Long acc) { return acc; } // 只回傳數字
    @Override public Long merge(Long a, Long b) { return a + b; }
}

class AverageAccumulator {
    Double sum = 0.0;
    long count = 0;
}

class PriceAverageAggregator implements AggregateFunction<StockDataFlink, AverageAccumulator, Double> {

    @Override
    public AverageAccumulator createAccumulator() {
        return new AverageAccumulator();
    }

    @Override
    public AverageAccumulator add(StockDataFlink value, AverageAccumulator acc) {
        acc.sum += value.price;
        acc.count += 1;
        return acc;
    }

    @Override
    public Double getResult(AverageAccumulator acc) {
        // 防止除以 0 的錯誤
        return acc.count == 0 ? 0.0 : acc.sum / acc.count;
    }

    @Override
    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
        a.sum += b.sum;
        a.count += b.count;
        return a;
    }
}

// 2. 增加一個 WindowFunction 來把「字」跟「次數」拼起來
// <輸入(Long), 輸出(String), Key型別(String), 視窗型別(TimeWindow)>
class MyWindowResult extends ProcessWindowFunction<Long, String, String, TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) {
        Long count = elements.iterator().next();
        out.collect(key + ": " + count); // 這裡的 key 就是那個單詞！
    }
}