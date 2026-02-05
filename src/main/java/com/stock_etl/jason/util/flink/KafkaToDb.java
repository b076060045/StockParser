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

// Window 與時間相關
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

// Kafka 相關
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

// 其他工具
import org.apache.flink.util.Collector;
import java.lang.Iterable;

public class KafkaToDb {
    
    public KafkaToDb() throws Exception {
        // 1. 初始化環境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 建立 Kafka Source (新版 API)
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:29092") // 建議先用 9092，除非你確定 Docker 映射是 29092
                .setTopics("news")
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

        // 4. 簡單處理並印出
        DataStream<String> upper_stream = stream.map(new UpperCaseMap());

        upper_stream.map(new LowerCaseMap()).keyBy(new MyKey()).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).aggregate(new WordCountAggregator(), new MyWindowResult()).print();

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
class WordCountAggregator implements AggregateFunction<String, Long, Long> {
    @Override public Long createAccumulator() { return 0L; }
    @Override public Long add(String value, Long acc) { return acc + 1L; }
    @Override public Long getResult(Long acc) { return acc; } // 只回傳數字
    @Override public Long merge(Long a, Long b) { return a + b; }
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