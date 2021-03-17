package com.youfan.flinkpractice;

import com.youfan.entity.BrandLike;
import com.youfan.kafka.KafkaEvent;
import com.youfan.map.BrandLikeMap;
import com.youfan.reduce.BrandLikeReduce;
import com.youfan.reduce.BrandLikeSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import com.youfan.kafka.KafkaEventSchema;
import javax.annotation.Nullable;

/**
 * author  Lock.xia
 * Date 2019-03-22
 */
public class BrandLikeTaskNew {
    public static void main(String[] args) {

        // 1.配置 kafka 的参数,并将参数放入参数工具中
        args = new String[]{"--input-topic", "scanProductLog", "--bootstrap.servers", "****", "--zookeeper.connect", "****", "--group-id", "lock"};
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // 2.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging(); // 禁止 jobManager 对于状态更新的打印
        // 如果发生故障，可以通过检查点机制来进行重启【重启次数 / 重启间隔时间】,每 5S 创建一个检查点文件
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 一次性語義
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        // 注册为全局作业参数的参数	ExecutionConfig	可以作为JobManager	Web界面中 的配置值以及用户定义的所有函数进行访问
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 将流的时间设置为时间的发生时间 eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 3.Flink Source
        DataStream<KafkaEvent> input = env.addSource(
                // consumer
                new FlinkKafkaConsumer010<>(
                        parameterTool.getRequired("input-topic"),
                        new KafkaEventSchema(),
                        parameterTool.getProperties())
                        .assignTimestampsAndWatermarks(new CustomWatermarkExtractorNew()));

        // 4.转化
        DataStream<BrandLike> brandLikeMap = input.flatMap(new BrandLikeMap());
        DataStream<BrandLike> branddLikeReduce = brandLikeMap.keyBy("groupbyfield").timeWindowAll(Time.seconds(2)).reduce(new BrandLikeReduce());

        // 5.将运算好的结果放入相应的数据库中
        branddLikeReduce.addSink(new BrandLikeSink());

        // 6.执行
        try {
            env.execute("brandLike analyse");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    // Flink的延迟机制 ，处理混乱时间的log
    private static class CustomWatermarkExtractorNew implements AssignerWithPeriodicWatermarks<KafkaEvent> {

        private static final long serialVersionUID = -6202990909713340179L;

        private Long currentTimestamp = Long.MIN_VALUE;

        @Override
        public long extractTimestamp(KafkaEvent event, long previousElementTimestamp) {
            this.currentTimestamp = event.getTimestamp();
            return event.getTimestamp();
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }
}
