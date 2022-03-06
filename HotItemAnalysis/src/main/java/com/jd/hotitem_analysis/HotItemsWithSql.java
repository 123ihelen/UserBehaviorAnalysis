package com.jd.hotitem_analysis;

import com.jd.hotitem_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Walter
 * @create 2022-02-28-8:29 下午
 */
public class HotItemsWithSql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> inputStream = env.readTextFile("/Users/wulinchong/IdeaProjects/UserBehaviorAnalysis/HotItemAnalysis/src/main/resources/UserBehavior.txt");

        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]), Integer.valueOf(fields[2]), String.valueOf(fields[3]), Long.valueOf(fields[4]));

        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimeStamp() * 1000L;
            }
        });

        EnvironmentSettings settings= EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();


        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);

        //将流转化为表
        Table dataTable = tabEnv.fromDataStream(dataStream, "itemId,behavior,timeStamp.rowtime as ts");


        // 分组开窗

        //table api
        Table windowAggTable = dataTable.filter("behavior=='pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId,w")
                .select("itemId,w.end as windowEnd,itemId.count as cnt");


        //利用开窗函数 对count值进行排序并获取row number

        //sql

        DataStream<Row> aggStream = tabEnv.toAppendStream(windowAggTable, Row.class);
        tabEnv.createTemporaryView("agg",aggStream,"itemId,windowEnd,cnt");

        Table resultTable = tabEnv.sqlQuery("select * from " +
                "(select *,row_number()over(partition by windowEnd order by cnt desc ) as row_num  from agg) " +
                "where row_num<=5");

        //tabEnv.toRetractStream(resultTable,Row.class).print();

        //纯sql实现

        tabEnv.createTemporaryView("data_table1",dataStream,"itemId,behavior,timeStamp.rowtime as ts");

        Table resultSqlTable = tabEnv.sqlQuery("select * from " +
                "(select *,row_number()over(partition by windowEnd order by cnt desc ) as row_num  from " +
                "(select itemId,count(itemId) as cnt" +
                ",HOP_END(ts,interval '5' minute,interval '1' hour) as windowEnd " +
                " from data_table1 " +
                " where behavior='pv' " +
                " group by itemId " +
                ",HOP(ts,interval '5' minute,interval '1' hour)" +
                "))" +
                "where row_num<=5");

        //1234556

        //hot-fix
        tabEnv.toRetractStream(resultSqlTable,Row.class).print();
        env.execute();

    }
}
