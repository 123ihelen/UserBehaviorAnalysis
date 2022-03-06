package com.jd.hotitem_analysis;

import com.jd.hotitem_analysis.beans.ItemViewCount;
import com.jd.hotitem_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author Walter
 * @create 2022-02-28-2:58 下午
 */
public class HotItems {

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
                return element.getTimeStamp()*1000L;
            }
        });

       //dataStream.print("og");

//        inputStream.map(new FlatMapFunction<String, Object>() {})


        DataStream<ItemViewCount> windowAggStream=dataStream
                .filter(data->"pv".equals(data.getBehavior()))
                .keyBy("itemId")
                .timeWindow(Time.hours(1),Time.minutes(5))
                .aggregate(new ItemCountAgg(),new WindowItemCountResult())
//                .process()
                ;

        //windowAggStream.print();

        DataStream<String> resultStream=windowAggStream
                .keyBy("windowEnd")
                .process(new TopHotItem(5))
                ;

        resultStream.print();

        env.execute("hot items analysis");

    }

    //自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long>{


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    // 自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long,ItemViewCount, Tuple, TimeWindow>{


        @Override
        public void apply(Tuple tunple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {

            Long itemId=tunple.getField(0);
            Long windowEnd= window.getEnd();
            Long count=input.iterator().next();
            out.collect(new ItemViewCount(itemId,windowEnd,count));
        }
    }

    public static class TopHotItem extends KeyedProcessFunction <Tuple,ItemViewCount,String>{

        //定于top N
        private Integer topSize;

        public TopHotItem(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount


        ListState<ItemViewCount> itemViewCountListState;
        @Override
        public void open(Configuration parameters) throws Exception {

            itemViewCountListState=getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-cout-list",ItemViewCount.class));


        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            //每来一条数据 存在List,并注册定时器
            itemViewCountListState.add(value);

            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，当前已收集到所有数据，排序输出

            ArrayList<ItemViewCount> itemViewCounts= Lists.newArrayList(itemViewCountListState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue()-o1.getCount().intValue();
                }
            });

            //将排名信息格式为string

            StringBuilder resultBuilder = new StringBuilder();

            resultBuilder.append("======\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp-1)).append("\n");

            //遍历列表 取出top N;

            for(int i=0;i< Math.min(topSize,itemViewCounts.size());i++){
                ItemViewCount itemViewCount=itemViewCounts.get(i);
                resultBuilder.append("NO:")
                        .append(i+1).append(":")
                        .append("商品ID：").append(itemViewCount.getItemId())
                        .append("热门度：").append(itemViewCount.getCount())
                        .append("\n");
            }

            resultBuilder.append("======\n\n");

            //控制输出频率

            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
                    
        }

    }

}
