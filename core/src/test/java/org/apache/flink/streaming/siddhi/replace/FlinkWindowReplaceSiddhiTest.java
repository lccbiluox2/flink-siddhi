package org.apache.flink.streaming.siddhi.replace;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.siddhi.SiddhiCEP;
import org.apache.flink.streaming.siddhi.entity.Person;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * 主要测试FLink  window 能否替代 flink siddhi
 */
public class FlinkWindowReplaceSiddhiTest {

    /**
     * 测试点：测试siddhi group by 没有窗口操作
     */
    @Test
    public void siddhiGroupNoWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //watermark 自动生成时间，默认每100ms一次
        env.getConfig().setAutoWatermarkInterval(100);


        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", 9993, "\n");

        DataStream<Person> outStream = text.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String s) throws Exception {
                Person exMap = com.alibaba.fastjson.JSON.parseObject(s, Person.class);
                System.out.println("反序列化数据" + exMap.toString());
                return exMap;
            }
        });


        // TODO: 测试的时候发现 这里必须使用keyBy 不然没有数据  outStream.keyBy("deviceId")  没结果
        DataStream<Row> output = SiddhiCEP
                .define("inputStream", outStream, "id", "name", "age")
                .cql("from inputStream [name=='lcc'] #window.timeBatch(600 sec) " +
                        " select 'aa' as bieming," +
                        "custome:now() as startTime," +
                        "custome:now() as endTime," +
                        "custome:uniqueEventId() as eventId," +
                        "sum_mertic " +
                        " group by name" +
                        " having sum_mertic > 2" +
                        " insert into AlertStream")
                .returnAsRow("AlertStream");

        output.print();

        env.execute();

    }
}