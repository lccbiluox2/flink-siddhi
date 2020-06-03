package org.apache.flink.streaming.siddhi.function;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.flink.streaming.siddhi.exception.TimeStampFunctionExecutor;
import org.junit.Test;

public class TimeStampFunctionExecutorTest {

    /**
     * 测地点：测试自定义函数的使用
     *
     * 测试结果：测试成功
     *
     * Events{ @timestamp = 1591112143141, inEvents = [Event{timestamp=1591112143141, data=[IBM, 2020-06-02 23:35:43], isExpired=false}], RemoveEvents = null }
     * Events{ @timestamp = 1591112143330, inEvents = [Event{timestamp=1591112143330, data=[WSO2, 2020-06-02 23:35:43], isExpired=false}], RemoveEvents = null }
     * Events{ @timestamp = 1591112143330, inEvents = [Event{timestamp=1591112143330, data=[GOOG, 2020-06-02 23:35:43], isExpired=false}], RemoveEvents = null }
     *
     *
     * @throws InterruptedException
     */
    @Test
    public void mainTets() throws InterruptedException {

        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        //Register the extension to Siddhi Manager
        siddhiManager.setExtension("custom:now()", TimeStampFunctionExecutor.class);

        //Siddhi Application
        String siddhiApp = "" +
                "define stream StockStream (symbol string, price long, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "select symbol, custom:now() as totalCount " +
                "insert into Output;";

        //Generating runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        //Adding callback to retrieve output events from query
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinters.print(timestamp, inEvents, removeEvents);
            }
        });

        //Retrieving InputHandler to push events into Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");

        //Starting event processing
        siddhiAppRuntime.start();

        //Sending events to Siddhi
        inputHandler.send(new Object[]{"IBM", 700L, 100L});
        inputHandler.send(new Object[]{"WSO2", 600L, 200L});
        inputHandler.send(new Object[]{"GOOG", 60L, 200L});
        Thread.sleep(500);

        //Shutting down the runtime
        siddhiAppRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();

    }


    /***
     * 测试点：测试将  siddhiManager.setExtension("custom:now()", TimeStampFunctionExecutor.class);
     *       注释掉，还能不能正常使用函数，该函数使用了@Extension注解
     * 测试结果：测试成功
     *        不在同一个同一个包下也是能使用的
     * 打印如下：
     * Events{ @timestamp = 1591145607618, inEvents = [Event{timestamp=1591145607618, data=[IBM, 2020-06-03 08:53:27], isExpired=false}], RemoveEvents = null }
     * Events{ @timestamp = 1591145607909, inEvents = [Event{timestamp=1591145607909, data=[WSO2, 2020-06-03 08:53:27], isExpired=false}], RemoveEvents = null }
     * Events{ @timestamp = 1591145607909, inEvents = [Event{timestamp=1591145607909, data=[GOOG, 2020-06-03 08:53:27], isExpired=false}], RemoveEvents = null }
     * @throws InterruptedException
     */
    @Test
    public void mainTest1() throws InterruptedException {

        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        //Register the extension to Siddhi Manager
//        siddhiManager.setExtension("custom:now()", TimeStampFunctionExecutor.class);

        //Siddhi Application
        String siddhiApp = "" +
                "define stream StockStream (symbol string, price long, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "select symbol, custom:now() as totalCount " +
                "insert into Output;";

        //Generating runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        //Adding callback to retrieve output events from query
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinters.print(timestamp, inEvents, removeEvents);
            }
        });

        //Retrieving InputHandler to push events into Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");

        //Starting event processing
        siddhiAppRuntime.start();

        //Sending events to Siddhi
        inputHandler.send(new Object[]{"IBM", 700L, 100L});
        inputHandler.send(new Object[]{"WSO2", 600L, 200L});
        inputHandler.send(new Object[]{"GOOG", 60L, 200L});
        Thread.sleep(500);

        //Shutting down the runtime
        siddhiAppRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();

    }

}