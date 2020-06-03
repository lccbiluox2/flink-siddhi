package org.apache.flink.streaming.siddhi.function;

import io.siddhi.core.event.Event;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Map;

public class EventPrinters {

    public static void print(Event[] events) {
        System.out.println(Arrays.deepToString(events));
    }

    public static void print(Map[] events) {
        System.out.println(Arrays.deepToString(events));
    }

    public static void print(Map event) {
        System.out.println(event);
    }


    public static void print(long timestamp, Event[] inEvents, Event[] removeEvents) {
        StringBuilder sb = new StringBuilder();
        sb.append("Events{ @timestamp = ").append(timestamp).
                append(", inEvents = ").append(Arrays.deepToString(inEvents)).
                append(", RemoveEvents = ").append(Arrays.deepToString(removeEvents)).append(" }");
        System.out.println(sb.toString());
    }

}