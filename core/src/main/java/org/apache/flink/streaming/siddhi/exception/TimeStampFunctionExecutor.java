package org.apache.flink.streaming.siddhi.exception;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.timestamp.TimestampGenerator;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Extension(
        name = "now",
        namespace = "custom",
        description = "return current time yyyy-MM-dd HHLmm:ss.format(new Date()",
        parameters = {

        },
        returnAttributes = @ReturnAttribute(
                description = "return current time yyyy-MM-dd HHLmm:ss.format(new Date()",
                type = {DataType.STRING}
        ),
        examples = @Example(
                syntax = "select id,custome:now() as now insert int outStream;",
                description = "noew "
        )
)
public class TimeStampFunctionExecutor extends FunctionExecutor {

    private TimestampGenerator timestampGenerator;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, SiddhiQueryContext siddhiQueryContext) {
        if(attributeExpressionExecutors.length != 0){
            throw new SiddhiAppValidationException("参数不对");
        }

        this.timestampGenerator = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator();
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        long timestamp = timestampGenerator.currentTime();
        return convertTimeToString(timestamp);
    }

    private Object convertTimeToString(long timestamp) {
        DateTimeFormatter dateformat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:m:ss");
        return dateformat.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()));
    }

    @Override
    protected Object execute(Object data, State state) {
        long timestamp = timestampGenerator.currentTime();
        return convertTimeToString(timestamp);
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }
}
