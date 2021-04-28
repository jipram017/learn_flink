

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.*;
import org.apache.flink.streaming.connectors.twitter.*;

import java.util.*;

public class TwitterLocationCount {
    public static void twitterLocationCount(StreamExecutionEnvironment env) throws Exception {

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "RCYpjz70HTh8DO3mf89v9AwCA");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "iJeUe9pq9mLhaQPqFsr1iq8wmT1N2MR5TPUiiYvqJMd6VSg0Po");
        props.setProperty(TwitterSource.TOKEN, "1386195886707855362-LHxhQwbWnEPRfKzJTT3A4alDVEm2Lk");
        props.setProperty(TwitterSource.TOKEN_SECRET, "OISfk4JzDI3QsoaR1H0DIvNh1os3lUesIVMW4x6LRWMpU");
        DataStream<String> streamSource = env.addSource(new TwitterSource(props));

        DataStream<Tuple2<String, Integer>> tweets = streamSource
                .map(new LocationCount())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);

        tweets.print();
    }

    public static class LocationCount implements MapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper jsonParser;
        public Tuple2<String, Integer> map(String value) throws Exception {
            if(jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            String location = jsonNode.has("user") && jsonNode.get("user").has("location")
                    ? jsonNode.get("user").get("location").textValue() : "unknown";
            return new Tuple2<String, Integer>(location, 1);
        }
    }
}
