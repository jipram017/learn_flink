import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

public class TwitterLocationCount {
    public static void twitterLocationCount(StreamExecutionEnvironment env, ParameterTool params) throws Exception {

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, params.getRequired("consumer_key"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, params.getRequired("consumer_secret"));
        props.setProperty(TwitterSource.TOKEN, params.getRequired("token"));
        props.setProperty(TwitterSource.TOKEN_SECRET, params.getRequired("token_secret"));
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
