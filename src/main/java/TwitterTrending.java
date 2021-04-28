

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.functions.windowing.*;
import org.apache.flink.streaming.api.windowing.time.*;
import org.apache.flink.streaming.api.windowing.windows.*;
import org.apache.flink.streaming.connectors.twitter.*;
import org.apache.flink.util.*;

import java.io.*;
import java.util.*;

public class TwitterTrending {
    public static void twitterTrending(StreamExecutionEnvironment env, ParameterTool params) throws Exception {
        Properties props = new Properties();
/*        props.setProperty(TwitterSource.CONSUMER_KEY, params.getRequired("consumer_key"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, params.getRequired("consumer_secret"));
        props.setProperty(TwitterSource.TOKEN, params.getRequired("token"));
        props.setProperty(TwitterSource.TOKEN_SECRET, params.getRequired("token_secret"));*/

        props.setProperty(TwitterSource.CONSUMER_KEY, "RCYpjz70HTh8DO3mf89v9AwCA");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "iJeUe9pq9mLhaQPqFsr1iq8wmT1N2MR5TPUiiYvqJMd6VSg0Po");
        props.setProperty(TwitterSource.TOKEN, "1386195886707855362-LHxhQwbWnEPRfKzJTT3A4alDVEm2Lk");
        props.setProperty(TwitterSource.TOKEN_SECRET, "OISfk4JzDI3QsoaR1H0DIvNh1os3lUesIVMW4x6LRWMpU");

        // Configure Twitter source
        TwitterSource twitterSource = new TwitterSource(props);
        DataStream<String> jsonTweets = env.addSource(twitterSource);

        DataStream<Tuple3<String, String, Integer>> outStream = jsonTweets
                .flatMap(new ExtractHashTags())
                .keyBy(0)
                .timeWindow(Time.seconds(30))
                .sum(2)
                .filter(new FilterHashTags());

        DataStream<LinkedHashMap<String, Integer>> ds = outStream
                .timeWindowAll(Time.seconds(30))
                .apply(new MostPopularTags());

/*        DataStream<TweetsCount> ds = outStream
                .timeWindowAll(Time.seconds(30))
                .apply(new GetTopHashTags());*/

        ds.print();
    }

    private static class TweetsCount implements Serializable {
        private static final long serialVersionUID = 1L;
        private final Date windowStart;
        private final Date windowEnd;
        private final String hashTag;
        private final String location;
        private final int count;

        public TweetsCount(long windowStart, long windowEnd, String hashTag, String location, int count){
            this.windowStart = new Date(windowStart);
            this.windowEnd = new Date(windowEnd);
            this.hashTag = hashTag;
            this.location = location;
            this.count = count;
        }

        @Override
        public String toString() {
            return "TweetsCount{" +
                    "windowStart=" + windowStart +
                    ", windowEnd=" + windowEnd +
                    ", hashTag='" + hashTag + '\'' +
                    ", location='" + location + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    private static class ExtractHashTags implements FlatMapFunction<String, Tuple3<String, String, Integer>>{
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void flatMap(String tweets, Collector<Tuple3<String, String, Integer>> collector) throws Exception {
            JsonNode jsonNode = mapper.readTree(tweets);
            JsonNode entities = jsonNode.get("entities");
            if (entities == null) return;
            JsonNode hashtags = entities.get("hashtags");
            if (hashtags == null) return;

            JsonNode user = jsonNode.get("user");
            if (user == null) return;
            JsonNode location = user.get("location");
            if (location == null) return;

            for (Iterator<JsonNode> iter = hashtags.elements(); iter.hasNext();){
                JsonNode node = iter.next();
                String hashtag = node.get("text").textValue();
                if(hashtag.matches("\\w+")){
                    collector.collect(new Tuple3<String, String, Integer>(hashtag, location.textValue(), 1));
                }
            }
        }
    }

    private static class FilterHashTags implements FilterFunction<Tuple3<String,String, Integer>>{

        @Override
        public boolean filter(Tuple3<String, String, Integer> hashtag) throws Exception {
            return hashtag.f2 != 1;
        }
    }

    private static class GetTopHashTags implements AllWindowFunction<Tuple3<String, String, Integer>,
            TweetsCount, TimeWindow> {

        @Override
        public void apply(TimeWindow timeWindow, Iterable<Tuple3<String, String, Integer>> hashtags, Collector<TweetsCount> collector) throws Exception {
            Tuple3<String, String, Integer> topHashTag = new Tuple3<>("", "", 0);
            for (Tuple3<String, String, Integer> hashtag : hashtags) {
                if(hashtag.f2 > topHashTag.f2){
                    topHashTag = hashtag;
                }
            }

            collector.collect(new TweetsCount(
                    timeWindow.getStart(), timeWindow.getEnd(),
                    topHashTag.f0, topHashTag.f1, topHashTag.f2));
        }
    }

    // Window functions
    public static class MostPopularTags implements AllWindowFunction<Tuple3<String, String, Integer>, LinkedHashMap<String, Integer>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple3<String, String, Integer>> tweets, Collector<LinkedHashMap<String, Integer>> collector) throws Exception {
            HashMap<String, Integer> hmap = new HashMap<String, Integer>();

            for (Tuple3<String, String, Integer> t: tweets) {
                int count = 0;
                if (hmap.containsKey(t.f0)) {
                    count = hmap.get(t.f0);
                }
                hmap.put(t.f0, count + t.f2);
            }

            Comparator<String> comparator = new ValueComparator(hmap);
            TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(comparator);

            sortedMap.putAll(hmap);

            LinkedHashMap<String, Integer> sortedTopN = sortedMap
                    .entrySet()
                    .stream()
                    .limit(10)
                    .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

            collector.collect(sortedTopN);
        }
    }

    public static class ValueComparator implements Comparator<String> {
        HashMap<String, Integer> map = new HashMap<String, Integer>();

        public ValueComparator(HashMap<String, Integer> map){
            this.map.putAll(map);
        }

        @Override
        public int compare(String s1, String s2) {
            if (map.get(s1) >= map.get(s2)) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}
