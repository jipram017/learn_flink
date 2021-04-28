

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SlidingWindow {
    public static void slidingWindow(DataStream<String> dataStream){
        DataStream<Tuple2<String, Integer>> outStream = dataStream
                .flatMap(new SplitSpecial())
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce(new TotalCount());

        outStream.print();
    }

    public static class SplitSpecial implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String input, Collector<Tuple2<String, Integer>> out) throws Exception{
            String[] vals = input.split("\\s+");
            for(String val : vals){
                out.collect(new Tuple2<String, Integer>(val.trim(), 1));
            }
        }
    }

    public static class TotalCount implements ReduceFunction<Tuple2<String, Integer>>{
        public Tuple2<String, Integer> reduce(
                Tuple2<String, Integer> cumulative,
                Tuple2<String, Integer> input
        ){
            return new Tuple2<String, Integer>(
                    input.f0,
                    cumulative.f1 + input.f1
            );
        }
    }
}
