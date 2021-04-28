

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingWindow {
    public static void tumblingWindow(DataStream<String> dataStream){
        DataStream<Tuple2<String, Double>> outStream = dataStream
                .map(new RowParser())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new SumAndCount())
                .map(new Average());

        outStream.print();
    }

    public static class RowParser implements MapFunction<String, Tuple3<String, Double, Integer>>{
        public Tuple3<String, Double, Integer> map(String input) throws Exception {
            try {
                String[] fields = input.split("\\s+");
                return new Tuple3<String, Double, Integer>(
                        fields[0].trim(),
                        Double.parseDouble(fields[1]),
                        1
                );
            } catch (Exception e){
                e.getStackTrace();
            }
            return null;
        }
    }

    public static class SumAndCount implements ReduceFunction<Tuple3<String, Double, Integer>> {
        public Tuple3<String, Double, Integer> reduce(
                Tuple3<String, Double, Integer> cumulative,
                Tuple3<String, Double, Integer> input
        ){
            return new Tuple3<String, Double, Integer>(
                    input.f0,
                    cumulative.f1 + input.f1,
                    cumulative.f2 + 1
            );
        }
    }

    public static class Average implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
        public Tuple2<String, Double> map(Tuple3<String, Double, Integer> input) throws Exception {
            return new Tuple2<String, Double>(
                    input.f0,
                    input.f1 / input.f2
            );
        }
    }
}
