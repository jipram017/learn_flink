import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class CogroupStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool param = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(param);

        DataStream<Tuple2<String, Double>> stream1 = env.socketTextStream("localhost", 8000)
                .map(new RowParser());
        DataStream<Tuple2<String, Double>> stream2 = env.socketTextStream("localhost", 9000)
                .map(new RowParser());

        if(stream1 == null || stream2 == null){
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String, Double>> outStream =
                stream1.coGroup(stream2)
                .where(new StreamKeySelector()).equalTo(new StreamKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new AverageProductivity());

        outStream.print();
        env.execute("Cogroup Stream Demo");
    }

    private static class StreamKeySelector implements KeySelector<Tuple2<String, Double>, String> {
        @Override
        public String getKey(Tuple2<String, Double> input) throws Exception {
            return input.f0;
        }
    }

    public static class RowParser implements MapFunction<String, Tuple2<String, Double>>{
        @Override
        public Tuple2<String, Double> map(String s) throws Exception {
            String[] input = s.split("\\s+");
            return Tuple2.of(input[0].trim(), Double.parseDouble(input[1].trim()));
        }
    }

    private static class AverageProductivity implements CoGroupFunction<
            Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>>{

        @Override
        public void coGroup(Iterable<Tuple2<String, Double>> iterable, Iterable<Tuple2<String, Double>> iterable1, Collector<Tuple2<String, Double>> collector) throws Exception {
            double hours = 0.0, performance = 0.0;
            String key = null;
            for(Tuple2<String, Double> hour : iterable1) {
                if(key==null) {
                    key = hour.f0;
                }

                hours = hours + hour.f1;
            }

            for(Tuple2<String, Double> perf : iterable){
                performance = performance + perf.f1;
            }

            collector.collect(Tuple2.of(key, performance/hours));
        }
    }
}