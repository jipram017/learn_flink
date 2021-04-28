import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IteratorStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> dataStream = env.fromElements(32,17,30,27,19,18,16,24,40,41);

        IterativeStream<Tuple2<Integer, Integer>> iterativeStream = dataStream
                .map(new AddIterCount()).iterate();

        DataStream<Tuple2<Integer, Integer>> checkMultiple4Stream = iterativeStream
                .map(new checkMultiple4());

        DataStream<Tuple2<Integer, Integer>> feedback = checkMultiple4Stream
                .filter(new FilterFunction<Tuple2<Integer, Integer>>() {
            @Override
            public boolean filter(Tuple2<Integer, Integer> input) throws Exception {
                return input.f0 % 4 != 0;
            }
        });

        iterativeStream.closeWith(feedback);
        DataStream<Tuple2<Integer, Integer>> outStream = iterativeStream
                .filter(new FilterFunction<Tuple2<Integer, Integer>>() {
            @Override
            public boolean filter(Tuple2<Integer, Integer> input) throws Exception {
                return input.f0 % 4 == 0;
            }
        });

        outStream.print();
        env.execute("Iterative stream demo");
    }

    private static class AddIterCount implements MapFunction<Integer, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
            return Tuple2.of(integer, 0);
        }
    }

    private static class checkMultiple4 implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>{
        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> input) throws Exception {
            Tuple2<Integer, Integer> output = null;
            if(input.f0 % 4 == 0){
                output = input;
            } else{
                output = Tuple2.of(input.f0 - 1, input.f1 + 1);
            }

            return output;
        }
    }
}