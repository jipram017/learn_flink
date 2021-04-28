import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class JoinStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool param = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(param);

        DataStream<String> dataStream = env.socketTextStream(param.get("host"), Integer.parseInt(param.get("port")));
        if(dataStream == null){
            System.exit(1);
            return;
        }

        DataStream<Tuple3<String, String, Double>> stream1 = dataStream.map(new SumNumbers());
        DataStream<Tuple3<String, String, Double>> stream2 = dataStream.map(new MultiplyNumbers());

        DataStream<Tuple3<String, Double, Double>> joinedStream = stream1.join(stream2)
                .where(new StreamKeySelector()).equalTo(new StreamKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new SumProductJoinFunction());

        joinedStream.print();
        env.execute("join stream");
    }

    public static class StreamKeySelector implements KeySelector<Tuple3<String, String, Double>, String>{

        @Override
        public String getKey(Tuple3<String, String, Double> value) throws Exception {
            return value.f0;
        }
    }

    public static class SumNumbers implements MapFunction<String, Tuple3<String,String,Double>>{

        @Override
        public Tuple3<String, String, Double> map(String s) throws Exception {
            Double sum = 0.00;
            String[] nums = s.split("\\s+");
            for(String num : nums){
                sum = sum + Double.parseDouble(num);
            }

            return new Tuple3<String, String, Double>(s, "Sum", sum);
        }
    }

    public static class MultiplyNumbers implements MapFunction<String, Tuple3<String, String, Double>>{

        @Override
        public Tuple3<String, String, Double> map(String s) throws Exception {
            Double product = 1.00;
            String[] nums = s.split("\\s+");
            for(String num: nums){
                product = product * Double.parseDouble(num);
            }

            return new Tuple3<String, String, Double>(s, "Product", product);
        }
    }

    public static class SumProductJoinFunction implements JoinFunction
                    <Tuple3<String, String, Double>,
                    Tuple3<String, String, Double>,
                    Tuple3<String, Double, Double>>{

        @Override
        public Tuple3<String, Double, Double> join(Tuple3<String, String, Double> s1,
                                                   Tuple3<String, String, Double> s2) throws Exception {
            return new Tuple3<String, Double, Double>(s1.f0, s1.f2, s2.f2);
        }
    }
}