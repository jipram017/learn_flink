import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        if(dataStream == null){
            System.exit(1);
            return;
        }

        DataStream<Tuple3<String, String, Double>> stream1 = dataStream.map(new SumNumbers());
        DataStream<Tuple3<String, String, Double>> stream2 = dataStream.map(new MultiplyNumbers());

        DataStream<Tuple3<String, String, Double>> unionStream = stream1.union(stream2);

        unionStream.print();
        env.execute("Union Streams Demo");
    }

    public static class SumNumbers implements MapFunction<String, Tuple3<String, String, Double>> {

        @Override
        public Tuple3<String, String, Double> map(String input) throws Exception {
            String[] nums = input.split("\\s+");
            Double sum = 0.00;
            for(String num : nums){
                sum = sum + Double.parseDouble(num);
            }

            return Tuple3.of(input, "Sum", sum);
        }
    }

    public static class MultiplyNumbers implements MapFunction<String, Tuple3<String, String, Double>> {

        @Override
        public Tuple3<String, String, Double> map(String input) throws Exception {
            String[] nums = input.split("\\s+");
            Double product = 1.00;
            for(String num : nums){
                product = product * Double.parseDouble(num);
            }

            return Tuple3.of(input, "Product", product);
        }
    }
}