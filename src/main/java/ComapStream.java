import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ComapStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool param = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(param);

        DataStream<String> stream1 = env.socketTextStream("localhost", 8000);
        DataStream<String> stream2 = env.socketTextStream("localhost", 9000);

        if(stream1 == null || stream2 == null) {
            System.exit(1);
            return;
        }

        DataStream<Tuple2<String, Double>> outStream = stream1.connect(stream2).map(new RowParser());

        outStream.print();
        env.execute("Comap Stream Demo");
    }

    public static class RowParser implements CoMapFunction<String, String, Tuple2<String, Double>>{

        @Override
        public Tuple2<String, Double> map1(String s) throws Exception {
            String[] inp1 = s.split("\\s+");
            return Tuple2.of(inp1[0].trim(), Double.parseDouble(inp1[1].trim()));
        }

        @Override
        public Tuple2<String, Double> map2(String s) throws Exception {
            String[] inp2 = s.split("\\s+");
            return Tuple2.of(inp2[0].trim(), Double.parseDouble(inp2[1].trim()));
        }
    }
}