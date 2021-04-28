
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class CountWindowEvaluate {
    public static void countWindowEvaluate(DataStream<String> dataStream){
        WindowedStream<NewAccount, Tuple, GlobalWindow> windowedStream = dataStream
                .map(new RowParser())
                .keyBy("staticKey")
                .countWindow(10);

        DataStream<Map<String, Integer>> outStream = windowedStream.apply(new collectUS());
        outStream.print();
    }

    public static class RowParser implements MapFunction<String,NewAccount> {
        public NewAccount map(String input) throws Exception {
            try {
                String[] fields = input.split("\\s+");
                return new NewAccount(
                        fields[0].trim(),
                        fields[1].trim(),
                        1
                );
            } catch (Exception e){
                e.getStackTrace();
            }
            return null;
        }
    }

    public static class collectUS implements WindowFunction<NewAccount, Map<String, Integer>,Tuple,GlobalWindow>{
        public void apply(Tuple tuple,
                          GlobalWindow globalWindow,
                          Iterable<NewAccount> iterable,
                          Collector<Map<String, Integer>> collector){

            Map<String, Integer> output = new HashMap<>();

            for(NewAccount signUp: iterable){
                if(signUp.country.equals("US")){
                    if(!output.containsKey(signUp.course)){
                        output.put(signUp.course, 1);
                    }
                    else{
                        output.put(signUp.course, output.get(signUp.course) + 1);
                    }
                }
            }

            collector.collect(output);
        }
    }

    public static class NewAccount{
        public Integer staticKey = 1;
        public String course;
        public String country;
        public Integer count;

        public NewAccount(){}
        public NewAccount(String course, String country, Integer count){
            this.count = count;
            this.course = course;
            this.country = country;
        }

        @Override
        public String toString() {
            return "NewAccount{" +
                    "staticKey=" + staticKey +
                    ", course='" + course + '\'' +
                    ", country='" + country + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
