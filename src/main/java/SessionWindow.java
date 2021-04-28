

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SessionWindow {
    public static void sessionWindow(DataStream<String> dataStream){
        DataStream<Tuple3<String, String, Double>> userClickStream = dataStream.map(text -> {
            String[] fields = text.split("\\s+");
            if (fields.length == 3){
                return new Tuple3<String, String, Double>(
                        fields[0].trim(),
                        fields[1].trim(),
                        Double.parseDouble(fields[2])
                );
            }
            throw new Exception("Not valid args passed");
        }, TypeInformation.of(new TypeHint<Tuple3<String, String, Double>>() {
        }));

        DataStream<Tuple3<String, String, Double>> maxPageVisitTime = userClickStream
                .keyBy(((KeySelector<Tuple3<String, String, Double>, Tuple2<String, String>>)
                                str -> new Tuple2<String, String>(str.f0, str.f1)),
                        TypeInformation.of(new TypeHint<Tuple2<String, String>>(){}))
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .max(2);

        maxPageVisitTime.print();
    }
}
