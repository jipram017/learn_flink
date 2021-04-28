
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class WordCountWindow {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);

        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);

        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                2, // number of restart attempts
                2000 // time in milliseconds between restarts
        ));

        DataStream<String> dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
        if (dataStream == null){
            System.exit(1);
            return;
        }

        DataStream<String> outStream2 = dataStream
                .map(new wordToTuple())
                .keyBy(0)
                .flatMap(new collectDistinctWords());

        outStream2.print();
        env.execute("flink state demo");
    }

    public static class wordToTuple implements MapFunction<String, Tuple2<Integer, String>>{
        @Override
        public Tuple2<Integer, String> map(String s) throws Exception {
            return Tuple2.of(1, s.trim());
        }
    }
    public static class collectWordCount extends RichFlatMapFunction<Tuple2<Integer, String>, String> {

        private transient ValueState<Map<String, Integer>> allWordsCount;

        @Override
        public void flatMap(Tuple2<Integer, String> input, Collector<String> out) throws Exception {
            Map<String, Integer> currentWordCounts = allWordsCount.value();
            if(input.f1.equals("print")){
                out.collect(currentWordCounts.toString());
                allWordsCount.clear();
            }
            else {
                if(!currentWordCounts.containsKey(input.f1)){
                    currentWordCounts.put(input.f1, 1);
                }
                else{
                    int wordCount = currentWordCounts.get(input.f1);
                    currentWordCounts.put(input.f1, 1 + wordCount);
                }
                allWordsCount.update(currentWordCounts);
            }
        }

        @Override
        public void open(Configuration config){
            ValueStateDescriptor<Map<String, Integer>> descriptor = new ValueStateDescriptor<Map<String, Integer>>(
                    "allWordsCount",
                    TypeInformation.of(new TypeHint<Map<String, Integer>>(){}),
                    new HashMap<String, Integer>()
            );
            allWordsCount = getRuntimeContext().getState(descriptor);
        }
    }

    public static class collectDistinctWords extends RichFlatMapFunction<Tuple2<Integer, String>, String> {

        ListState<String> allDistinctWords;
        @Override
        public void flatMap(Tuple2<Integer, String> input, Collector<String> out) throws Exception {
            Iterable<String> currentDistinctWords = allDistinctWords.get();
            boolean exist = false;

            if(input.f1.equals("print")){
                out.collect(currentDistinctWords.toString());
                allDistinctWords.clear();
            }
            else {
                for(String word : currentDistinctWords){
                    if(word.equals(input.f1)){
                        exist =true;
                        break;
                    }
                }

                if(!exist){
                    allDistinctWords.add(input.f1);
                }
            }
        }

        @Override
        public void open(Configuration config){
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                    "all distinct words",
                    String.class);
            allDistinctWords = getRuntimeContext().getListState(descriptor);
        }
    }

    public static class collectTotalWordsCount extends RichFlatMapFunction<Tuple2<Integer, String>, String>{
        private transient ReducingState<Integer> totalCountState;

        @Override
        public void flatMap(Tuple2<Integer, String> input, Collector<String> out) throws Exception {
            if(input.f1.equals("print")){
                out.collect(totalCountState.get().toString());
                totalCountState.clear();
            }
            else{
                totalCountState.add(1);
            }
        }

        @Override
        public void open(Configuration config){
            ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<Integer>(
                    "total words count",
                    new ReduceFunction<Integer>() {
                        @Override
                        public Integer reduce(Integer integer, Integer t1) throws Exception {
                            return integer + t1;
                        }
                    },
                    Integer.class
            );

            totalCountState = getRuntimeContext().getReducingState(descriptor);
        }
    }
}
