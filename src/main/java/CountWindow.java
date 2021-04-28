
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class CountWindow {
    public static void countWindow(DataStream<String> dataStream) {
        DataStream<WordCount> outStream = dataStream
                .map(new RowParser())
                .keyBy("word")
                .countWindow(5)
                .sum("count");

        outStream.print();
    }

    public static class RowParser implements MapFunction<String, WordCount> {
        public WordCount map(String input) throws Exception {
            try {
                String[] fields = input.split("\\s+");
                return new WordCount(
                        fields[0].trim(),
                        1
                );
            } catch (Exception e){
                e.getStackTrace();
            }
            return null;
        }
    }

    public static class WordCount {
        public String word;
        public Integer count;
        public WordCount(){
        }

        public WordCount(String word, Integer count){
            this.word = word;
            this.count= count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
