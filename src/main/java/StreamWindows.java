

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWindows {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);

        // env.getConfig().setGlobalJobParameters(params);

        // Use Flink processing time as event timestamp
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        // Run in local
        env.setParallelism(1);

        // Enable fault-tolerance, 60s checkpointing
        env.enableCheckpointing(60000);

        // Enable restart
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 500L));

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);
        if (dataStream == null && Integer.parseInt(params.get("opt")) != 7 && Integer.parseInt(params.get("opt")) != 6){
            System.exit(1);
            return;
        }

        switch(Integer.parseInt(params.get("opt"))) {
            case 1: {
                System.out.println("Running a tumbling window operation");
                TumblingWindow tumblingWindow = new TumblingWindow();
                tumblingWindow.tumblingWindow(dataStream);
                env.execute("Flink Tumbling Window Demo Application");
                break;
            }

           case 2: {
                System.out.println("Running a sliding window operation");
                SlidingWindow slidingWindow = new SlidingWindow();
                slidingWindow.slidingWindow(dataStream);
               env.execute("Flink Sliding Window Demo Application");
                break;
            }

            case 3: {
                System.out.println("Running a count window operation");
                CountWindow countWindow = new CountWindow();
                countWindow.countWindow(dataStream);
                env.execute("Flink Count Window Demo Application");
                break;
            }

            case 4: {
                System.out.println("Running a session window operation");
                SessionWindow sessionWindow = new SessionWindow();
                sessionWindow.sessionWindow(dataStream);
                env.execute("Flink Tumbling Session Demo Application");
                break;
            }

            case 5: {
                System.out.println("Running an evaluated count window operation");
                CountWindowEvaluate countWindow = new CountWindowEvaluate();
                countWindow.countWindowEvaluate(dataStream);
                env.execute("Flink Evaluated Count Window Demo Application");
                break;
            }

            case 6: {
                System.out.println("Running a Flink-Twitter demo application");
                TwitterLocationCount twitterLocationCount = new TwitterLocationCount();
                twitterLocationCount.twitterLocationCount(env, params);
                env.execute("Flink Twitter Trending Demo Application");
                break;
            }

            case 7: {
                System.out.println("Running a Flink-Twitter trending demo application");
                TwitterTrending twitterTrending = new TwitterTrending();
                twitterTrending.twitterTrending(env, params);
                env.execute("Flink Twitter Trending Demo Application");
                break;
            }

            default: {
                System.out.println("No such window operation available for this number");
                break;
            }
        }

    }
}
