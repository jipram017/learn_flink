import org.apache.flink.api.java.utils.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;

public class StreamUtil {
    public static DataStream<String> getDataStream(StreamExecutionEnvironment env, final ParameterTool params){
        DataStream<String> dataStream = null;

        if(!params.has("opt")){
            System.out.println("Use --opt to specify window operation");
            return null;
        }

        if (Integer.parseInt(params.get("opt")) != 6 && Integer.parseInt(params.get("opt")) != 7) {
            if(params.has("input")){
                dataStream = env.readTextFile(params.get("input"));
            }
            else if(params.has("host") && params.has("port")){
                dataStream = env.socketTextStream(params.get("host"), Integer.parseInt(params.get("port")));
            }
            else {
                System.out.println("Use --host and --port to specify socket input");
                System.out.println("Use --input to specify file input");
                return null;
            }
        }

        else {
            System.out.println("Executing twitter demo app");
        }

        return dataStream;
    }
}
