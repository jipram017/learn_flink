import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PageRankData {
    private static final int numPages = 5;
    private static final int maxIterations = 10;
    public static final Object[][] EDGES = {
            {1L, 2L},
            {1L, 15L},
            {2L, 3L},
            {2L, 4L},
            {2L, 5L},
            {2L, 6L},
            {2L, 7L},
            {3L, 13L},
            {4L, 2L},
            {5L, 11L},
            {5L, 12L},
            {6L, 1L},
            {6L, 7L},
            {6L, 8L},
            {7L, 1L},
            {7L, 8L},
            {8L, 1L},
            {8L, 9L},
            {8L, 10L},
            {9L, 14L},
            {9L, 1L},
            {10L, 1L},
            {10L, 13L},
            {11L, 12L},
            {11L, 1L},
            {12L, 1L},
            {13L, 14L},
            {14L, 12L},
            {15L, 1L},
    };

    public static final DataSet<Long> getDefaultPagesDataSet(ExecutionEnvironment env){
       return env.generateSequence(1,15);
    }

    public static DataSet<Tuple2<Long, Long>> getDefaultLinksDataSet(ExecutionEnvironment env){
        List<Tuple2<Long, Long>> collection = new ArrayList<Tuple2<Long, Long>>();
        for (Object[] e : EDGES){
            collection.add(new Tuple2<Long, Long>((Long) e[0], (Long) e[1]));
        }
        return env.fromCollection(collection);
    }

    public static int getNumberOfPages(){
        return numPages;
    }

    public static int getNumberOfIterations(){
        return maxIterations;
    }
}