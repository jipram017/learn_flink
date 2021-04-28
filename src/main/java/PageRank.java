import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class PageRank {
    private static final double DAMPENING_FACTOR = 0.85;
    private static final double EPSILON = 0.0001;

    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool param = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(param);

        final int max_iterations = param.getInt("iterations", PageRankData.getNumberOfIterations());
        final int num_pages = param.getInt("numPages", PageRankData.getNumberOfPages());

        // Get input data
        DataSet<Long> pagesInput = getPagesDataSet(env, param);
        DataSet<Tuple2<Long, Long>> linksInput = getLinksDataSet(env, param);

        // Assign initial rank to pages
        DataSet<Tuple2<Long, Double>> pagesWithRank = pagesInput.map(new RankAssigner(1.0d/num_pages));

        // Build adjacency list from link input
        DataSet<Tuple2<Long, Long[]>> adjListInput = linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

        // Set iterative data set
        IterativeDataSet<Tuple2<Long, Double>> iterativeDataSet = pagesWithRank.iterate(max_iterations);

        DataSet<Tuple2<Long, Double>> newRanks = iterativeDataSet.join(adjListInput).where(0).equalTo(0)
                .flatMap(new JoinVertexWithEdgesMatch())
                // Collect and sum rank
                .groupBy(0)
                .aggregate(Aggregations.SUM, 1)
                // Apply dampening factor
                .map(new Dampener(DAMPENING_FACTOR, num_pages));

        DataSet<Tuple2<Long, Double>> finalRanks = iterativeDataSet.closeWith(
                newRanks,
                newRanks.join(iterativeDataSet).where(0).equalTo(0).filter(new EpsilonFilter()));

        // Emit result
        if (param.has("output")) {
            finalRanks.writeAsCsv(param.get("output"), "\n", "");
            env.execute("Basic rank example");
        }
        else{
            System.out.println("Printing final output to stdout. Use --output to specify output path");
            finalRanks.print();
        }

    }

    /* ========================================== */
    /* USER FUNCTION                              */
    /* ========================================== */

    private static final class RankAssigner implements MapFunction<Long, Tuple2<Long, Double>> {
        Tuple2<Long, Double> outPageWithRank;
        public RankAssigner(double rank){
            this.outPageWithRank = new Tuple2<Long, Double>(-1L, rank);
        }

        @Override
        public Tuple2<Long, Double> map(Long aLong) throws Exception {
            outPageWithRank.f0 = aLong;
            return outPageWithRank;
        }
    }

    private static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>>{
        private final ArrayList<Long> neighbors = new ArrayList<Long>();
        @Override
        public void reduce(Iterable<Tuple2<Long, Long>> iterable, Collector<Tuple2<Long, Long[]>> collector) throws Exception {
            neighbors.clear();
            Long id = 0L;

            for(Tuple2<Long,Long> iter : iterable){
                id = iter.f0;
                neighbors.add(iter.f1);
            }

            collector.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
        }
    }

    private static final class Dampener implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>{
        private final double dampening;
        private final double randomJump;

        public Dampener(double dampening, double numVertices){
            this.dampening = dampening;
            this.randomJump = (1-dampening)/numVertices;
        }

        @Override
        public Tuple2<Long, Double> map(Tuple2<Long, Double> input) throws Exception {
            input.f1 = (input.f1 * dampening) + randomJump;
            return input;
        }
    }

    private static final class JoinVertexWithEdgesMatch implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {
        @Override
        public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> input, Collector<Tuple2<Long, Double>> collector) throws Exception {
            Long[] neighbors = input.f1.f1;
            double rank = input.f0.f1;
            double rankToDistribute = rank / ((double) neighbors.length);

            for(Long neighbor : neighbors){
                collector.collect(new Tuple2<Long, Double>(neighbor, rankToDistribute));
            }
        }
    }

    private static class EpsilonFilter implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {
        @Override
        public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> input) throws Exception {
            return Math.abs(input.f0.f1 - input.f1.f1) > EPSILON;
        }
    }

    /* ========================================== */
    /* UTIL METHODS                                /
    /* ========================================== */

    private static DataSet<Long> getPagesDataSet(ExecutionEnvironment env, ParameterTool param) {
        if(param.has("pages")) {
            return env.readCsvFile(param.get("pages"))
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Long.class)
                    .map(new MapFunction<Tuple1<Long>, Long>() {
                        @Override
                        public Long map(Tuple1<Long> input) throws Exception {
                            return input.f0;
                        }
                    });
        }
        else{
            System.out.println("Executing pageRank with default page data set");
            System.out.println("Please use --pages to specify file input");
            return PageRankData.getDefaultPagesDataSet(env);
        }
    }

    private static DataSet<Tuple2<Long, Long>> getLinksDataSet(ExecutionEnvironment env, ParameterTool param){
        if (param.has("links")){
            return env.readCsvFile(param.get("links"))
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Long.class, Long.class);
        }
        else{
            System.out.println("Executing pageRank with default link data set");
            System.out.println("Please use --links to specify file input");
            return PageRankData.getDefaultLinksDataSet(env);
        }
    }
}