package TopN;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Top10NonUnique {
    public static void main(String[] args) {
        String inputPath = "I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap03\\scala\\uniqeue\\*";
        
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("topunique");
        sparkConf.setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile(inputPath).coalesce(2);
        JavaPairRDD<String, Integer> kvRDD = lines.mapToPair(f -> {
            String[] tokens = f.split(",");
            return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
        });

        JavaPairRDD<String, Integer> uniqueKeys = kvRDD.reduceByKey((x, y) -> {
            return x + y;
        });

        JavaRDD<SortedMap<Integer, String>> partitions = uniqueKeys.mapPartitions((Iterator<Tuple2<String, Integer>> iter) -> {
            SortedMap<Integer, String> localTopN = new TreeMap<>();

            while (iter.hasNext()) {
                Tuple2<String, Integer> tuple2 = iter.next();
                localTopN.put(tuple2._2, tuple2._1);
                //keep only top N
                if (localTopN.size() > 10) {
                    localTopN.remove(localTopN.firstKey());
                }
            }
            return Collections.singletonList(localTopN).iterator();
        });

        //对所有分区数据进行聚合
        SortedMap<Integer, String> reduceMap = partitions.reduce((x, y) -> {
            SortedMap<Integer, String> localTopN = new TreeMap<>();
            for (Map.Entry<Integer, String> top10 : x.entrySet()) {
                localTopN.put(top10.getKey(), top10.getValue());
                if (localTopN.size() > 10) {
                    localTopN.remove(localTopN.firstKey());
                }
            }

            for (Map.Entry<Integer, String> top10 : y.entrySet()) {
                localTopN.put(top10.getKey(), top10.getValue());
                if (localTopN.size() > 10) {
                    localTopN.remove(localTopN.firstKey());
                }
            }

            return localTopN;
        });

        // STEP-10: emit final top-N
        for (Map.Entry<Integer, String> entry : reduceMap.entrySet()) {
            System.out.println(entry.getKey() + "--" + entry.getValue());
        }

        ctx.close();
        ctx.stop();
    }
}
