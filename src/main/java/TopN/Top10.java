package TopN;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Top10 {
    public static void main(String[] args) {
        /*if (args.length < 1){
            System.err.println("Usage: Top10 <input> <output>");
            System.exit(1);
        }
        */
        String inputPath = "I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap03\\scala\\sample_input\\*";
        System.out.println("args[0]: <input-path>=" + inputPath);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("topN");
        sparkConf.setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile(inputPath, 1);
        //num, cat name, weight
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(f -> {
            String[] tokens = f.split(",");
            return new Tuple2<String, Integer>(tokens[1], Integer.parseInt(tokens[2]));
        });

        //List<Tuple2<String, Integer>> collect = pairs.collect();
        //对每个分区求top10
        JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions((Iterator<Tuple2<String, Integer>> iter) -> {
            SortedMap<Integer, String> top10 = new TreeMap<>();
            while (iter.hasNext()) {
                Tuple2<String, Integer> next = iter.next();
                top10.put(next._2, next._1);
                if (top10.size() > 10) {
                    top10.remove(top10.firstKey());
                }
            }
            //将Map转换成一个list，并返回iterator
            return Collections.singletonList(top10).iterator();
        });
        // 将分区的所有top10汇总到本地
        /*List<SortedMap<Integer, String>> alltop10 = partitions.collect();
        for(SortedMap<Integer, String> localtop10 : alltop10){
            for (Map.Entry<Integer, String> entry : localtop10.entrySet() ){
                System.out.println(entry.getKey() +"---" + entry.getValue());
            }
        }*/
        SortedMap<Integer, String> finaltop10 = partitions.reduce((x, y) -> {
            SortedMap<Integer, String> top10 = new TreeMap<>();

            for (Map.Entry<Integer, String> entry : x.entrySet()) {
                top10.put(entry.getKey(), entry.getValue());
                if (top10.size() > 10) {
                    top10.remove(top10.firstKey());
                }
            }

            for (Map.Entry<Integer, String> entry : y.entrySet()) {
                top10.put(entry.getKey(), entry.getValue());
                if (top10.size() > 10) {
                    top10.remove(top10.firstKey());
                }
            }
            return top10;
        });

        for(Map.Entry<Integer,String> entry : finaltop10.entrySet()){
            System.out.println(entry.getKey() + "---" + entry.getValue());
        }


    }
}
