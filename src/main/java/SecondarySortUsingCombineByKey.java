import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

public class SecondarySortUsingCombineByKey {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SecondarySortUsingCombineByKey <input> <output>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("inputPath=" + inputPath);
        String outputPath = args[1];
        System.out.println("outputPath=" + outputPath);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("secondsort");
        sparkConf.setMaster("local");
        final JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(
           f->{
             String[] tokens = f.split(",");
             Tuple2<Integer, Integer> timevalue = new Tuple2<Integer, Integer>(Integer.parseInt(tokens[1]),
               Integer.parseInt(tokens[2]));
             return new Tuple2<String, Tuple2<Integer,Integer>>(tokens[0], timevalue);
        });

        List<Tuple2<String,Tuple2<Integer, Integer>>> output = pairs.collect();

        for(Tuple2 t : output){
            Tuple2<Integer, Integer> timevalue = (Tuple2<Integer,Integer>) t._2;
            System.out.println(t._1 + "," + timevalue._1 + "," + timevalue._1);
        }

        Function<Tuple2<Integer, Integer>, SortedMap<Integer,Integer>> createCombiner
                = (Tuple2<Integer,Integer> x) -> {
            Integer time = x._1;
            Integer value = x._2;
            SortedMap<Integer,Integer> map = new TreeMap<>();
            map.put(time, value);
            return map;
        };



    }
}
