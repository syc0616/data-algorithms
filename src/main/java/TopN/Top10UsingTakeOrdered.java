package TopN;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class Top10UsingTakeOrdered {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Top10UsingTakeOrdered");
        sparkConf.setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        String inputPath = "I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap03\\scala\\uniqeue\\*";
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);
        JavaPairRDD<String, Integer> kvRDD = lines.mapToPair(f -> {
            String[] tokens = f.split(",");
            return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
        });

        JavaPairRDD<String, Integer> uniqueKeys = kvRDD.reduceByKey((x, y) -> {
            return x + y;
        });

        List<Tuple2<String, Integer>> topNResult = uniqueKeys.takeOrdered(5,MyTupleComparator.INSTANCE);

        for (Tuple2<String, Integer> entry : topNResult) {
            System.out.println(entry._2 + "--" + entry._1);
        }

    }

    static class MyTupleComparator implements Comparator<Tuple2<String,Integer>> , Serializable{
        final static MyTupleComparator INSTANCE = new MyTupleComparator();

        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return -o1._2.compareTo(o2._2);// sorts RDD elements descending (use for Top-N)
            // return t1._2.compareTo(t2._2);   // sorts RDD elements ascending (use for Bottom-N)
        }
    }
}
