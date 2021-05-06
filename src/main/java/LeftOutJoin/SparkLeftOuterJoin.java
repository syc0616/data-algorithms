package LeftOutJoin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.*;

public class SparkLeftOuterJoin {
    public static void main(String[] args) {
        String usersInputFile = "I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap04\\scala\\users.tsv";
        String transactionsInputFile = "I:\\work\\code\\spark-anylysis\\main\\scala\\org\\dataalgorithms\\chap04\\scala\\transactions.tsv";
        System.out.println("users="+ usersInputFile);
        System.out.println("transactions="+ transactionsInputFile);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("leftjoin");
        sparkConf.setMaster("local");

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> users = ctx.textFile(usersInputFile, 1);
        JavaPairRDD<String, Tuple2<String, String>> userRDD = users.mapToPair(f -> {
            String[] userRecord = f.split("\t");
            Tuple2<String, String> location = new Tuple2<>("L",
                    userRecord[1]);
            return new Tuple2<String, Tuple2<String, String>>(userRecord[0], location);
        });

        JavaRDD<String> transactionsRDD = ctx.textFile(transactionsInputFile, 1);
        JavaPairRDD<String, Tuple2<String,String>> transRDD = transactionsRDD.mapToPair(f->{
           String[] transactionRecord = f.split("\t");
           Tuple2<String, String> product = new Tuple2<>("P", transactionRecord[1]);
           return new Tuple2<String,Tuple2<String,String>>(transactionRecord[2],product);
        });

        JavaPairRDD<String, Tuple2<String, String>> allRDD = transRDD.union(userRDD);
        // group allRDD by userID
        JavaPairRDD<String, Iterable<Tuple2<String,String>>> groupedRDD = allRDD.groupByKey();

        // <location, product>
        JavaPairRDD<String, String> productLocationsRDD = groupedRDD.flatMapToPair(f -> {
            //location or product
            Iterable<Tuple2<String, String>> pairs = f._2;
            List<String> products = new ArrayList<String>();

            String location = "unknown";
            for (Tuple2<String, String> t2 : pairs) {
                if (t2._1.equalsIgnoreCase("L")) {
                    location = t2._2;
                } else {
                    // t2._1.equals("P")
                    products.add(t2._2);
                }
            }

            List<Tuple2<String, String>> kvList = new ArrayList<>();
            for (String product : products) {
                kvList.add(new Tuple2<String, String>(product, location));
            }

            return kvList.iterator();
        });

        // group by location
        JavaPairRDD<String, Iterable<String>> productByLocations = productLocationsRDD.groupByKey();
        // uniqueLocation - unique location
        /*JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniqueLocations = productByLocations.mapValues(f -> {
            Iterable<String> itr = f;
            Set<String> uniqueLocations = new HashSet<String>();
            for (String location : itr) {
                uniqueLocations.add(location);
            }
            return new Tuple2<Set<String>, Integer>(uniqueLocations, uniqueLocations.size());
        });*/

        
        Function<String,Set<String>> createCombiner = new Function<String,Set<String>>(){
            @Override
            public Set<String> call(String s) throws Exception {
                Set<String> set = new HashSet<>();
                set.add(s);
                return set;
            }
        };

        Function2<Set<String>, String, Set<String>> mergeValue = new Function2<Set<String>, String, Set<String>>() {
            @Override
            public Set<String> call(Set<String> set, String s) throws Exception {
                set.add(s);
                return set;
            }
        };

        Function2<Set<String>, Set<String>, Set<String>> mergeCombiners =
                new Function2<Set<String>, Set<String>, Set<String>>() {
            @Override
            public Set<String> call(Set<String> a, Set<String> b) throws Exception {
                a.addAll(b);
                return a;
            }
        };


        System.out.println("--- debug4 begin ---");
        JavaPairRDD<String, Set<String>> productByUniqueLocations = productLocationsRDD.combineByKey(createCombiner, mergeValue, mergeCombiners);
        Map<String, Set<String>> debug4 = productByUniqueLocations.collectAsMap();
        for(Map.Entry<String, Set<String>> entry : debug4.entrySet()) {
            System.out.println(entry.getKey() + ", " + entry.getValue());
        }

        /*
        List<Tuple2<String, Tuple2<Set<String>, Integer>>>  debug4 = productByUniqueLocations.collect();

        for (Tuple2<String, Tuple2<Set<String>, Integer>> t2 : debug4) {
            System.out.println("debug4 t2._1="+t2._1);
            System.out.println("debug4 t2._2="+t2._2);
        }*/
    }
}
