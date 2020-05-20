import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;
import scala.Tuple3;
import java.io.IOException;
import java.text.*;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class SparkCovid19_1 {
    public static void main(String[] args) {
        if(args.length < 4){
            System.out.println("more arguments needed!");
            System.exit(1);
        }
        String start_date = args[1].trim();
        String end_date = args[2].trim();
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
        // create spark context
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //read data from a input path
        JavaRDD<String> line = jsc.textFile(args[0]);
        //combine word together
        JavaRDD<String> lines = line.filter(x->! x.startsWith("date"));
        JavaPairRDD<String, String> wordAndOne = lines.mapToPair(x->
                new Tuple2<String, String>(x.split(",")[1] + "," + x.split(",")[0],x.split(",")[3])
        );
        // filter
        JavaPairRDD<String, String> date_in_between = wordAndOne.filter(x->
            data_compare(x._1.split(",")[1].trim(), start_date) <= 0
                    && data_compare(x._1.split(",")[1].trim(), end_date) >= 0

        );
        //transfer
        JavaPairRDD<String, Integer> map = date_in_between.mapToPair(x->
                new Tuple2<String,Integer>(x._1.split(",")[0],Integer.parseInt(x._2)));

        //put them together
        JavaPairRDD<String, Integer> reduce = map.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //sort them
        JavaPairRDD<String, Integer> result = reduce.sortByKey(true);

        //save data to hdfs
        result.saveAsTextFile(args[3]);
        //release source
        jsc.stop();
    }

    // -1: date1 after date2   1: date1 before date2  0: date1 is same with date2
    public static int data_compare(String date1, String date2) throws ParseException {
        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = sdformat.parse(date1);
        Date d2 = sdformat.parse(date2);
        if(d1.compareTo(d2) > 0) {
            return -1;
        } else if(d1.compareTo(d2) < 0) {
            return 1;
        } else{
            return 0;
        }
    }
}