import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import java.util.Map;

public class SparkCovid19_2 {
    public static void main(String[] args) {
        if(args.length < 3){
            System.out.println("more arguments needed!");
            System.out.println(args.length);
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");
        // create spark context
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //read data from a input path
        JavaRDD<String> line = jsc.textFile(args[0]);
        JavaRDD<String> populations = jsc.textFile(args[1]);

        // filter population
        JavaRDD<String> population_filter = populations.filter(x->x.split(",").length == 5);
        JavaPairRDD<String,String> populations_map = population_filter.mapToPair(x->
                new Tuple2<String,String>(x.split(",")[1],x.split(",")[x.split(",").length-1])
        );

        JavaPairRDD<String,String> populations_filter = populations_map.filter(x->!x._1.equals("location") && !x._2.equals("Europe"));

        JavaPairRDD<String, Double> populations_result = populations_filter.mapToPair(x->
                new Tuple2<String, Double>(x._1,Double.parseDouble(x._2)));

        // broadcast variable for frequent use data set
        Broadcast<Map<String,Double>> broadcast_populations = jsc.broadcast(populations_result.collectAsMap());

        //combine word together
        JavaRDD<String> lines = line.filter(x->! x.startsWith("date"));
        JavaPairRDD<String, String> wordAndOne = lines.mapToPair(x->
                new Tuple2<String, String>(x.split(",")[1],x.split(",")[2])
        );

        //transfer
        JavaPairRDD<String, Double> map = wordAndOne.mapToPair(x->
                new Tuple2<String,Double>(x._1,Double.parseDouble(x._2)));

        //put them together
        JavaPairRDD<String, Double> reduce = map.reduceByKey((Function2<Double, Double, Double>) Double::sum);

        //divide population
        JavaPairRDD<String, Double> result = reduce.mapToPair(x->{
                Map<String, Double> m = broadcast_populations.getValue();
                if(m.containsKey(x._1)){
                    return new Tuple2<String,Double>(x._1,1000000 * x._2 / m.get(x._1));
                }
                else{
                    return new Tuple2<String,Double>(x._1,-1.0);
                }
            }
        );

        //save data to hdfs
        result.sortByKey().saveAsTextFile(args[2]);

        //release source
        jsc.stop();
    }
}