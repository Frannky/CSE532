import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

public class Covid19_3 {
    public static class CountMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            String line = value.toString().trim();
            String[] words = line.split(",");
            double case_number;
            if(words[2].trim().equals("new_cases")) return;
            else {
                case_number = Double.parseDouble(words[2].trim());
            }
            String country_name = words[1].trim();
            context.write(new Text(country_name), new DoubleWritable(case_number));
        }
    }
    public static class CountReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException
        {
            Map<String, Double> map = new HashMap<String, Double>();
            URI[] files = context.getCacheFiles();
            // read population.csv file data
            if (files != null && files.length > 0){
                String line = "";
                String path = files[0].toString().trim();
                int lastindex = path.lastIndexOf('/');
                BufferedReader population = new BufferedReader(new FileReader(path.substring(lastindex+1, path.length())));
                while ((line = population.readLine()) != null) {
                    if (line.startsWith("countriesAndTerritories")) {
                        continue;
                    }
                    if(line.startsWith("\"")){
                        String[] words = line.split(",");
                        if(words[words.length - 1].equals("Europe")) continue;
                        Double num = Double.parseDouble(words[words.length - 1].trim());
                        String country = words[2].trim();
                        map.put(country, num);
                    }
                    else{
                        String[] words = line.split(",");
                        if(words[words.length - 1].equals("Europe")) continue;
                        Double num = Double.parseDouble(words[words.length - 1].trim());
                        String country = words[1].trim();
                        map.put(country, num);
                    }
                }
            }
            double sum = 0;
            for (DoubleWritable value : values){
                sum += value.get();
            }
            if(map.get(key.toString()) == null) return;
            sum = (sum / (map.get(key.toString()))) * 1000000;
            context.write(key, new DoubleWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://127.0.0.1:9000");
        Job job = Job.getInstance(conf);
        job.addCacheFile(new Path(args[1]).toUri());
        job.setJarByClass(Covid19_3.class);
        job.setMapperClass(Covid19_3.CountMapper.class);
        job.setReducerClass(Covid19_3.CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
