

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Covid19_1 {
    public static class CountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            String world = conf.get("contain_world");
            String line = value.toString().trim();
            String[] words = line.split(",");
            int case_number;
            if(words[2].trim().equals("new_cases")) return;
            else {
                case_number = Integer.parseInt(words[2].trim());
            }
            String country_name = words[1].trim();

            if(world.equals("false")){
                if(country_name.equals("World")) return;
            }
            context.write(new Text(country_name), new IntWritable(case_number));
        }
    }

    public static class CountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable>
    {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException
        {
             int sum = 0;
             for (IntWritable value : values){
                 sum += value.get();
             }
             context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://127.0.0.1:9000");
        String world = args[1];
        conf.set("contain_world",world);
        Job job = Job.getInstance(conf);
        job.setJarByClass(Covid19_1.class);
        job.setMapperClass(Covid19_1.CountMapper.class);
        job.setReducerClass(Covid19_1.CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
