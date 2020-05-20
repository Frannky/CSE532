import java.io.IOException;
import java.text.*;
import java.util.Date;
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

public class Covid19_2 {
    public static class CountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            String start_date = conf.get("start_date");
            String end_date = conf.get("end_date");
            String line = value.toString().trim();
            String[] words = line.split(",");
            String actual_date = words[0].trim();
            int case_number;
            if(words[3].trim().equals("new_deaths")) return;
            else {
                case_number = Integer.parseInt(words[3].trim());
            }
            String country_name = words[1].trim();

            try {
                if(data_compare(actual_date, start_date) > 0 || data_compare(actual_date, end_date) < 0){
                    return;
                }
            } catch (ParseException e) {
                e.printStackTrace();
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
        // date format YYYY/MM/DD
        String start_date = args[1];
        String end_date = args[2];
        conf.set("start_date",start_date);
        conf.set("end_date", end_date);
        Job job = Job.getInstance(conf);
        job.setJarByClass(Covid19_2.class);
        job.setMapperClass(Covid19_2.CountMapper.class);
        job.setReducerClass(Covid19_2.CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
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
