package logparser;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class LogParser extends Configured implements Tool {

  private static Long TOTAL_BYTES = 0L;	

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new LogParser(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "logparser");
    job.setJarByClass(this.getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);
    job.waitForCompletion(true);
    
    TOTAL_BYTES = job.getCounters().getGroup("Meph").findCounter("TOTAL BYTES").getValue();

    System.out.println("Total Bytes: " + TOTAL_BYTES);
    
    return 1;
    
  }

  public static class Map extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    private static final Pattern LOG_REGEX = Pattern.compile("^(.*) - - (.*) (\\d{3}) (\\d+) \"(.*)$");

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      
      String line = lineText.toString();
      Matcher matcher = LOG_REGEX.matcher(line);      
      if (matcher.find()) {    	 
    	  LongWritable status = new LongWritable(Long.valueOf(matcher.group(3)));
    	  LongWritable bytes = new LongWritable(Long.valueOf(matcher.group(4)));
   	  
    	  context.write(status,bytes);    	  
    	  context.getCounter("Meph", "TOTAL BYTES").increment(Long.valueOf(matcher.group(4)));
      }  
    }
  }

  public static class Reduce extends Reducer<LongWritable, LongWritable, LongWritable,LongWritable> {
    @Override
    public void reduce(LongWritable key, Iterable<LongWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (LongWritable count : counts) {
        sum += count.get();
      }
      context.write(key, new LongWritable(sum));
    }
  }
}