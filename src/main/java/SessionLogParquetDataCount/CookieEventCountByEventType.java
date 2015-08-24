package SessionLogParquetDataCount;

import com.mediav.data.log.CookieEvent;
import com.twitter.elephantbird.mapreduce.input.combine.DelegateCombineFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.hadoop.thrift.ParquetThriftInputFormat;

import java.io.IOException;

/**
 * Created by chruqin on 15-8-24.
 */
public class CookieEventCountByEventType extends Configured implements Tool {

  public enum EVENTTYPE {
    S, C, T, V, OTHER
  }

  public static class CookieEventMapper extends Mapper<LongWritable, CookieEvent, IntWritable, LongWritable> {

    private final static LongWritable one = new LongWritable(1);

    @Override
    public void map(LongWritable key, CookieEvent value, Context context) throws IOException, InterruptedException {
      if (value != null) {
        if (value.isSetEventType()) {
          context.write(new IntWritable(value.getEventType()), one);
        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<IntWritable, LongWritable, Text, LongWritable> {

    private LongWritable result = new LongWritable();

    @Override
    public void reduce(IntWritable key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {

      long outValue = 0;
      for (LongWritable count : value) {
        outValue += count.get();
      }
      String outputKey = null;
      switch ((char)key.get()) {
        case 's':
          context.getCounter(EVENTTYPE.S).increment(outValue);
          outputKey = "s";
          break;
        case 't':
          context.getCounter(EVENTTYPE.T).increment(outValue);
          outputKey = "t";
          break;
        case 'c':
          context.getCounter(EVENTTYPE.C).increment(outValue);
          outputKey = "c";
          break;
        case 'v':
          context.getCounter(EVENTTYPE.V).increment(outValue);
          outputKey = "v";
          break;
        default:
          context.getCounter(EVENTTYPE.OTHER).increment(outValue);
          outputKey = "other";
      }
      result.set(outValue);
      context.write(new Text(outputKey), result);
    }
  }

  public int run(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Path in = new Path(args[0]);
    Path out = new Path(args[1]);

    // setup job
    Job job = Job.getInstance(conf);
    job.setJobName("CookieEventCountByEventType");
    job.setJarByClass(CookieEventCountByEventType.class);
    job.setMapperClass(CookieEventMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // set InputFormatClass to be DelegateCombineFileInputFormat to Combine Small Splits
    job.setInputFormatClass(DelegateCombineFileInputFormat.class);
    DelegateCombineFileInputFormat.setCombinedInputFormatDelegate(job.getConfiguration(), ParquetThriftInputFormat.class);
    ParquetThriftInputFormat.addInputPath(job, in);

    // be sure to set ParquetThriftInputFormat ReadSupportClass and ThriftClass
    ParquetThriftInputFormat.setReadSupportClass(job, CookieEvent.class);
    ParquetThriftInputFormat.setThriftClass(job.getConfiguration(), CookieEvent.class);

    FileOutputFormat.setOutputPath(job, out);
    return job.waitForCompletion(true) ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new CookieEventCountByEventType(), args);
  }
}
