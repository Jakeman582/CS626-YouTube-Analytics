package Counter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChannelWordCounter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		// Make sure an input path and output path are specified
		if(args.length != 2) {
			System.err.println("Usage: ChannelWordCounter <input path> <output path>");
			System.exit(0);
		}
		
		// Configure this job
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJarByClass(ChannelWordCounter.class);
		job.setJobName("Channel Word Counter");
		
		// Set the input and output locations
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Use the SequenceFile as input
		job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
		
		// Make sure the Reducer has the global word set
		job.addCacheFile(new Path("output/Project/GlobalWordCount/part-r-00000").toUri());
		
		// Use the associated mapper and reducer
		job.setMapperClass(ChannelWordCounterMapper.class);
		job.setReducerClass(ChannelWordCounterReducer.class);
		
		// Specify the output key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ChannelWordCounter(), args);
		System.exit(exitCode);
	}

}
