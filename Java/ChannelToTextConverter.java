import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ChannelToTextConverter extends Configured implements Tool {
	
	static class ChannelToTextMapper extends Mapper<LongWritable, Channel, Text, NullWritable> {
		
		@Override
		public void map(LongWritable key, Channel value, Context context) throws IOException, InterruptedException{
			
			String channelString = value.toString();
			
			context.write(new Text(channelString), NullWritable.get());
			
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		// Make sure an input directory and output directory is specified
		if(args.length != 2) {
			System.err.println("Usage: YouTubeAnalyser <input directory> <output directory>");
			System.exit(0);;
		}
		
		// Configure the job
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJarByClass(YouTubeAnalyser.class);
		job.setJobName("Global YouTube Average Calculator");
		
		// Make sure to get the input and output locations
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// We only use the ChannelToTextMapper, no reducer
		job.setMapperClass(ChannelToTextMapper.class);
		
		// Specify output key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		// Run the job and return any status codes
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ChannelToTextConverter(), args);
		System.exit(exitCode);
	}

}
