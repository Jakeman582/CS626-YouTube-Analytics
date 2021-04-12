import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VideoFilter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		// Make sure an input path and output path are specified
		if(args.length != 2) {
			System.err.println("Usage: VideoFilter <input path> <output path>");
			System.exit(0);
		}
		
		// Create the job
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJarByClass(VideoFilter.class);
		job.setJobName("Video Filter");
		
		// Set the input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job,  new Path(args[1]));
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		// Use the SequenceFile as input
		job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
		
		// Make sure the averages for each channel are available
		job.addCacheFile(new Path("output/Project/ChannelFile/part-r-00000").toUri());
		
		// We need to define all output directories since MultipleOutputs
		// is being used
		MultipleOutputs.addNamedOutput(
				job, 
				"Channel", 
				TextOutputFormat.class, 
				Text.class, 
				NullWritable.class
		);
		MultipleOutputs.addNamedOutput(
				job, 
				"Global", 
				TextOutputFormat.class, 
				Text.class, 
				NullWritable.class
		);
		
		// We use the associated Mapper and Reducer
		job.setMapperClass(VideoFilterMapper.class);
		job.setReducerClass(VideoFilterReducer.class);
		
		// We need to specify the map and reduce output key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Video.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		// Wait for job completion and return the status
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new VideoFilter(), args);
		System.exit(exitCode);
	}

}
