import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The YouTubeAnalyser class aggregates data from multiple YouTube
 * channels in order to calculate a global average for the number 
 * of views, comments, and like/dislike ratio
 * 
 * @author Jacob Hiance
 *
 */

public class YouTubeAnalyser extends Configured implements Tool {

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
		
		// We want to get text data from the generated SequenceFile
		job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
		
		// We use the analyser classes to do this jib
		job.setMapperClass(YouTubeAnalyserMapper.class);
		job.setReducerClass(YouTubeAnalyserReducer.class);
		
		// Specify output key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Video.class);
		job.setOutputKeyClass(Channel.class);
		job.setOutputValueClass(NullWritable.class);
		
		// Run the job and see if it completes successfully
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new YouTubeAnalyser(), args);
		System.exit(exitCode);
	}

}
