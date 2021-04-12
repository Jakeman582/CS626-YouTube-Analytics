import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A class for converting a SequenceFile composed of video data
 * into a text file to make sure the SequenceFile is being written
 * correctly
 * 
 * @author Jacob Hiance
 *
 */
public class SequenceFileToTextFileConverter extends Configured implements Tool {
	
	/* We only need to out put the value found in the original SequenceFile, there
	 * is no need for a reducer */
	static class TextFileMapper extends Mapper<Text, Text, Text, NullWritable> {
		
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
			
			// Make sure to remove the ".txt" extension from the key
			String[] filename = key.toString().split("[.]");
			
			// Create the new line of data for output
			Text line = new Text(filename[0] + " | " + value.toString());
			context.write(line, NullWritable.get());
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		// Make sure an input directory and output directory is specified
		if(args.length != 2) {
			System.err.println("Usage: SequenceFileToTextFileConverter <input directory> <output directory>");
			System.exit(0);;
		}
		
		// Configure the job
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJarByClass(SequenceFileToTextFileConverter.class);
		job.setJobName("SequenceFile to text file converter");
		
		// Make sure to get the input and output locations
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// We want to generate a text file from a SequenceFile
		job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
		
		// Make sure the class is set as the mapper
		job.setMapperClass(TextFileMapper.class);
		
		// Specify output key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		// Run the job
		return job.waitForCompletion(true) ? 0 : 1;

	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SequenceFileToTextFileConverter(), args);
		System.exit(exitCode);
	}

}
