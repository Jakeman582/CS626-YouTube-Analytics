import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * We can merge all of the small YouTube data files into one larger SequenceFile
 * since this will reduce the number of Mappers per file, reducing disk seek time
 * and any overhead associated with tracking Mappers
 * 
 * @author Jacob Hiance
 *
 */
public class SmallFileToSequenceFileConverter extends Configured implements Tool {
	
	/* We can use a Mapper to associate filenames with each line of output */
	static class SequenceFileMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		/* We want to extract the file name */
		private Text filenameKey;
		
		/* Get the file name before any data processing */
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			
			// Get the base name of the file
			InputSplit split = context.getInputSplit();
			Path filePath = ((FileSplit) split).getPath();
			String filename = filePath.getName();
			
			// Use the part of the name before ".txt"
			filenameKey = new Text(filename);
		}
		
		/* Write out each line of input to the context, along with the file name */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(filenameKey, value);
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		// Make sure an input directory and output directory is specified
		if(args.length != 2) {
			System.err.println("Usage: SmallFileToSequenceFileConverter <input directory> <output directory>");
			System.exit(0);;
		}
		
		// Configure the job
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		job.setJarByClass(SmallFileToSequenceFileConverter.class);
		job.setJobName("Small file to SequenceFile converter");
		
		// Make sure to get the input and output locations
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// We want to generate a SequenceFile from a bunch of smaller files
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		// Make sure the class is set as the mapper
		job.setMapperClass(SequenceFileMapper.class);
		
		// Specify output key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Don't use compression for now
		SequenceFileOutputFormat.setCompressOutput(job, false);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SmallFileToSequenceFileConverter(), args);
		System.exit(exitCode);;
	}
	
}
