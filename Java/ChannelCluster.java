package Cluster;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Utils.ClusterManager;
import Utils.HDFSReader;
import Utils.Point;

public class ChannelCluster extends Configured implements Tool{
	
	private final int CLUSTERS = 6;
	@Override
	public int run(String[] args) throws Exception {
		
		// Make sure an input and output are specified
		if(args.length != 2) {
			System.out.println("Usage: ChannelCluster <input file> <output directory>");
			System.exit(0);
		}
		
		// Start setting up the configuration for this job
		Configuration config = new Configuration();
		
		// We need to load the YouTubers with their word vectors so we can 
		// initialize a set of clusters to save for later use
		config.set("NUMBER_OF_CLUSTERS", "" + CLUSTERS);
		ArrayList<String> channels = HDFSReader.readFile(args[0], config);
		ArrayList<Integer> indices = ClusterManager.getInitialCenters(channels, CLUSTERS);
		ClusterManager.writeClusterCenters(channels, indices, config);
		
		// Configure the job as normal
		Job job = Job.getInstance(config);
		job.setJarByClass(ChannelCluster.class);
		job.setJobName("Channel k-Means Clustering");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(ChannelClusterMapper.class);
		job.setReducerClass(ChannelClusterReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Point.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ChannelCluster(), args));
	}

}
