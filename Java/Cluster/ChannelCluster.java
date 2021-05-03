package Cluster;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
	
	@Override
	public int run(String[] args) throws Exception {
		
		// Make sure an input and output are specified
		if(args.length != 2) {
			System.out.println("Usage: ChannelCluster <input file> <output directory>");
			System.exit(0);
		}
		
		// Start setting up the configuration for this job
		Configuration config = new Configuration();
		
		// Get the number of channels to cluster
		ArrayList<String> channels = HDFSReader.readFile(args[0], config);
		int numberOfChannels = channels.size();
		
		// We can try different numbers of clusters from 2 to (numberOfChannels - 1)
		for(int numberOfClusters = 2; numberOfClusters < numberOfChannels - 1; numberOfClusters++) {
			
			// Log the number of clusters being used
			System.out.println("Number of clusters: " + numberOfClusters);
		
			// We need to load the YouTubers with their word vectors so we can 
			// initialize a set of clusters to save for later use
			config.set("NUMBER_OF_CLUSTERS", "" + numberOfClusters);
			
			ArrayList<Integer> indices = ClusterManager.getInitialCenters(channels, numberOfClusters);
			ClusterManager.writeClusterCenters(channels, indices, config);
			
			// We need a count of how many iterations have passed
			int count = 0;
			
			// See if the clusters have converged
			boolean converged = false;
			
			// We want to do clustering until there is convergence
			while(!converged) {
				
				// Configure the job as normal
				Job job = Job.getInstance(config);
				job.setJarByClass(ChannelCluster.class);
				job.setJobName("Channel k-Means Clustering");
				
				// We need to save the results in separate locations
				String currentPath = args[1] + numberOfClusters + "/" + count;
				String previousPath = args[1] + numberOfClusters + "/" + (count - 1) + "/part-r-00000";
				
				System.out.println("Writing to " + currentPath);
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(currentPath));
				
				job.setMapperClass(ChannelClusterMapper.class);
				job.setReducerClass(ChannelClusterReducer.class);
				
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				job.waitForCompletion(true);
				
				// If we are not on the first iteration, then previous cluster data
				// has been saved, so we can compare to see if convergence was achieved
				if(count > 0) {
					
					// Read the results from the previous iteration and the current iteration
					ArrayList<String> previousClusters = HDFSReader.readFile(previousPath, config);
					ArrayList<String> currentClusters = HDFSReader.readFile(currentPath + "/part-r-00000", config);
					
					// Store the labels and their cluster assignments for quick lookup
					Map<String, String> previous = new HashMap<String, String>();
					Map<String, String> current = new HashMap<String, String>();
					for(String group : previousClusters) {
						previous.put(group.split("\t")[0], group.split("\t")[1]);
					}
					for(String group : currentClusters) {
						current.put(group.split("\t")[0], group.split("\t")[1]);
					}
					
					// Compare to see if any differences exist
					// Assume they are the same; if any differences are found, set 
					// "converged" to false
					converged = true;
					for(String channel : current.keySet()) {
						if(!current.get(channel).equals(previous.get(channel))) {
							converged = false;
						}
					}
					
				}
				
				// We need to increment and continue clustering
				count++;
				
			}
			
			// For this number of clusters, compute the average distance from each point 
			// to it's assigned cluster's center
			double squareDistance = 0.0;
			
			// Get the cluster assignments from the previous procedure
			String resultsFile = args[1] + numberOfClusters + "/" + (count - 1) + "/part-r-00000";
			ArrayList<String> points = HDFSReader.readFile(resultsFile, config);
			Map<String, String> clusterAssignments = new HashMap<String, String>();
			for(String assignment : points) {
				clusterAssignments.put(assignment.split("\t")[0], assignment.split("\t")[1]);
			}
			
			// We also need the word vectors for each YouTuber args[0]
			ArrayList<String> vectors = HDFSReader.readFile(args[0], config);
			Map<String, String> wordVectors = new HashMap<String, String>();
			for(String vector : vectors) {
				wordVectors.put(vector.split(Point.LABEL_DELIMITER)[0], vector.split(Point.LABEL_DELIMITER)[1]);
			}
			
			// For each YouTuber
			for(String youtuber : wordVectors.keySet()) {
				
				// Get the assigned cluster
				String cluster = clusterAssignments.get(youtuber);
				
				// Get the cluster center from the configuration
				ArrayList<Double> center = Point.getCoordinateListFromCoordinateString(config.get(cluster));
				
				// Get the YouTuber's word vector
				ArrayList<Double> vector = Point.getCoordinateListFromCoordinateString(wordVectors.get(youtuber));
				
				// Compute the squared distance
				for(int index = 0; index < vector.size(); index++) {
					squareDistance += Math.pow(vector.get(index) - center.get(index), 2);
				}
				
			}
			
			int numberOfYouTubers = wordVectors.keySet().size();
			double averageDistance = squareDistance / numberOfYouTubers;
			
			// Save the average square distance, along with the number of clusters
			// used, to a local file
			String localFile = "AverageSquareDistance.txt";
			FileWriter writer = new FileWriter(localFile, true);
			writer.write("" + numberOfClusters + "\t" + averageDistance + "\n");
			writer.close();
			
		}
		
		return 0;
		
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ChannelCluster(), args));
	}

}
