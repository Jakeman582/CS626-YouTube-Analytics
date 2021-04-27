package Cluster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Utils.ClusterManager;
import Utils.Point;

/*
 * The mapper seemed to run faster if everything was not packaged into a Point
 * object, but instead just by passing the text objects around. Perhaps 
 * try to refactor the Point utility class to just offer parsing methods
 * instead of representing an actual object
 */

public class ChannelClusterMapper extends Mapper<LongWritable, Text, Text, Point> {
	
	private Map<String, ArrayList<Double>> clusters;
	
	public void setup(Context context) throws IOException, InterruptedException{
		
		clusters = new HashMap<String, ArrayList<Double>>();
		Configuration config = context.getConfiguration();
		int numberOfClusters = Integer.parseInt(config.get("NUMBER_OF_CLUSTERS"));
		
		// Read in the cluster centers from the job configuration
		// and populate the array list
		for(Point point : ClusterManager.readClusterCenters(numberOfClusters, config)) {
			clusters.put(
					point.getLabel().toString(), 
					new ArrayList<Double>(point.getCoordinatesArrayList())
			);
		}

	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		// Form an array from the word count in the passed in value
		Point point = new Point(value.toString());
		
		// For each cluster, keep track of the closest label and minimum distance
		String closestLabel = "";
		double minimum = Double.MAX_VALUE;
		for(String label : clusters.keySet()) {
			
			// Compute the distance from this cluster to the current
			// data point
			double accumulator = 0.0;
			ArrayList<Double> clusterCenter = clusters.get(label);
			for(int index = 0; index < point.getCoordinatesArrayList().size(); index++) {
				double x = clusterCenter.get(index) - point.getCoordinatesArrayList().get(index);
				accumulator += x * x;
			}
			
			// Check to see if the squared distance is smaller than the current minimum
			// squared distance found
			if(accumulator < minimum) {
				minimum = accumulator;
				closestLabel = String.copyValueOf(label.toCharArray());
			}
			
		}
		
		context.write(new Text(closestLabel), point);
		
	}
	
}
