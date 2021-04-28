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

/**
 * 
 * @author cloudera
 *
 */
public class ChannelClusterMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Map<String, ArrayList<Double>> clusters;	// 
	
	/**
	 * 
	 */
	public void setup(Context context) throws IOException, InterruptedException{
		
		clusters = new HashMap<String, ArrayList<Double>>();
		Configuration config = context.getConfiguration();
		int numberOfClusters = Integer.parseInt(config.get("NUMBER_OF_CLUSTERS"));
		
		// Read in the cluster centers from the job configuration
		// and populate the array list
		for(String point : ClusterManager.readClusterCenters(numberOfClusters, config)) {
			clusters.put(
					Point.getLabelString(point),
					Point.getCoordinateListFromPointString(point)
			);
		}

	}
	
	/**
	 * 
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		// Form an array from the word count in the passed in value
		//Point point = new Point(value.toString());
		String pointLabel = Point.getLabelString(value.toString());
		String pointCoordinates = Point.getCoordinateString(value.toString());
		ArrayList<Double> list = Point.getCoordinateListFromPointString(value.toString());
		
		// For each cluster, keep track of the closest label and minimum distance
		String closestLabel = "";
		double minimum = Double.MAX_VALUE;
		for(String label : clusters.keySet()) {
			
			// Compute the distance from this cluster to the current
			// data point
			double accumulator = 0.0;
			ArrayList<Double> clusterCenter = clusters.get(label);
			for(int index = 0; index < list.size(); index++) {
				double x = clusterCenter.get(index) - list.get(index);
				accumulator += x * x;
			}
			
			// Check to see if the squared distance is smaller than the current minimum
			// squared distance found
			if(accumulator < minimum) {
				minimum = accumulator;
				closestLabel = String.copyValueOf(label.toCharArray());
			}
			
		}
		
		context.write(new Text(closestLabel), new Text(pointLabel + Point.LABEL_DELIMITER + pointCoordinates));
		
	}
	
}
