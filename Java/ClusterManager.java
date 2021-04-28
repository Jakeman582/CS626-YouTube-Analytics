package Utils;

import java.util.ArrayList;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;

public class ClusterManager {
	
	/**
	 * 
	 */
	public static ArrayList<Integer> getInitialCenters(ArrayList<String> points, int numberOfClusters) {
		
		// Create an ArrayList to return the selected indices
		ArrayList<Integer> indices = new ArrayList<Integer>();
		
		// Pick the appropriate number of cluster centers from the 
		// supplied points
		Random generator = new Random();
		while(indices.size() < numberOfClusters) {
			int newIndex = generator.nextInt(points.size());
			if(!indices.contains(newIndex)) {
				indices.add(newIndex);
			}
		}
		
		return indices;
		
	}
	
	/**
	 * 
	 */
	public static void writeClusterCenters(ArrayList<String> points, ArrayList<Integer> indices, Configuration config) {
		
		for(int index = 0; index < indices.size(); index++) {
			
			// Get the index of the center from the list of indices
			int key = indices.get(index);
			
			// Set a label for the cluster having the specified center
			String label = "" + index;
			
			// Get the string representing the vector components
			String vector = points.get(key).split("\t")[1];
			
			// Write the label and associated center to the configuration
			config.set(label, vector);
			
		}
		
	}
	
	/**
	 * 
	 */
	public static ArrayList<String> readClusterCenters(int numberOfClusters, Configuration config) {
		
		ArrayList<String> centers = new ArrayList<String>();
		for(int index = 0; index < numberOfClusters; index++) {
			String label = "" + index;
			String center = config.get(label);
			centers.add(label + Point.LABEL_DELIMITER + center);
		}
		
		return centers;
		
	}

}
