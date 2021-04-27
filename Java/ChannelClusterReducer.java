package Cluster;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.Point;

public class ChannelClusterReducer extends Reducer<Text, Point, Text, Text> {
	
	public void reduce(Text key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
		
		// For the set of points passed in, calculate it's new center
		ArrayList<Double> newCenter = new ArrayList<Double>();
		int count = 0;
		for(Point point : points) {
			
			// For each of the passed in clusters, write out the YouTuber's name
			// along with the cluster label
			context.write(point.getLabel(), key);
			
			// For the first cluster, we just set each element of the array list
			// to whatever is in the first point's word vector
			ArrayList<Double> words = point.getCoordinatesArrayList();
			for(int index = 0; index < words.size(); index++) {
				if(count == 0) {
					newCenter.add(words.get(index));
				} else {
					newCenter.set(index, newCenter.get(index) + words.get(index));
				}
			}
			
			// Make sure to count the number of passed in clusters
			count++;
		}
		
		// Find the new cluster center by dividing each component by the count
		for(int index = 0; index < newCenter.size(); index++) {
			newCenter.set(index, newCenter.get(index) / count);
		}
		
		// Write the new cluster to the center
		Configuration config = context.getConfiguration();
		config.set(key.toString(), Point.stringifyCoordinateList(newCenter));
		
	}
	
}
