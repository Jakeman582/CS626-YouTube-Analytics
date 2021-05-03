package Cluster;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.Point;

/**
 * 
 * @author cloudera
 *
 */
public class ChannelClusterReducer extends Reducer<Text, Text, Text, Text> {
	
	/**
	 * 
	 */
	public void reduce(Text key, Iterable<Text> points, Context context) throws IOException, InterruptedException {
		
		// For the set of points passed in, calculate it's new center
		int count = 0;
		ArrayList<Double> newCenter = new ArrayList<Double>();
		for(Text pointString : points) {
			
			String point = Point.getLabelString(pointString.toString());
			ArrayList<Double> list = Point.getCoordinateListFromPointString(pointString.toString());
			
			// For each of the passed in clusters, write out the YouTuber's name
			// along with the cluster label
			context.write(new Text(point), key);
			
			// For the first cluster, we just set each element of the array list
			// to whatever is in the first point's word vector
			//ArrayList<Double> words = point.getCoordinatesArrayList();
			for(int index = 0; index < list.size(); index++) {
				if(count == 0) {
					newCenter.add(list.get(index));
				} else {
					newCenter.set(index, newCenter.get(index) + list.get(index));
				}
			}
			
			// Make sure to count the number of passed in clusters to keep track of which file
			// to write to, and which iteration we're on
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
