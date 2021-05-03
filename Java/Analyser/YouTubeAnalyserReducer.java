package Analyser;
import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Utils.Channel;
import Utils.Video;

public class YouTubeAnalyserReducer extends Reducer<Text, Video, Channel, NullWritable>{
	
	@Override
	public void reduce(Text key, Iterable<Video> videos, Context context) throws IOException, InterruptedException {
		
		// We need to keep a running total of the number of videos seen
		long count = 0;
		
		// Collect the totals for each video
		long views = 0;
		long comments = 0;
		double ratio = 0;
		
		// Collect the data from each video
		for(Video video : videos) {
			count++;
			views += video.getViews().get();
			comments += video.getComments().get();
			ratio += video.getRatio().get();
		}
		
		// Calculate the averages
		double averageViews = ((double) views) / count;
		double averageComments = ((double) comments) / count;
		double averageRatio = ratio / count;
		
		// Output the data
		context.write(
				new Channel(key.toString(), averageViews, averageComments, averageRatio), 
				NullWritable.get()
		);
		
	}

}
