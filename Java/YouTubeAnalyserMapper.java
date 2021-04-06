import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class YouTubeAnalyserMapper extends Mapper<Text, Text, Text, Video> {
	
	// Each part of the value can be split into 5 parts
	private final int DATA_ITEMS = 5;
	
	private final int VIEW_COUNT = 0;
	private final int LIKE_COUNT = 1;
	private final int DISLIKE_COUNT = 2;
	private final int COMMENT_COUNT = 3;
	private final int VIDEO_TITLE = 4;
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		
		// Make sure to remove the ".txt" extension from the key
		String filename = key.toString().split("[.]")[0];
		
		// Make sure to get each part of the video data string
		String[] videoData = value.toString().split(" \\| ", DATA_ITEMS);
		
		// Extract the numeric data from the input data
		long views = Long.parseLong(videoData[VIEW_COUNT]);
		long comments = Long.parseLong(videoData[COMMENT_COUNT]);
		double likes = Double.parseDouble(videoData[LIKE_COUNT]);
		double dislikes = Double.parseDouble(videoData[DISLIKE_COUNT]);
		
		// Calculate the like/dislike ratio
		// For now, if dislikes = 0, then we will simply set it equal to one
		if(dislikes == 0) { dislikes = 1; }
		double ratio = likes/dislikes;
		
		// Construct the video data
		Video video = new Video(filename, videoData[VIDEO_TITLE], views, comments, ratio);
		
		// Write out the necessary data to the context for each channel
		context.write(new Text(filename), video);
		
		// Write out the same data for a global YouTubeChannel
		context.write(new Text("_"), video);
		
	}

}
