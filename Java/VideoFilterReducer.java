import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class VideoFilterReducer extends Reducer<Text, Video, Text, NullWritable> {
	
	// We need to load data for the averages into this reducer so 
	// we can filter each title
	private ChannelParser channelParser;
	private final String GLOBAL_CHANNEL_NAME = "GLOBALYOUTUBECHANNEL";
	
	// We want each YouTuber to have their own file of video titles
	private MultipleOutputs<Text, NullWritable> multipleOutputs;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		
		URI[] files = context.getCacheFiles();
		
		multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
		channelParser = new ChannelParser(new Path(files[0]));
	}
	
	@Override
	public void reduce(Text key, Iterable<Video> videos, Context context) throws IOException, InterruptedException {
		
		// Get this Youtuber's average data
		double channelViewAverage = channelParser.getAverageViews(key.toString()).get();
		double channelCommentAverage = channelParser.getAverageComments(key.toString()).get();
		double channelRatioAverage = channelParser.getAverageRatio(key.toString()).get();
		
		// Get the global averages
		double globalViewAverage = channelParser.getAverageViews(GLOBAL_CHANNEL_NAME).get();
		double globalCommentAverage = channelParser.getAverageViews(GLOBAL_CHANNEL_NAME).get();
		double globalRatioAverage = channelParser.getAverageViews(GLOBAL_CHANNEL_NAME).get();
		
		
		
		for(Video video : videos) {
			
			// If this video's metrics are better than the channel average,
			// save to a file
			if(video.getViews().get() > channelViewAverage &&
				video.getComments().get() > channelCommentAverage &&
				video.getRatio().get() > channelRatioAverage)
			{
				multipleOutputs.write(
						"Channel",
						video.getTitle(), 
						NullWritable.get(),
						"Channel/"+key.toString()
				);
			}
			
			// If this video's metrics are better than the channel average,
			// save to a file
			if(video.getViews().get() > globalViewAverage &&
				video.getComments().get() > globalCommentAverage &&
				video.getRatio().get() > globalRatioAverage)
			{
				multipleOutputs.write(
						"Global",
						video.getTitle(), 
						NullWritable.get(),
						"Global/"+key.toString()
				);
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		multipleOutputs.close();
	}
	
}
