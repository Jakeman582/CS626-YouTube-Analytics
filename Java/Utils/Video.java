package Utils;

import java.io.*;
import org.apache.hadoop.io.*;

/**
 * A writable class that allows you to easily group YouTube video data
 * into one structure.
 * 
 * Every video has an associated YouTube channel (the user who uploaded 
 * the video), a title, a number of views, a number of comments, and a 
 * like/dislike ratio.
 * 
 * @author Jacob Hiance
 *
 */
public class Video implements WritableComparable<Video>{
	
	private Text channel;		// The user who uploaded the video
	private Text title;		// The title of the video
	private LongWritable views;		// The number of views this video has
	private LongWritable comments;	// The number of comments this video has
	private DoubleWritable ratio;	// The like/dislike ratio for this video
	
	/* Constructors */
	public Video() {
		set(new Text(), new Text(), new LongWritable(), new LongWritable(), new DoubleWritable());
	}
	
	public Video(String channel, String title, long views, long comments, double ratio) {
		set(
				new Text(channel), 
				new Text(title), 
				new LongWritable(views), 
				new LongWritable(comments), 
				new DoubleWritable(ratio)
		);
	}
	
	public Video(Text channel, Text title, LongWritable views, LongWritable comments, DoubleWritable ratio) {
		set(channel, title, views, comments, ratio);
	}
	
	/* Populate this video's fields */
	public void set(Text channel, Text title, LongWritable views, LongWritable comments, DoubleWritable ratio) {
		this.channel = channel;
		this.title = title;
		this.views = views;
		this.comments = comments;
		this.ratio = ratio;
	}
	
	/* We need to be able to retrieve each field */
	public Text getChannel() { return this.channel; }
	public Text getTitle() { return this.title; }
	public LongWritable getViews() { return this.views; }
	public LongWritable getComments() { return this.comments; }
	public DoubleWritable getRatio() { return this.ratio; }
	
	/* We need to be able to write out a video object */
	@Override
	public void write(DataOutput out) throws IOException {
		this.channel.write(out);
		this.title.write(out);
		this.views.write(out);
		this.comments.write(out);
		this.ratio.write(out);
	}
	
	/* We need to be able to read in each field for a video */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.channel.readFields(in);
		this.title.readFields(in);
		this.views.readFields(in);
		this.comments.readFields(in);
		this.ratio.readFields(in);
	}
	
	/* We need to provide a hash function to allow for partitioning in the 
	 * MapReduce job */
	@Override
	public int hashCode() {
		int hash = this.channel.hashCode();
		hash = (hash * 113) + this.title.hashCode();
		hash = (hash * 109) + this.views.hashCode();
		hash = (hash * 107) + this.views.hashCode();
		hash = (hash * 103) + this.comments.hashCode();
		hash = (hash * 101) + this.ratio.hashCode();
		return hash;
	}
	
	/* Testing for equality */
	@Override
	public boolean equals(Object other) {
		
		// If the passed in object is a video, compare its fields
		if(other instanceof Video) {
			Video otherVideo = (Video) other;
			return this.channel.equals(otherVideo.channel) && 
					this.title.equals(otherVideo.title) &&
					this.views.equals(otherVideo.views) &&
					this.comments.equals(otherVideo.comments) &&
					this.ratio.equals(otherVideo.ratio);
		}
		
		// If the other object isn't a video, it can't be equal to this video
		return false;
	}
	
	/* Get a string representation of this video */
	@Override
	public String toString() {
		return channel + "\t" + title + "\t" + views + "\t" + comments + "\t" + ratio;
	}
	
	/* Compare one video to another lexicographically by channel, then title */
	@Override
	public int compareTo(Video otherVideo) {
		int compare = this.channel.compareTo(otherVideo.channel);
		if(compare != 0) {
			return compare;
		}
		return this.title.compareTo(otherVideo.title);
	}
	

}
