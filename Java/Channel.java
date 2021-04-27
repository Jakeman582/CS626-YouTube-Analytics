package Utils;

import java.io.*;
import org.apache.hadoop.io.*;

/**
 * A writable class that allows you to easily group YouTube channel data
 * into one structure.
 * 
 * Every channel has an associated YouTube user (the user who uploaded 
 * the video), average number of views, average number of comments, and the 
 * average like/dislike ratio.
 * 
 * @author Jacob Hiance
 *
 */
public class Channel implements WritableComparable<Channel>{
	
	private Text user;	// The user who uploaded the video
	private DoubleWritable averageViews;	// The number of views this video has
	private DoubleWritable averageComments;	// The number of comments this video has
	private DoubleWritable averageRatio;	// The like/dislike ratio for this video
	
	/* Constructors */
	public Channel() {
		set(new Text(), new DoubleWritable(), new DoubleWritable(), new DoubleWritable());
	}
	
	public Channel(String user, double averageViews, double averageComments, double averageRatio) {
		set(
				new Text(user),
				new DoubleWritable(averageViews), 
				new DoubleWritable(averageComments), 
				new DoubleWritable(averageRatio)
		);
	}
	
	public Channel(Text user, DoubleWritable averageViews, DoubleWritable averageComments, DoubleWritable averageRatio) {
		set(user, averageViews, averageComments, averageRatio);
	}
	
	/* Populate this Channel's fields */
	public void set(Text user, DoubleWritable averageViews, DoubleWritable averageComments, DoubleWritable averageRatio) {
		this.user = user;
		this.averageViews = averageViews;
		this.averageComments = averageComments;
		this.averageRatio = averageRatio;
	}
	
	/* We need to be able to retrieve each field */
	public Text getUser() { return this.user; }
	public DoubleWritable getAverageViews() { return this.averageViews; }
	public DoubleWritable getAverageComments() { return this.averageComments; }
	public DoubleWritable getAverageRatio() { return this.averageRatio; }
	
	/* We need to be able to write out a Channel object */
	@Override
	public void write(DataOutput out) throws IOException {
		this.user.write(out);
		this.averageViews.write(out);
		this.averageComments.write(out);
		this.averageRatio.write(out);
	}
	
	/* We need to be able to read in each field for a Channel */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.user.readFields(in);
		this.averageViews.readFields(in);
		this.averageComments.readFields(in);
		this.averageRatio.readFields(in);
	}
	
	/* We need to provide a hash function to allow for partitioning in the 
	 * MapReduce job */
	@Override
	public int hashCode() {
		int hash = this.user.hashCode();
		hash = (hash * 109) + this.averageViews.hashCode();
		hash = (hash * 107) + this.averageComments.hashCode();
		hash = (hash * 101) + this.averageRatio.hashCode();
		return hash;
	}
	
	/* Testing for equality */
	@Override
	public boolean equals(Object other) {
		
		// If the passed in object is a Channel, compare its user
		if(other instanceof Channel) {
			Channel otherChannel = (Channel) other;
			return this.user.equals(otherChannel.user);
		}
		
		// If the other object isn't a Channel, it can't be equal to this video
		return false;
	}
	
	/* Get a string representation of this Channel */
	@Override
	public String toString() {
		return this.user + "\t" + this.averageViews + "\t" + this.averageComments + "\t" + this.averageRatio;
	}
	
	/* Compare one Channel to another lexicographically by user */
	@Override
	public int compareTo(Channel otherChannel) {
		return this.user.compareTo(otherChannel.user);
	}

}
