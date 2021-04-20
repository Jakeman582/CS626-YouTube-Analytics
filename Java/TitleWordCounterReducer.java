import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TitleWordCounterReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
	
	// We want a length cutoff
	private final int LENGTH_CUTOFF = 2;
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		if(key.toString().length() > LENGTH_CUTOFF) {
			context.write(key, NullWritable.get());
		}
	}

}
