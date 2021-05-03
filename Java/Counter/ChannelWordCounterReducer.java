package Counter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelWordCounterReducer extends Reducer<Text, Text, Text, Text> {
	
	// Each reducer has to output a count of each word
	TreeMap<String, Integer> wordCount;
	
	private final int LENGTH_CUTOFF = 2;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		
		wordCount = new TreeMap<String, Integer>();
		Path file = new Path(context.getCacheFiles()[0]);
		
		// Attempt to read each line of the file
		try {
			
			// Open the file for reading each channel line
			BufferedReader reader = new BufferedReader(new FileReader(file.getName()));
			
			try {
				// Populate the tree with the word and and current count (0)
				String line = reader.readLine();
				while(line != null) {
					wordCount.put(line, 0);
					line = reader.readLine();
				}
				
			} finally {
				// Make sure the reader is closed
				reader.close();
			}
			
			
		} catch(IOException exception) {
			exception.printStackTrace();
		}
		
	}
	
	public void reduce(Text key, Iterable<Text> words, Context context) throws IOException, InterruptedException {
		
		// We need to construct a string representing the vector
		StringBuilder wordVector = new StringBuilder();
		
		// Remove the .txt extension from the file name
		String filename = key.toString().split("[.]")[0];
		
		// Increment the counts for each word found in this YouTuber's titles
		for(Text word : words) {
			
			if(word.getLength() > LENGTH_CUTOFF) {
				int count = wordCount.get(word.toString());
				wordCount.put(word.toString(), count + 1);
			}
			
		}
		
		// Construct the vector of values for each word
		for(String word : wordCount.keySet()) {
			wordVector.append(wordCount.get(word));
			wordVector.append(" ");
		}
		
		// Remove the last space
		wordVector.deleteCharAt(wordVector.length() - 1);
		
		// Write to the context
		context.write(new Text(filename), new Text(wordVector.toString()));
		
	}

}
