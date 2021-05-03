package Counter;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChannelWordCounterMapper extends Mapper<Text, Text, Text, Text> {
	
	private final int DATA_ITEMS = 5;
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		// Make sure to get each word of the title
		String[] videoData = value.toString().split(" \\| ", DATA_ITEMS);
		String[] words = videoData[DATA_ITEMS - 1].split("[\\s_/]+");
		//String[] words = videoData[DATA_ITEMS - 1].split("[\\s_/\\x00-\\x7f]+");
		
		// For each word
		for(String word : words) {
			
			word = word.replaceAll("[_/]+", " ");
			word = word.replaceAll("[^\\x00-\\x7f]+", "");
			String newWord = "";
			
			// Remove all punctuation marks
			for(char c : word.toCharArray()) {
				if(Character.isLetterOrDigit(c) &&
						(c >= 'A' && c <= 'a'))
				{
					newWord += c;
				}
			}
			
			context.write(key, new Text(newWord.toLowerCase()));
		
		}
	}

}
