package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<Text, Text, Text, IntWritable> {

	Map<String, Map<String, Integer>> tagWordCount = new HashMap<String, Map<String, Integer>>();

	@Override
	protected void reduce(LongWritable key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {		
	
		// Map<String, Integer> counts = new HashMap<String, Integer>();
		Integer count = 0;
		for (Text word : value) {
			String w = word.toString();
			count = count + Integer.parseInt(w);
		}

		context.write(key, new IntWritable(count));
		
		// StringBuilder builder = new StringBuilder();
		// for (Map.Entry<String, Integer> e : counts.entrySet()) 
			// builder.append(e.getKey() + ":" + e.getValue() + ";");
	}	
}
