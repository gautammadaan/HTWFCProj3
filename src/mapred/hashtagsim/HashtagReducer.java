package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;

public class HashtagReducer extends Reducer<Text, MapWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<MapWritable> value, Context context)
			throws IOException, InterruptedException {

		Map<String, Integer> counts = new HashMap<String, Integer>();

		for (MapWritable countMap : value) {
			for (Writable k : countMap.keySet()) {
				String word = ((Text) k).toString();
				if (counts.containsKey(word)) {
					int count = counts.get(word);
					count++;
					counts.put(word, count);
				} else {
					counts.put(word, 1);
				}
			}
		}

		/*
		 * We have changed the output from: Hashtag
		 * feature1:count1;feature2:count2;...to feature
		 * hashtag1:count1;hashtag2:count2;....
		 */
		if (counts.size() > 1) {
			StringBuilder builder = new StringBuilder();
			for (Map.Entry<String, Integer> e : counts.entrySet())
				builder.append(e.getKey() + ":" + e.getValue() + ";");

			context.write(key, new Text(builder.toString()));
		}
	}
}
