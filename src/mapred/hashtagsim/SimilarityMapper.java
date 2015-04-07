package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	Text text = new Text();
	IntWritable intWritable = new IntWritable();

	/**
	 * We compute the inner product of feature vector of every hashtag with that
	 * of #job
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] hashtag_featureVector = line.split("\\s+", 2);
		String[] hashCount = hashtag_featureVector[1].split(";");
		String[] hashTag = new String[hashCount.length];
		int[] count = new int[hashCount.length];
		for (int i = 0; i < hashCount.length; i++) {
			String[] pair = hashCount[i].split(":");
			hashTag[i] = pair[0];
			count[i] = Integer.parseInt(pair[1]);
		}

		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < count.length; i++) {
			for (int j = i + 1; j < count.length; j++) {
				builder.setLength(0);
				int val = count[i] * count[j];
				// Add a comparision to avoid pairs like (#a #b) & (#b #a)
				if (hashTag[i].compareTo(hashTag[j]) < 0) {
					builder.append(hashTag[i]);
					builder.append("\t");
					builder.append(hashTag[j]);
				} else {
					builder.append(hashTag[j]);
					builder.append("\t");
					builder.append(hashTag[i]);
				}

				String hashPair = builder.toString();
				text.set(hashPair);
				intWritable.set(val);
				context.write(text, intWritable);
			}
		}
	}

}
