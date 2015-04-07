package mapred.hashtagsim;

import java.io.IOException;

import mapred.util.Tokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashtagMapper extends
		Mapper<LongWritable, Text, Text, MapWritable> {

	MapWritable countMap = new MapWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);

		countMap.clear();

		/*
		 * Iterate all words, find out all hashtags, then iterate all other
		 * non-hashtag words and map out.
		 */
		for (String word : words) {
			if (word.startsWith("#")) {

				Text wordText = new Text(word);
				if (countMap.containsKey(wordText)) {
					IntWritable count = (IntWritable) countMap.get(wordText);
					System.out.println(count.get());
					count.set(count.get() + 1);
				} else {
					countMap.put(wordText, new IntWritable(1));
				}
			}
		}
		if (countMap.size() != 0) {
			for (String word : words) {
				if (word.startsWith("#") == false) {
					context.write(new Text(word), countMap);
				}
			}
		}

	}
}
