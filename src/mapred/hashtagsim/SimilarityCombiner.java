package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

//A Combiner to reduce the time taken for shuffle and sort stage
public class SimilarityCombiner extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable intWritable = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {

		Integer count = 0;
		for (IntWritable val : value) {
			count = count + val.get();
		}
		intWritable.set(count);
		context.write(key, intWritable);

	}
}
