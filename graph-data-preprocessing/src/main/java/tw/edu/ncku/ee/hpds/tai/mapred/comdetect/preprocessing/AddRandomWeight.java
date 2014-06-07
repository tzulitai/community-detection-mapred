package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.preprocessing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;


public class AddRandomWeight {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text person = new Text();
		private Text mapout = new Text();
		private String[] tmp = new String[2];
		private double randomVariable;
		private double lamba = 0.1;
		private int weight;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			for (int i = 0; itr.hasMoreTokens() && i < 2; i++) {
				tmp[i] = itr.nextToken();
			}
			person.set(tmp[0]);
			// Generate a weight by expontial distribution
			randomVariable = Math.random();
			weight = (int) java.lang.Math.ceil(-Math.log(1 - randomVariable)
					/ lamba);
			mapout.set(tmp[1] + "\t" + weight);
			context.write(person, mapout);
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				result.set(val);
				context.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: friendWeight <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "friend weight");
		job.setJarByClass(AddRandomWeight.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
