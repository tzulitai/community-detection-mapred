package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.preprocessing;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CalculateInfluence {

	public static class EdgeWeightReaderMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text interKey = new Text(); // Source node.
											// Format: 101 (Text)
		private Text interValue = new Text(); // Destination node, weight
												// Format: 54123,20 (Text)

		public void map(Object offset, Text edgeAndWeight, Context context)
				throws IOException, InterruptedException {
			String eawStr = new String(edgeAndWeight.toString());
			String[] eawStrSplit = eawStr.split("\t");
			interKey.set(eawStrSplit[0]);
			interValue.set(eawStrSplit[1] + "\t" + eawStrSplit[2]);
			context.write(interKey, interValue);
		}
	}

	public static class InfluenceCalculatorReducer extends
			Reducer<Text, Text, NullWritable, Text> {

		private Text resultValue = new Text();

		public void reduce(Text sourceNode, Iterable<Text> destNodeWithWeights,
				Context context) throws IOException, InterruptedException {

			double weightSum = 0;
			String tmpValue = new String();
			String[] tmpindivValue = new String[2];
			ArrayList<Text> cache = new ArrayList<Text>();

			// Calculate total edge weight
			for (Text itr : destNodeWithWeights) {
				tmpValue = itr.toString();
				tmpindivValue = tmpValue.split("\t");
				weightSum += Double.valueOf(tmpindivValue[1]);

				Text tmpItr = new Text();
				tmpItr.set(itr);
				cache.add(tmpItr);
			}

			// Calculate and output individual influence
			int cacheSize = cache.size();
			for (int count = 0; count < cacheSize; count++) {
				tmpValue = cache.get(count).toString();
				tmpindivValue = tmpValue.split("\t");
				resultValue.set(sourceNode.toString()
						+ "\t"
						+ tmpindivValue[0]
						+ "\t"
						+ String.valueOf(Double.valueOf(tmpindivValue[1])
								/ weightSum));
				context.write(NullWritable.get(), resultValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: calculateinfluence <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Calculate Influence");
		job.setNumReduceTasks(128);
		job.setJarByClass(CalculateInfluence.class);
		job.setMapperClass(EdgeWeightReaderMapper.class);
		job.setReducerClass(InfluenceCalculatorReducer.class);
		job.setMapOutputKeyClass(Text.class);// map output key
		job.setMapOutputValueClass(Text.class);// map output value
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
