package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.preprocessing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import tw.edu.ncku.ee.hpds.tai.mapred.comdetect.utils.NewmanMetricCounters;
import tw.edu.ncku.ee.hpds.tai.mapred.comdetect.utils.NewmanMetricInfo;
import tw.edu.ncku.ee.hpds.tai.mapred.comdetect.utils.NewmanMetricInfoFactory;

public class CalculateTotInfluence {

	public static class EdgeWeightTokenizerMapper extends
			Mapper<Object, Text, Text, DoubleWritable> {

		private Text interKey = new Text(); // Node
		private DoubleWritable interValue = new DoubleWritable(); // Single
																	// associated
																	// edge
																	// weight
		private String[] inValueSplit;
		
		public void map(Object offset, Text EdgeWithInfluence, Context context)
			throws IOException, InterruptedException {

			String ewiStr = new String(EdgeWithInfluence.toString());
			inValueSplit = ewiStr.split("\\t");

			// <k, v> = <node a, associate edge weight>
			interKey.set(inValueSplit[0]);
			interValue.set(Double.valueOf(inValueSplit[2]));
			context.write(interKey, interValue);

			// <k, v> = <node b, associate edge weight>
			interKey.set(inValueSplit[1]);
			context.write(interKey, interValue);

			// <k, v> = <node a + node b, associate edge weight> (original k-v)
			interKey.set(inValueSplit[0] + "\t" + inValueSplit[1]);
			context.write(interKey, interValue);
		}
	}

	public static class TotalEdgeWeightCalculateReducer extends
			Reducer<Text, DoubleWritable, NullWritable, Text> {
		
		private void incrementTotalInfluenceSumCounter(Double inc, Context context) {
			long incNormalized = (long) (inc * Math.pow(10, 6));
			context.getCounter(NewmanMetricCounters.TOTAL_INFLUENCE_SUM).increment(incNormalized);
		}
		
		private void setNewMaxNodeId(int newId, Context context) {
			long newIdNormalized = (long) newId;
			context.getCounter(NewmanMetricCounters.MAX_NODE_ID).setValue(newIdNormalized);
		}
		
		private boolean isLargerThanOldId(int newId, Context context) {
			
			long oldId = context.getCounter(NewmanMetricCounters.MAX_NODE_ID).getValue();
			if((long)newId > (long)oldId) return true;
			return false;
		}

		private Text resultValue = new Text();

		public void reduce(Text calculatedNode,
				Iterable<DoubleWritable> AssociatedInfluences, Context context)
				throws IOException, InterruptedException {

			double totalInfluence = 0;
			String calNodeStr = calculatedNode.toString();

			// If the current <k-v> is a node's associate edge weights,
			// Sum up all the weights and output "node	0	totalInfluence"
			if (!calNodeStr.contains("\t")) {

				for (DoubleWritable inf : AssociatedInfluences) {
					totalInfluence += inf.get();
				}

				resultValue.set(calNodeStr + "\t" + "0\t"
						+ Double.toString(totalInfluence));

				context.write(NullWritable.get(), resultValue);
				
				// Also, find the maximum node id
				// to validate the metric file
				// later used in the newman algorithm.
				// This part of code should be executed for all node ids.
				int calNodeInt = Integer.valueOf(calNodeStr);
				if(isLargerThanOldId(calNodeInt, context))
					setNewMaxNodeId(Integer.valueOf(calculatedNode.toString()),context);

				// else, (not a <k-v> intended for edge weight calculation)
				// just output the <k-v>
			} else {

				for (DoubleWritable inf : AssociatedInfluences) {

					// In fact, there should only be one value in
					// AssociatedInfluences
					// Therefore, only one k-v should be outputted
					resultValue.set(calculatedNode.toString() + "\t"
							+ inf.toString());
					context.write(NullWritable.get(), resultValue);
					
					incrementTotalInfluenceSumCounter(inf.get(), context);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: calculatetotinfluence <in> <out> <path_to_metric_file_in_HDFS>");
			System.exit(2);
		}

		Job job = new Job(conf, "Calculate Total Influence of Nodes");
		job.setJarByClass(CalculateTotInfluence.class);
		job.setMapperClass(EdgeWeightTokenizerMapper.class);
		job.setReducerClass(TotalEdgeWeightCalculateReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.waitForCompletion(true);
		
		// Write the Counter updates to metric file in HDFS
		//Path metricFilePathInHDFS = new Path(otherArgs[2]);
		
		
		/*NewmanMetricInfoFactory metricInfoFactory = new NewmanMetricInfoFactory();
		metricInfoFactory.readMetricInfoFromHDFS(metricFilePathInHDFS, conf);
		
		double newTotInfluenceSum = 
				job.getCounters().findCounter(NewmanMetricCounters.TOTAL_INFLUENCE_SUM).getValue() / Math.pow(10, 18);
		
		long newMaxNodeId = 
				job.getCounters().findCounter(NewmanMetricCounters.MAX_NODE_ID).getValue();
		
		metricInfoFactory.getMetricInfo().updateTotInfluenceSum(Double.toString(newTotInfluenceSum));
		metricInfoFactory.getMetricInfo().updateMaxNodeId(Integer.toString((int)newMaxNodeId));
		metricInfoFactory.writeMetricInfoToHDFS(metricFilePathInHDFS, conf);
		*/
		
		double newTotInfluenceSum = 
				job.getCounters().findCounter(NewmanMetricCounters.TOTAL_INFLUENCE_SUM).getValue() / Math.pow(10, 6);
		
		long newMaxNodeId = 
				job.getCounters().findCounter(NewmanMetricCounters.MAX_NODE_ID).getValue();
		
		NewmanMetricInfo metricInfo = new NewmanMetricInfo();
		metricInfo.update("101", 
							Integer.toString((int)newMaxNodeId), 
							Double.toString(newTotInfluenceSum), 
							"0", 
							"0");
		
		FileWriter fw = new FileWriter("/home/hpds/metricFile.txt");
		BufferedWriter bw = new BufferedWriter(fw);
		
		bw.write(metricInfo.toString());
		bw.close();
		
		System.exit(0);
	}
}