package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.preprocessing;

import java.io.IOException;
import java.util.StringTokenizer;

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

	public static class EdgeWeightReaderMapper
		extends Mapper<Object, Text, Text, Text>{
			
		private Text interKey = new Text();    // Source node.
											   // Format: 101 (Text)
		private Text interValue = new Text();  // Destination node, weight
											   // Format: 54123,20 (Text) 
		private String destNodeWithWeight = new String();
		
		public void map(Object offset, Text edgeAndWeight, Context context)
			throws IOException, InterruptedException {
			StringTokenizer eawStr = new StringTokenizer(edgeAndWeight.toString());
			
			interKey.set(eawStr.nextToken());
			
			destNodeWithWeight.concat(eawStr.nextToken() + "," + eawStr.nextToken());
			interValue.set(destNodeWithWeight);
			
			context.write(interKey, interValue);
		}
		}
	
	public static class InfluenceCalculatorReducer
		extends Reducer<Text, Text, NullWritable, Text>{
		
		private Text resultValue = new Text();
		
		public void reduce(Text sourceNode, Iterable<Text> destNodeWithWeights, Context context)
			throws IOException, InterruptedException {
			
			int weightSum = 0;
			int tmpWeight = 0;
			String tmpValue = new String();
			String tmpDestNode = new String();
			
			// Calculate total edge weight
			for (Text itr : destNodeWithWeights){
				
				tmpValue = itr.toString();
				tmpWeight = Integer.valueOf(tmpValue.substring(tmpValue.indexOf(",")+1));
				
				weightSum += tmpWeight;
			}
			
			// Calculate and output individual influence
			for (Text itr : destNodeWithWeights){
				
				tmpValue = itr.toString();
				tmpWeight = Integer.valueOf(tmpValue.substring(tmpValue.indexOf(",")+1));
				tmpDestNode = tmpValue.substring(0, tmpValue.indexOf(",")-1);
				
				tmpValue.concat(sourceNode.toString() + "\t" 
								+ tmpDestNode + "\t"
								+ Integer.toString(tmpWeight/weightSum));
				
				context.write(NullWritable.get(), resultValue);
			}
		}
	}
	
	public void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: calculateinfluence <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Calculate Influence");
		job.setJarByClass(CalculateInfluence.class);
		job.setMapperClass(EdgeWeightReaderMapper.class);
		job.setReducerClass(InfluenceCalculatorReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
