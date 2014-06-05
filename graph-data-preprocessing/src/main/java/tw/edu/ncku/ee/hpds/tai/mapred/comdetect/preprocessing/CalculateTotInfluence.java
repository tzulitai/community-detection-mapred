package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.preprocessing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CalculateTotInfluence {

	public static class EdgeWeightTokenizerMapper
		extends Mapper<Object, Text, Text, IntWritable>{
		
		private Text interKey = new Text();					// Node
		private IntWritable interValue = new IntWritable();	// Single associated edge weight
		private String[] inValueSplit;
		
		public void map(Object offset, Text EdgeWithInfluence, Context context)
			throws IOException, InterruptedException {
			String ewiStr = new String(EdgeWithInfluence.toString());
			inValueSplit = ewiStr.split("\\t");
			
			// <k, v> = <node a, associate edge weight>
			interKey.set(inValueSplit[0]);
			interValue.set(Integer.valueOf(inValueSplit[2]));
			context.write(interKey, interValue);
			
			// <k, v> = <node b, associate edge weight>
			interKey.set(inValueSplit[1]);
			context.write(interKey, interValue);
			
			// <k, v> = <node a + node b, associate edge weight> (original k-v)
			interKey.set(inValueSplit[0] + "\t" + inValueSplit[1]);
			context.write(interKey, interValue);
		}
	}
	
	public static class TotalEdgeWeightCalculateReducer
		extends Reducer<Text, IntWritable, NullWritable, Text>{
		
		private Text resultValue = new Text();
		
		public void reduce(Text calculatedNode, Iterable<IntWritable> AssociatedInfluences, Context context)
			throws IOException, InterruptedException {
			
			int totalInfluence = 0;
			
			// If the current <k-v> is a node's associate edge weights,
			// Sum up all the weights and output "node	0	totalInfluence"
			if (!calculatedNode.toString().contains("\t")){  
				
				for (IntWritable inf : AssociatedInfluences){
					totalInfluence += inf.get();
				}
				
				resultValue.set(calculatedNode.toString() + "\t"
								+ "0\t"
								+ Integer.toString(totalInfluence));
				
				context.write(NullWritable.get(), resultValue);
			
			// else, (not a <k-v> intended for edge weight calculation)
			// just output the <k-v>
			} else {	
				
				for (IntWritable inf : AssociatedInfluences){
					
					// In fact, there should only be one value in AssociatedInfluences
					// Therefore, only one k-v should be outputted
					resultValue.set(calculatedNode.toString() + "\t"
									+ inf.toString());
					context.write(NullWritable.get(), resultValue);
				}	
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: calculatetotinfluence <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Calculate Total Influence of Nodes");
		job.setJarByClass(CalculateTotInfluence.class);
		job.setMapperClass(EdgeWeightTokenizerMapper.class);
		job.setReducerClass(TotalEdgeWeightCalculateReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}