package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.newman;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MergeAndAdjust {

	public static class EdgeWeightTokenizerMapper
		extends Mapper<Object, Text, Text, DoubleWritable>{
		
		// <k-v> = <(i,j) - (edgeWeight)>
		private Text interKey = new Text();
		private DoubleWritable interValue = new DoubleWritable();
		private String[] inValueSplit;
		
		public void map(Object offset, Text EdgeWithWeight, Context context)
			throws IOException, InterruptedException {
			
			inValueSplit = EdgeWithWeight.toString().split("\\t");
			
			interKey.set(inValueSplit[0] + "," + inValueSplit[1]);
			interValue.set(Double.valueOf(inValueSplit[2]));
			context.write(interKey, interValue);
		}
	}
	
	public static class EdgeWeightAdjusterReducer
		extends Reducer<Text, DoubleWritable, NullWritable, Text>{
		
		private Text resultValue = new Text();
		private Path[] metricFilePath;
		private double newWeightSum;
		private String CURRENT_MERGE_I;
		private String[] inKeySplit;
		
		public void reduce(Text nodePair, Iterable<DoubleWritable> edgeWeights, Context context)
			throws IOException, InterruptedException {
			
			metricFilePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			BufferedReader br = new BufferedReader( new FileReader(metricFilePath[0].toString()));
			
			while(br.ready()){
				String tmpLine = br.readLine();
				String[] lineSplit = tmpLine.split(",");
				
				CURRENT_MERGE_I = lineSplit[3];
			}
			
			inKeySplit = nodePair.toString().split(",");
				
			if((inKeySplit[0] == CURRENT_MERGE_I) && inKeySplit[1] == "0"){
				
				for (DoubleWritable ew : edgeWeights){
					
					resultValue.set(inKeySplit[0] + "\t"
									+ "0\t"
									+ ew.toString());
			
					context.write(NullWritable.get(), resultValue);
					
				}
					
			} else {
					
				newWeightSum = 0;
				
				for (DoubleWritable ew : edgeWeights) newWeightSum += ew.get();
				
				resultValue.set(inKeySplit[0] +"\t"
								+ inKeySplit[1] + "\t"
								+ Double.toString(newWeightSum));
				
				context.write(NullWritable.get(), resultValue);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Usage: newman-sort-mod-change <in> <out> <path/to/metric/file>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Newman algorithm - Sort Modularity Change");
		
		DistributedCache.addCacheFile(new URI(otherArgs[2]), conf);
		
		job.setJarByClass(MergeAndAdjust.class);
		job.setMapperClass(EdgeWeightTokenizerMapper.class);
		job.setReducerClass(EdgeWeightAdjusterReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
