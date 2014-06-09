package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.newman;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.Math;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CalculateModularityChange {
	
	public static class EdgeTokenizerMapper
		extends Mapper<Object, Text, Text, Text>{
		
		private Text interKey = new Text();
		private Text interValue = new Text();
		private String[] inValueSplit;
		private Path[] metricFilePath;
		private int MIN_NODE_ID;
		private int MAX_NODE_ID;
		
		public void map(Object offset, Text EdgeWithInfluence, Context context)
			throws IOException, InterruptedException {
			
			metricFilePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			BufferedReader br = new BufferedReader(new FileReader(metricFilePath[0].toString()));
			
			while(br.ready()){
				String tmpLine = br.readLine();
				String[] lineSplit = tmpLine.split(",");
				
				MIN_NODE_ID = Integer.valueOf(lineSplit[0]);
				MAX_NODE_ID = Integer.valueOf(lineSplit[1]);
			}
			
			String ewiStr = new String(EdgeWithInfluence.toString());
			inValueSplit = ewiStr.split("\\t");
					
			if (inValueSplit[1] != null) {
				
				interKey.set(inValueSplit[0] + "," + inValueSplit[1]);
				interValue.set(inValueSplit[0] + "," + inValueSplit[1] + "," + inValueSplit[2]);
				context.write(interKey, interValue);
				
			} else {
				
				// Loop through all possible node pair permutations 
				for (int nodeCount_1 = MIN_NODE_ID; nodeCount_1 <= MAX_NODE_ID; nodeCount_1++) {
					for (int nodeCount_2 = nodeCount_1 + 1; nodeCount_2 <= MAX_NODE_ID; nodeCount_2++){
						
						interKey.set(Integer.toString(nodeCount_1) + "," + Integer.toString(nodeCount_2));
						interValue.set(inValueSplit[0] + "," + inValueSplit[1] + "," + inValueSplit[2]);
						context.write(interKey, interValue);
					}
				}
			}
		}
	}
	
	public static class CalculateModularityDiffReducer
		extends Reducer<Text, Text, NullWritable, Text>{
		
		private Text resultValue = new Text();
		private String[] interKeySplit;
		private String[] interValueSplit;
		private Path[] metricFilePath;
		
		private int a = 1;
		private int b = 0;
		private double modDiff = 0;
		
		private int TOTAL_INFLUENCE_SUM;
		
		public void reduce(Text mergeScheme, Iterable<Text> infoSequence, Context context)
			throws IOException, InterruptedException {
			
			metricFilePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			BufferedReader br = new BufferedReader(new FileReader(metricFilePath[0].toString()));
			
			while(br.ready()){
				String tmpLine = br.readLine();
				String[] lineSplit = tmpLine.split(",");
				
				TOTAL_INFLUENCE_SUM = Integer.valueOf(lineSplit[2]);
			}
			
			interKeySplit = mergeScheme.toString().split(",");
			
			for (Text inf : infoSequence) {
				
				interValueSplit = inf.toString().split(",");
				
				if (interKeySplit[1] != "0"){
					b += Integer.valueOf(interValueSplit[2]);
				} else {
					a *= Integer.valueOf(interValueSplit[2]);
				} 
			}
			
			// adding a random value between 0.0 and 1.0 to break ties in modularity differences
			modDiff = TOTAL_INFLUENCE_SUM*b + a + Math.random();
			
			for (Text inf : infoSequence) {
				
				interValueSplit = inf.toString().split(",");
				
				resultValue.set("-" + Double.toString(modDiff) +"\t"
								+ interValueSplit[0] + "\t"
								+ interValueSplit[1] + "\t"
								+ interValueSplit[2]);
				
				context.write(NullWritable.get(), resultValue);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Usage: newman-calculate-mod-change <in> <out> <path/to/metric/file>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Newman algorithm - Calculate Modularity Change");
		
		DistributedCache.addCacheFile(new URI(otherArgs[2]), conf);
		
		job.setJarByClass(CalculateModularityChange.class);
		job.setMapperClass(EdgeTokenizerMapper.class);
		job.setReducerClass(CalculateModularityDiffReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
