package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.newman;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Math;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CalculateModularityChange {
	
	public static class EdgeTokenizerMapper
		extends Mapper<Object, Text, Text, Text>{
		
		private Text interKey = new Text();
		private Text interValue = new Text();
		private String[] inValueSplit;
		private int MIN_NODE_ID;
		private int MAX_NODE_ID;
		
		public void map(Object offset, Text EdgeWithInfluence, Context context)
			throws IOException, InterruptedException {
			
			Path[] metricFilePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());		
			BufferedReader br = new BufferedReader(new FileReader(metricFilePath[0].toString()));
			
			
			while(br.ready()){
				String tmpLine = br.readLine();
				String[] lineSplit = tmpLine.split(",");
				
				MIN_NODE_ID = Integer.valueOf(lineSplit[0]);
				MAX_NODE_ID = Integer.valueOf(lineSplit[1]);
			}
			br.close();
			
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
		
		private double a = 1;
		private double b = 0;
		private double modDiff = 0;
		
		private Double TOTAL_INFLUENCE_SUM;
		
		public void reduce(Text mergeScheme, Iterable<Text> infoSequence, Context context)
			throws IOException, InterruptedException {
			ArrayList<Text> cache = new ArrayList<Text>();
			Path[] metricFilePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());		
			BufferedReader br = new BufferedReader(new FileReader(metricFilePath[0].toString()));
			
			while(br.ready()){
				String tmpLine = br.readLine();
				String[] lineSplit = tmpLine.split(",");
				
				TOTAL_INFLUENCE_SUM = Double.valueOf(lineSplit[2]);
			}
			
			br.close();
			
			interKeySplit = mergeScheme.toString().split(",");
			
			for (Text inf : infoSequence) {
				
				interValueSplit = inf.toString().split(",");
				
				if (interKeySplit[1] != "0"){
					b += Double.valueOf(interValueSplit[2]);
				} else {
					a *= Double.valueOf(interValueSplit[2]);
				}
				
				Text tmpinf = new Text();
				tmpinf.set(inf);
				cache.add(tmpinf);
			}
			
			// adding a random value between 0.0 and 1.0 to break ties in modularity differences
			modDiff = TOTAL_INFLUENCE_SUM*b + a + Math.random();
			
			int cacheSize = cache.size();
			//for (Text inf : infoSequence) {
			for (int count = 0; count < cacheSize; count++) {				
				interValueSplit = cache.get(count).toString().split(",");
				
				// "negative" modularity difference
				// so that the minimum will represent the maximum value
				// (for simplification in the next stage of MR computation)
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
		DistributedCache.addCacheFile(new URI("/user/hdfs-2.2.0/metricFile.txt"), job.getConfiguration());
		//DistributedCache.addCacheFile(new URI("/user/hdfs-2.2.0/metricFile.txt"), conf);
		//DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/hdfs-2.2.0/metricFile.txt"), conf);
		
		//job.addCacheFile(new URI("/user/hdfs-2.2.0/metricFile.txt"));
		
		job.setJarByClass(CalculateModularityChange.class);
		job.setMapperClass(EdgeTokenizerMapper.class);
		job.setReducerClass(CalculateModularityDiffReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
