package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.newman;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import tw.edu.ncku.ee.hpds.tai.mapred.comdetect.newman.utils.MetricCounters;

public class SortModularityChange {
	
	public static class ModDiffTokenizerMapper
		extends Mapper<Object, Text, DoubleWritable, Text>{
		
		private DoubleWritable interModDiff = new DoubleWritable();
		private Text interValue = new Text();
		
		public void map(Object offset, Text modDiffWithInfo, Context context)
			throws IOException, InterruptedException {
			String mdwiStr = new String(modDiffWithInfo.toString());
			String[] mdwiStrSplit = mdwiStr.split("\\t");
			
			interModDiff.set(Double.valueOf(mdwiStrSplit[0]));
			interValue.set(mdwiStrSplit[1] + ","
							+ mdwiStrSplit[2] + ","
							+ mdwiStrSplit[3]);
			
			// interModDiff will be sorted by value in the shuffle and sort phase.
			// So reduce function will receive the <k-v> representing the maximum modularity change first
			context.write(interModDiff, interValue);
		}
	}
	
	public static class MaxModChangeRetrieverReducer
		extends Reducer<DoubleWritable, Text, NullWritable, Text>{
		
		private class MaxModChangeNodePair {
			
			private Text NodeI;
			private Text NodeJ;
			private Double pairEdgeInfluence;
		
			public Text getNodeI() {
				return NodeI;
			}
			
			public Text getNodeJ() {
				return NodeJ;
			}
			
			public double getInfluence() {
				return pairEdgeInfluence;
			}
			
			public void setNodeI(String i) {
				NodeI.set(NodeI == null ? i : this.NodeI.toString());
			}
			
			public void setNodeJ(String j) {
				NodeJ.set(NodeJ == null ? j : this.NodeJ.toString());
			}
			
			public void setInfluence(double inf) {
				pairEdgeInfluence = (pairEdgeInfluence == null) ? inf : this.pairEdgeInfluence;
			}
		}
		
		private Text resultValue = new Text();
		private MaxModChangeNodePair ijPair;
		private String[] infoSeqSplit;
		private Path[] metricFilePath;
		
		// Variables to save values of metrics
		private int MIN_NODE_ID;
		private int MAX_NODE_ID;
		private double TOTAL_INFLUENCE_SUM;
		private String CURRENT_MERGE_I;
		private String CURRENT_MERGE_J;
		
		public void reduce(DoubleWritable modChange, Iterable<Text> infoSequence, Context context)
			throws IOException, InterruptedException {
			
			metricFilePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			BufferedReader br = new BufferedReader(new FileReader(metricFilePath[0].toString()));
			while(br.ready()){
				String tmpLine = br.readLine();
				String[] lineSplit = tmpLine.split(",");
				
				MIN_NODE_ID = Integer.valueOf(lineSplit[0]);
				MAX_NODE_ID = Integer.valueOf(lineSplit[1]);
				TOTAL_INFLUENCE_SUM = Double.valueOf(lineSplit[2]);
				CURRENT_MERGE_I = lineSplit[3];
				CURRENT_MERGE_J = lineSplit[4];
			}
			
			for(Text inf : infoSequence){		
				
				// i,j,IJedgeWeight
				infoSeqSplit = inf.toString().split(",");
				
				// TODO this implementation introduces a lot of redundant operations...
				// very dumb implementation...
				// Should try to use Counters
				
				// this ijPair denotes the node pair with max modularity change,
				// and will only be set once (specifically, set to the first infoSequence that arrives).
				// Executive sets will not change the private field values (ijPair.NodeI & ijPair.NodeJ)
				ijPair.setNodeI(infoSeqSplit[0]);
				ijPair.setNodeJ(infoSeqSplit[1]);
				ijPair.setInfluence(Double.valueOf(infoSeqSplit[2]));
				
				if ( ((infoSeqSplit[0] == ijPair.getNodeI().toString()) || (infoSeqSplit[1] == ijPair.getNodeJ().toString()))
						&& infoSeqSplit[1] != "0") {
					
					resultValue.set(ijPair.getNodeI().toString() + "\t"
									+ infoSeqSplit[1] + "\t"
									+ infoSeqSplit[2]);
					
					context.write(NullWritable.get(), resultValue);
					
				} else if (((infoSeqSplit[0] == ijPair.getNodeI().toString()) || (infoSeqSplit[1] == ijPair.getNodeJ().toString()))
						&& infoSeqSplit[1] == "0") {
					
					resultValue.set(ijPair.getNodeI().toString() + "\t"
									+ "0\t"
									+ infoSeqSplit[2]);
					
					context.write(NullWritable.get(), resultValue);
					
				} else if (infoSeqSplit[2] == ijPair.getNodeJ().toString()) {
					
					resultValue.set(infoSeqSplit[0] + "\t"
									+ ijPair.getNodeI().toString() + "\t"
									+ infoSeqSplit[2]);
					
					context.write(NullWritable.get(), resultValue);
					
				} else if (infoSeqSplit[2] == ijPair.getNodeI().toString()) {
					
					if(Integer.valueOf(infoSeqSplit[0]) < Integer.valueOf(ijPair.getNodeI().toString())) {
						
						resultValue.set(infoSeqSplit[0] + "\t"
										+ ijPair.getNodeI().toString() + "\t"
										+ infoSeqSplit[2]);
					} else {
						
						resultValue.set(ijPair.getNodeI().toString() + "\t"
										+ infoSeqSplit[0] + "\t"
										+ infoSeqSplit[2]);
					}
					
					context.write(NullWritable.get(), resultValue);
					
				} else {
					
					resultValue.set(infoSeqSplit[0] + "\t"
									+ infoSeqSplit[1] + "\t"
									+ infoSeqSplit[2]);
					
					context.write(NullWritable.get(), resultValue);
				}
			}
			
			// Update the metric file
			// (Will be rewritten many times, but the values are actually the same)
			FileWriter fw = new FileWriter("/home/hpds/metricFile.txt");
			String metricInfo = new String(String.valueOf(MIN_NODE_ID) + ","
											+ String.valueOf(MAX_NODE_ID) + ","
											+ String.valueOf(TOTAL_INFLUENCE_SUM) + ","
											+ CURRENT_MERGE_I + ","
											+ CURRENT_MERGE_J);
			fw.write(metricInfo);
			fw.close();
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
		
		job.setJarByClass(SortModularityChange.class);
		job.setMapperClass(ModDiffTokenizerMapper.class);
		job.setReducerClass(MaxModChangeRetrieverReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
