package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.newman;

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

public class SortModularityChange {
	
	public static class ModDiffTokenizerMapper
		extends Mapper<Object, Text, IntWritable, Text>{
		
		private IntWritable interModDiff = new IntWritable();
		private Text interValue = new Text();
		
		public void map(Object offset, Text modDiffWithInfo, Context context)
			throws IOException, InterruptedException {
			String mdwiStr = new String(modDiffWithInfo.toString());
			String[] mdwiStrSplit = mdwiStr.split("\\t");
			
			interModDiff.set(Integer.valueOf(mdwiStrSplit[0]));
			interValue.set(mdwiStrSplit[1] + ","
							+ mdwiStrSplit[2] + ","
							+ mdwiStrSplit[3]);
			
			// interModDiff will be sorted by value in the shuffle and sort phase.
			// So reduce function will receive the <k-v> representing the maximum modularity change first
			context.write(interModDiff, interValue);
		}
	}
	
	public static class MaxModChangeRetrieverReducer
		extends Reducer<IntWritable, Text, NullWritable, Text>{
		
		private class MaxModChangeNodePair {
			
			private Text NodeI;
			private Text NodeJ;
			
			public Text getNodeI() {
				return NodeI;
			}
			
			public Text getNodeJ() {
				return NodeJ;
			}
			
			public void setNodeI(String i) {
				NodeI.set(NodeI == null ? i : this.NodeI.toString());
			}
			
			public void setNodeJ(String j) {
				NodeJ.set(NodeJ == null ? j : this.NodeJ.toString());
			}
		}
		
		private Text resultValue = new Text();
		private MaxModChangeNodePair ijPair;
		private String[] infoSeqSplit;
		
		public void reduce(IntWritable modChange, Iterable<Text> infoSequence, Context context)
			throws IOException, InterruptedException {
			
			for(Text inf : infoSequence){
				
				// i,j,IJedgeWeight
				infoSeqSplit = inf.toString().split(",");
				
				// this ijPair denotes the node pair with max modularity change,
				// and will only be set once (specifically, set to the first infoSequence that arrives).
				// Executive sets will not change the private field values (ijPair.NodeI & ijPair.NodeJ)
				ijPair.setNodeI(infoSeqSplit[0]);
				ijPair.setNodeJ(infoSeqSplit[1]);
				
				// TODO Use Counters to record the change in t (i.e., edgeWeight of ijPair)
				
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
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: newman-sort-mod-change <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Newman algorithm - Sort Modularity Change");
		job.setJarByClass(SortModularityChange.class);
		job.setMapperClass(ModDiffTokenizerMapper.class);
		job.setReducerClass(MaxModChangeRetrieverReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
