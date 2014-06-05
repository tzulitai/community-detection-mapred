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

public class CalculateModularityChange {
	
	public static class EdgeTokenizerMapper
		extends Mapper<Object, Text, Text, Text>{
		
		private Text interKey = new Text();
		private Text interValue = new Text();
		private String[] inValueSplit;
		
		private static int minNodeId = 101;
		private static int maxNodeId = 64500000;
		
		public void map(Object offset, Text EdgeWithInfluence, Context context)
			throws IOException, InterruptedException {
			String ewiStr = new String(EdgeWithInfluence.toString());
			inValueSplit = ewiStr.split("\\t");
					
			if (inValueSplit[1] != null) {
				
				interKey.set(inValueSplit[0] + "," + inValueSplit[1]);
				interValue.set(inValueSplit[0] + "," + inValueSplit[1] + "," + inValueSplit[2]);
				context.write(interKey, interValue);
				
			} else {
				
				// Loop through all possible node pair permutations 
				for (int nodeCount_1 = minNodeId; nodeCount_1 <= maxNodeId; nodeCount_1++) {
					for (int nodeCount_2 = nodeCount_1 + 1; nodeCount_2 <= maxNodeId; nodeCount_2++){
						
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
		
		private int a = 1;
		private int b = 0;
		
		public void reduce(Text mergeScheme, Iterable<Text> infoSequence, Context context)
			throws IOException, InterruptedException {
			
			interKeySplit = mergeScheme.toString().split(",");
			
			for (Text inf : infoSequence) {
				
				interValueSplit = inf.toString().split(",");
				
				if (interKeySplit[1] != "0"){
					b += Integer.valueOf(interValueSplit[2]);
				} else {
					a *= Integer.valueOf(interValueSplit[2]);
				}
				
				// TODO
			}
		}
	}
}
