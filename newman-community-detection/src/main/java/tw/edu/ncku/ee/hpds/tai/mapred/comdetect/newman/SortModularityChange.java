package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.newman;

import java.io.IOException;
import java.lang.Math;

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
			// So reduce function will recieve the <k-v> representing the maximum modularity change first
			context.write(interModDiff, interValue);
		}
	}
}
