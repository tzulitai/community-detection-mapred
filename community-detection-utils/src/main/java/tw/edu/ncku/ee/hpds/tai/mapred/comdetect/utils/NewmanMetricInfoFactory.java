package tw.edu.ncku.ee.hpds.tai.mapred.comdetect.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class NewmanMetricInfoFactory {
	
	private NewmanMetricInfo metricInfo;
	
	public NewmanMetricInfoFactory(){
		metricInfo = new NewmanMetricInfo();
	}
	
	public NewmanMetricInfoFactory(NewmanMetricInfo metricInfo){
		this.metricInfo = metricInfo;
	}
	
	public NewmanMetricInfo getMetricInfo(){
		return metricInfo;
	}
	
	public void readMetricInfoFromHDFS(Path pathToInfoInHDFS, Configuration conf) throws IOException{
		
		try{
			
			FileSystem hdfs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(pathToInfoInHDFS)));
			
			String metricInfoStr;
			metricInfoStr = br.readLine();
			while(metricInfoStr != null){
				
				String[] metricInfoStrSplit = metricInfoStr.split(",");
				metricInfo.update(metricInfoStrSplit[0], 
									metricInfoStrSplit[1], 
									metricInfoStrSplit[2], 
									metricInfoStrSplit[3], 
									metricInfoStrSplit[4]);
			}
			
		} catch(Exception e){
			
		}
	}
	
	public void writeMetricInfoToHDFS(Path pathToInfoInHDFS, Configuration conf){
		
		try{
			
			FileSystem hdfs = FileSystem.get(conf);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(pathToInfoInHDFS, true)));
	
			bw.write(metricInfo.toString());
			bw.close();
			
		} catch(Exception e){
			
		}
	}
}
