package com.ucr.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AvgTempMonth {
	
	public static class AvgTempMonthMapper extends Mapper<Object, Text, Text, Text>{
		
		HashMap<String, String> temperatureData = new HashMap();
		
		protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p : localCacheFiles) {
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				String line = reader.readLine();
				while(line != null) {
					if(line.indexOf("STN") < 0) {
						String[] cols = line.split("\\s+");
						String stnNo = cols[0];
						String wban = cols[1];
						String date = cols[2];
						String month = date.substring(4,6);
						String temp = cols[3];
						String count = cols[4];
//						for(int i=0;i<cols.length;i++) {
//							System.out.println("For index "+ i + " = " + cols[i]);
//						}
						if(!temp.isEmpty() && !count.isEmpty()) {
							temperatureData.put(stnNo, temp+":"+count+":"+month);
						}
					}

					line = reader.readLine();
				}
				reader.close();
			}
		}
		
		public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = key.toString();
			if(line.indexOf("USAF") >= 0) {
				return;
			}
			try {
				String[] cols = line.split(",");
				if(cols.length < 5) {
					return;
				}
				String stnNo = cols[0].replaceAll("\"", "");
				String wban = cols[1].replaceAll("\"", "");
				String country = cols[3].replaceAll("\"", "");
				if(country.equals("US")) {
					String state = cols[4].replaceAll("\"", "");
					if(!state.isEmpty()) {
						if(temperatureData.containsKey(stnNo)) {
							try {
								String[] tempData = temperatureData.get(stnNo).split(":");
								Double averageTemp = Double.valueOf(tempData[0]);
								Double count = Double.valueOf(tempData[1]);
								String month = tempData[2];
								Double totalTemp = averageTemp * count;
								String outputValue = String.valueOf(totalTemp) + "=" + String.valueOf(count);
								context.write(new Text(state+":"+month), new Text(outputValue));
							}
							catch(Exception e) {
								System.out.println("Exception in calc :: "+ temperatureData.get(stnNo) + " :: " + e.toString());
							}
							
						}
					}
				}
				
			}
			catch(Exception e) {
				System.out.println("Exception for :: "+ line + " :: " + e.toString());
			}
		}
	}
	
	public static class AvgTempMonthReducer extends Reducer<Text, Text, Text, DoubleWritable>{
		
		private static String getMonthInString(int month) {
			String[] months = new String[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
			return months[month-1];
		}
		
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			//AK	324.0::24.0
			double totalTemp = 0.0;
			double totalCount = 0.0;
			String allValues = "";
			String[] stateVsMonth = key.toString().split(":");
			String state = stateVsMonth[0];
			String month = stateVsMonth[1];
			String monthInString = getMonthInString(Integer.parseInt(month));
			for(Text t : values) {
				allValues += t.toString() + "---";
				String[] tempData = t.toString().split("=");
				if(tempData.length < 2) {
//					System.out.println("Data not sufficient :: " + t.toString());
					continue;
				}
				Double tempAtState = Double.valueOf(tempData[0]);
				Double count = Double.valueOf(tempData[1]);
				totalCount += count;
				totalTemp += tempAtState;
			}
			if(totalCount == 0) {
				totalCount++;
			}
//			System.out.println("Computing for "+key+" = " + totalTemp + " / " + totalCount);
			Double averageTempAtState = totalTemp / totalCount;
//			System.out.println("Computing for "+key+" -> " + totalTemp + " / " + totalCount + " = " + String.valueOf(averageTempAtState));
//			context.write(key, new Text(averageTempAtState.toString()));
			context.write(new Text(state+" "+monthInString), new DoubleWritable(averageTempAtState));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Join");
		
		job.setJarByClass(AvgTempMonth.class);
		job.setMapperClass(AvgTempMonthMapper.class);
		
		DistributedCache.addCacheFile(new URI("/input_dir/2006.txt"), job.getConfiguration());
		DistributedCache.addCacheFile(new URI("/input_dir/2007.txt"), job.getConfiguration());
		DistributedCache.addCacheFile(new URI("/input_dir/2008.txt"), job.getConfiguration());
		DistributedCache.addCacheFile(new URI("/input_dir/2009.txt"), job.getConfiguration());
		
		job.setReducerClass(AvgTempMonthReducer.class);
//		job.setCombinerClass(AvgTempMonthReducer.class);
//		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
//		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
//		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AvgTempMapper.class);
//		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TemperatureMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}