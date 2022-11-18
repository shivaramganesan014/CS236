package com.ucr.mapreduce;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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

public class StableTemperature {
	
	public static class MinMaxComparator extends WritableComparator{
		public MinMaxComparator() {
			super(MinMaxValue.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			MinMaxValue v1 = (MinMaxValue)w1;
			MinMaxValue v2 = (MinMaxValue)w2;
			return v2.compareTo(v1);
		}
	}
	
	public static class MinMaxValue implements WritableComparable<MinMaxValue>{

		Double minTemperature;
		Double maxTemperature;
		String minMonth;
		String maxMonth;
		Double difference;
		String state;
		
		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}

		@Override
		public String toString() {
			return maxTemperature + ", " + maxMonth + " :: " + minTemperature + ", " + minMonth + " :: " + difference;
		}
		
//		public MinMaxValue() {
//
//		}
		
		public Double getDifference() {
			return difference;
		}

		public void setDifference(Double difference) {
			this.difference = difference;
		}

		public Double getMinTemperature() {
			return minTemperature;
		}

		public void setMinTemperature(Double minTemperature) {
			this.minTemperature = minTemperature;
		}

		public Double getMaxTemperature() {
			return maxTemperature;
		}

		public void setMaxTemperature(Double maxTemperature) {
			this.maxTemperature = maxTemperature;
		}

		public String getMinMonth() {
			return minMonth;
		}

		public void setMinMonth(String minMonth) {
			this.minMonth = minMonth;
		}

		public String getMaxMonth() {
			return maxMonth;
		}

		public void setMaxMonth(String maxMonth) {
			this.maxMonth = maxMonth;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(this.minTemperature);
			out.writeDouble(this.maxTemperature);
			out.writeUTF(maxMonth);
			out.writeUTF(minMonth);
			out.writeDouble(difference);
			out.writeUTF(state);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			minTemperature = in.readDouble();
			maxTemperature = in.readDouble();
			maxMonth = in.readUTF();
			minMonth = in.readUTF();
			difference = in.readDouble();
			state = in.readUTF();
		}

		@Override
		public int compareTo(MinMaxValue o) {
			return o.getDifference().compareTo(difference);
		}
		
	}
	
	public static class TemperatureMapper extends Mapper<Object, Text, Text, Text>{
		
		HashMap<String, String> stationData = new HashMap();
		
		protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p : localCacheFiles) {
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				String line = reader.readLine();
				while(line != null) {
					if(line.indexOf("USAF") < 0) {
						String[] cols = line.split(",");
						if(cols.length >= 5) {
							String stnNo = cols[0].replaceAll("\"", "");
							String country = cols[3].replaceAll("\"", "");
							String state = cols[4].replaceAll("\"", "");
							if(country.equals("US") && !state.isEmpty()) {
								stationData.put(stnNo, state);
							}
						}
					}
					line = reader.readLine();
				}
				reader.close();
			}
		}
		
		public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if(line.indexOf("WBAN") >= 0) {
				return;
			}
			try {
				String[] cols = line.split("\\s+");
				String stnNo = cols[0].replaceAll("\"", "");
				String date = cols[2].replaceAll("\"", "");
				String avgTemp = cols[3].replaceAll("\"", "");
				String count = cols[4].replaceAll("\"", "");
				if(stationData.containsKey(stnNo)) {
					String state = stationData.get(stnNo);
					String month = date.substring(4,6);
					String stateVsMonth = state+"::"+month;
					String tempVsCount = avgTemp+"::"+count;
					context.write(new Text(stateVsMonth), new Text(tempVsCount));
				}
				
			}
			catch(Exception e) {
				System.out.println("Exception for :: "+ line + " :: " + e.toString());
			}
		}
	}
	
	public static String getMonthInString(int month) {
		String[] months = new String[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
		return months[month-1];
	}
	
	public static class TemperatureReducer extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//AK	324.0::24.0
			double totalTemp = 0.0;
			double totalCount = 0.0;
			String allValues = "";
			String[] stateVsMonth = key.toString().split("::");
			String state = stateVsMonth[0];
			String month = stateVsMonth[1];
			String monthInString = getMonthInString(Integer.parseInt(month));
			for(Text t : values) {
				String[] tempData = t.toString().split("::");
				if(tempData.length < 2) {
					continue;
				}
				Double tempAtState = Double.valueOf(tempData[0]);
				Double count = Double.valueOf(tempData[1]);
				totalCount += count;
				totalTemp += tempAtState * count;
			}
			if(totalCount == 0) {
				totalCount++;
			}
			Double averageTempAtState = totalTemp / totalCount;
//			System.out.println("Computing for "+key+" -> " + totalTemp + " / " + totalCount + " = " + String.valueOf(averageTempAtState));
			context.write(new Text(state), new Text(averageTempAtState.toString()+", "+monthInString));
		}
	}
	
	public static class MinMaxMapper extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//AK	8.46182197828121, January
			String line = value.toString();
			String[] cols = line.split("\\s+");
			context.write(new Text(cols[0]), new Text(cols[1]));
		}
	}
	
	public static class MinMaxReducer extends Reducer<Text, Text, Text, MinMaxValue>{
		
		HashMap<String, MinMaxValue> map = new HashMap();
		
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, MinMaxValue>.Context context)
				throws IOException, InterruptedException {
			Double maxTemp = Double.MIN_VALUE;
			Double minTemp = Double.MAX_VALUE;
			String minMonth = "";
			String maxMonth = "";
//			Double totalTemp = 0.0;
//			Double totalCount = 0.0;
			for(Text t : values) {
				String[] tempVsMonth = t.toString().split(",");
				Double temp = Double.valueOf(tempVsMonth[0]);
//				totalTemp += temp;
//				totalCount++;
				if(temp > maxTemp) {
					maxTemp = temp;
					maxMonth = tempVsMonth[1];
				}
				if(temp < minTemp) {
					minTemp = temp;
					minMonth = tempVsMonth[1];
				}
			}
//			Double avgTemp = totalTemp/totalCount;
			MinMaxValue outputValue = new MinMaxValue();
			outputValue.setMaxMonth(maxMonth);
			outputValue.setMinMonth(minMonth);
			outputValue.setMaxTemperature(maxTemp);
			outputValue.setMinTemperature(minTemp);
			outputValue.setDifference(maxTemp - minTemp);
			outputValue.setState(key.toString());
//			String outValue = maxTemp.toString() + ", " + maxMonth + " " + minTemp.toString() + ", " + minMonth + " " + (maxTemp - minTemp);
//			System.out.println(outValue);
//			context.write(key, outputValue);
			map.put(key.toString(), outputValue);
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, MinMaxValue>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<MinMaxValue> list = new ArrayList(map.values());
			Collections.sort(list, new MinMaxComparator());
			for(MinMaxValue v : list) {
				context.write(new Text(v.getState()), v);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		
		String inputPathFile = "/input_dir/WeatherStationLocations.csv";
		String stateTempOutputFile = "/states_temperature";
		String minMaxTempOutputFile = "/min_max_temparature";
		
		Configuration deleteConf = new Configuration();
		deleteConf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		deleteConf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
	    FileSystem  hdfs = FileSystem.get(URI.create("hdfs://localhost:9820"), deleteConf);
	    hdfs.delete(new Path(stateTempOutputFile), true);
	    hdfs.delete(new Path(minMaxTempOutputFile), true);
		
		Long job1StartTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MapJoin");
		
		job.setJarByClass(StableTemperature.class);
		job.setMapperClass(TemperatureMapper.class);
		
		DistributedCache.addCacheFile(new URI(inputPathFile), job.getConfiguration());
		
		job.setReducerClass(TemperatureReducer.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path("/input_dir/2007.txt"), TextInputFormat.class, TemperatureMapper.class);
		MultipleInputs.addInputPath(job, new Path("/input_dir/2006.txt"), TextInputFormat.class, TemperatureMapper.class);
		MultipleInputs.addInputPath(job, new Path("/input_dir/2008.txt"), TextInputFormat.class, TemperatureMapper.class);
		MultipleInputs.addInputPath(job, new Path("/input_dir/2009.txt"), TextInputFormat.class, TemperatureMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(stateTempOutputFile));
		
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.print("Job 1 completed in :: " + (System.currentTimeMillis() - job1StartTime));
		job.waitForCompletion(true);
		Long job2StartTime = System.currentTimeMillis();
		Configuration confMinMax = new Configuration();
		Job minMaxJob = Job.getInstance(confMinMax, "MinMaxJob");
		minMaxJob.setJarByClass(StableTemperature.class);
		minMaxJob.setMapperClass(MinMaxMapper.class);
		
		minMaxJob.setReducerClass(MinMaxReducer.class);
		minMaxJob.setInputFormatClass(KeyValueTextInputFormat.class);
		minMaxJob.setMapOutputKeyClass(Text.class);
		minMaxJob.setMapOutputValueClass(Text.class);
		minMaxJob.setOutputKeyClass(Text.class);
		minMaxJob.setOutputValueClass(MinMaxValue.class);
//		minMaxJob.setSortComparatorClass(MinMaxComparator.class);
		
		
		FileInputFormat.addInputPath(minMaxJob, new Path(stateTempOutputFile));
		FileOutputFormat.setOutputPath(minMaxJob, new Path(minMaxTempOutputFile));
		
		System.out.print("Job 2 completed in :: " + (System.currentTimeMillis() - job2StartTime));
		
		System.exit(minMaxJob.waitForCompletion(true) ? 0 : 1);
		
	}

}