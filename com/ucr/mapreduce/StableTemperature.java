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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
	
//	public static class MinMaxValueColumn extends MinMaxValue{
//		String maxTemp = "MaxTemp";
//		String maxMonth = "MaxMonth";
//		String minMonth = "MinMonth";
//		String minTemp = "MinTemp";
//		String difference = "TempDifference";
//		String minPrec = "MinPrec";
//		String maxPrec = "MaxPrec";
//		
//		String sep = "\t";
//		
//		public String toString() {
//			return maxTemp + sep + maxMonth + sep + minTemp + sep + minMonth + sep + difference + sep + minPrec + sep + maxPrec;
//		}
//	}
	
	public static class MinMaxValue implements WritableComparable<MinMaxValue>{

		Double minTemperature;
		Double maxTemperature;
		String minMonth;
		String maxMonth;
		Double difference;
		String state;
		Double maxMonthPrec;
		public Double getMaxMonthPrec() {
			return maxMonthPrec;
		}

		public void setMaxMonthPrec(Double maxMonthPrec) {
			this.maxMonthPrec = maxMonthPrec;
		}

		public Double getMinMonthPrec() {
			return minMonthPrec;
		}

		public void setMinMonthPrec(Double minMonthPrec) {
			this.minMonthPrec = minMonthPrec;
		}

		Double minMonthPrec;
		
		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}

		@Override
		public String toString() {
			if(state == null) {
				String maxTemp = "MaxTemp";
				String maxMonth = "MaxMonth";
				String minMonth = "MinMonth";
				String minTemp = "MinTemp";
				String difference = "TempDifference";
				String minPrec = "MinMonthPrec";
				String maxPrec = "MaxMonthPrec";
				
				String sep = "\t";
				return maxTemp + sep + maxMonth + sep + minTemp + sep + minMonth + sep + difference + sep + minPrec + sep + maxPrec;
			}
			
			return format(maxTemperature) + "\t" + maxMonth + "\t" + format(minTemperature) + "\t" + minMonth + "\t" + format(difference) + "\t" + format(minMonthPrec) + "\t" + format(maxMonthPrec);
		}
		
		String format(Double d) {
			return String.format("%.2f", d);
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
			out.writeDouble(minMonthPrec);
			out.writeDouble(maxMonthPrec);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			minTemperature = in.readDouble();
			maxTemperature = in.readDouble();
			maxMonth = in.readUTF();
			minMonth = in.readUTF();
			difference = in.readDouble();
			state = in.readUTF();
			minMonthPrec = in.readDouble();
			maxMonthPrec = in.readDouble();
		}

		@Override
		public int compareTo(MinMaxValue o) {
			return o.getDifference().compareTo(difference);
		}
		
	}
	
	public static class TemperatureMapper extends Mapper<Object, Text, Text, Text>{
		
		HashMap<String, String> stationData = new HashMap();
		
		protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			//weather stations input file
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
				String precepitation = cols[19].replace("\"", "");
				if(stationData.containsKey(stnNo)) {
					String state = stationData.get(stnNo);
					String month = date.substring(4,6);
					String stateVsMonth = state+"::"+month;
					String tempVsCount = avgTemp+"::"+count;
					String outValue = tempVsCount + "::" + precepitation;
//					context.write(new Text(stateVsMonth), new Text(tempVsCount));
					context.write(new Text(stateVsMonth), new Text(outValue));
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
	
	public static class TemperatureCombiner extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//sample input -> AK	324.0::24.0
			double totalTemp = 0.0;
			double totalPrec = 0.0;
			double totalCount = 0.0;
			String allValues = "";
			String[] stateVsMonth = key.toString().split("::");
			if(stateVsMonth.length < 2) {
				System.out.println("State Vs Month - not suficient data :: " + key.toString());
				return;
			}
			String state = stateVsMonth[0];
			String month = stateVsMonth[1];
			String monthInString = getMonthInString(Integer.parseInt(month));
			for(Text t : values) {
				String[] tempData = t.toString().split("::");
				if(tempData.length < 3) {//temp, count and precipitation
					continue;
				}
				Double tempAtState = Double.valueOf(tempData[0]);
				Double count = Double.valueOf(tempData[1]);
				Double prec = Double.valueOf(tempData[2].substring(0, tempData[2].length()-1));
				
				totalPrec += prec;
				
				totalCount += count;
				totalTemp += tempAtState * count;
			}
			if(totalCount == 0) {
				totalCount++;
			}
			Double averageTempAtState = totalTemp / totalCount;
			Double averagePrec = totalPrec / totalCount;
//			System.out.println("Computing for "+key+" -> " + totalTemp + " / " + totalCount + " = " + String.valueOf(averageTempAtState));
			context.write(new Text(state), new Text(averageTempAtState.toString()+"\t"+monthInString + "\t" + averagePrec));
		}
	}
	
	public static class TemperatureReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		public void run(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			setup(context);
			Text column = new Text("State");
//			Text values = new Text("MaxTemp \t MaxMonth \t MinTemp \t MinMonth \t TempDiff \t MinPrecipitation \t MaxPrecipitation");
			context.write(column, new Text("AvgTemperature \t Month \t AvgPrecipitation"));
			while (context.nextKeyValue()) {
		       reduce(context.getCurrentKey(), context.getValues(), context);
		    }
			cleanup(context);
		}
		
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//sample input -> AK	324.0::24.0
			for(Text t : values) {
				context.write(key, t);
			}
		}
	}
	
	public static class MinMaxMapper extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//AK	8.46182197828121, January, 0.08
			String line = value.toString();
			String[] cols = line.split("\\s+");
			String valueStr = "";
			for(int i=1;i<cols.length;i++) {
				valueStr += cols[i] + "\t";
			}
			context.write(new Text(cols[0]), new Text(valueStr));
		}
	}
	
	public static class MinMaxReducer extends Reducer<Text, Text, Text, MinMaxValue>{
		
		HashMap<String, MinMaxValue> map = new HashMap();
		
		@Override
		public void run(Reducer<Text, Text, Text, MinMaxValue>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
//			super.run(context);
			setup(context);
			Text column = new Text("State");
//			Text values = new Text("MaxTemp \t MaxMonth \t MinTemp \t MinMonth \t TempDiff \t MinPrecipitation \t MaxPrecipitation");
			context.write(column, new MinMaxValue());
			while (context.nextKeyValue()) {
		       reduce(context.getCurrentKey(), context.getValues(), context);
		    }
			cleanup(context);
		}
		
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, MinMaxValue>.Context context)
				throws IOException, InterruptedException {
			Double maxTemp = Double.MIN_VALUE;
			Double minTemp = Double.MAX_VALUE;
			String minMonth = "";
			String maxMonth = "";
//			Double averagePrecipitation = 0.0;
			double minPrec = Double.MAX_VALUE;
			double maxPrec = Double.MIN_VALUE;
//			Double totalTemp = 0.0;
//			Double totalCount = 0.0;
			for(Text t : values) {
//				if(t.toString().indexOf("State") >= 0) {
//					continue;
//				}
//				System.out.println("*****"+t.toString());
				String[] tempVsMonth = t.toString().split("\\s+");
				try {
					Double temp = Double.valueOf(tempVsMonth[0]);
					Double prec = Double.valueOf(tempVsMonth[2]);
//					totalTemp += temp;
//					totalCount++;
					if(temp > maxTemp) {
						maxTemp = temp;
						maxMonth = tempVsMonth[1];
						maxPrec = Double.valueOf(tempVsMonth[2]);
					}
					if(temp < minTemp) {
						minTemp = temp;
						minMonth = tempVsMonth[1];
						minPrec = Double.valueOf(tempVsMonth[2]);
					}
				}
				catch(Exception e) {
					
				}
				
			}
//			averagePrecipitation = (minPrec + maxPrec) / 2;
//			Double avgTemp = totalTemp/totalCount;
			MinMaxValue outputValue = new MinMaxValue();
			outputValue.setMaxMonth(maxMonth);
			outputValue.setMinMonth(minMonth);
			outputValue.setMaxTemperature(maxTemp);
			outputValue.setMinTemperature(minTemp);
			outputValue.setDifference(maxTemp - minTemp);
			outputValue.setState(key.toString());
			outputValue.setMinMonthPrec(minPrec);
			outputValue.setMaxMonthPrec(maxPrec);
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
				if(v.getState().equals("State")) {
					continue;
				}
				context.write(new Text(v.getState()), v);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		
//		String inputPathFile = "/input_dir/WeatherStationLocations.csv";
		String inputPathFile = args[0];
		String outputFilePathName = args[2];
		String stateTempOutputFile = "/states_temperature";
		String minMaxTempOutputFile = "/min_max_temparature";
		
		//delete existing output files before running
		Configuration deleteConf = new Configuration();
		deleteConf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		deleteConf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
	    FileSystem  hdfs = FileSystem.get(URI.create("hdfs://localhost:9820"), deleteConf);
	    hdfs.delete(new Path(outputFilePathName+stateTempOutputFile), true);
	    hdfs.delete(new Path(outputFilePathName + minMaxTempOutputFile), true);
		
	    //job to get average temperature for each states
		Long job1StartTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AverageTemperatureByStates");
		
		job.setJarByClass(StableTemperature.class);
		job.setMapperClass(TemperatureMapper.class);
		
		//adding the weather statations csv file in distributed cache
		DistributedCache.addCacheFile(new URI(inputPathFile), job.getConfiguration());
		
		job.setReducerClass(TemperatureReducer.class);
		job.setCombinerClass(TemperatureCombiner.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		String temperatureFileDir = args[1];
		FileSystem fs = FileSystem.get(conf);
		
		

		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(temperatureFileDir), true);
		
		 while(fileStatusListIterator.hasNext()){
		        LocatedFileStatus fileStatus = fileStatusListIterator.next();
		        //do stuff with the file like ...
		        MultipleInputs.addInputPath(job, fileStatus.getPath(), TextInputFormat.class, TemperatureMapper.class);
		    }
		
//		MultipleInputs.addInputPath(job, new Path("/input_dir/2007.txt"), TextInputFormat.class, TemperatureMapper.class);
//		MultipleInputs.addInputPath(job, new Path("/input_dir/2006.txt"), TextInputFormat.class, TemperatureMapper.class);
//		MultipleInputs.addInputPath(job, new Path("/input_dir/2008.txt"), TextInputFormat.class, TemperatureMapper.class);
//		MultipleInputs.addInputPath(job, new Path("/input_dir/2009.txt"), TextInputFormat.class, TemperatureMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(outputFilePathName+stateTempOutputFile));
		
		System.out.print("Job 1 completed in :: " + (System.currentTimeMillis() - job1StartTime));
		job.waitForCompletion(true);
		
		Long job2StartTime = System.currentTimeMillis();
		Configuration confMinMax = new Configuration();
		
		//job to get min amd max temperature by month
		Job minMaxJob = Job.getInstance(confMinMax, "MinMaxTemperature");
		minMaxJob.setJarByClass(StableTemperature.class);
		minMaxJob.setMapperClass(MinMaxMapper.class);
		
		minMaxJob.setReducerClass(MinMaxReducer.class);
//		minMaxJob.setCombinerClass(MinMaxReducer.class);
		minMaxJob.setInputFormatClass(KeyValueTextInputFormat.class);
		minMaxJob.setMapOutputKeyClass(Text.class);
		minMaxJob.setMapOutputValueClass(Text.class);
		minMaxJob.setOutputKeyClass(Text.class);
		minMaxJob.setOutputValueClass(MinMaxValue.class);
//		minMaxJob.setSortComparatorClass(MinMaxComparator.class);
		
		
		FileInputFormat.addInputPath(minMaxJob, new Path(outputFilePathName+stateTempOutputFile));
		FileOutputFormat.setOutputPath(minMaxJob, new Path(outputFilePathName+minMaxTempOutputFile));
		
		System.out.print("Job 2 completed in :: " + (System.currentTimeMillis() - job2StartTime));
		
		System.exit(minMaxJob.waitForCompletion(true) ? 0 : 1);
		
	}

}