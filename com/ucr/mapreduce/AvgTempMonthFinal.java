package com.ucr.mapreduce;

import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgTempMonth {
	
	// comparing average temperature values in order to sort them
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
	
	// get and set functions for the final output
	public static class MinMaxValue implements WritableComparable<MinMaxValue> {
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
				String minPrec = "MinPrec";
				String maxPrec = "MaxPrec";
				
				String sep = "\t";
				return maxTemp + sep + maxMonth + sep + minTemp + sep + minMonth + sep + difference + sep + minPrec + sep + maxPrec;
			}
			return maxTemperature + "\t" + maxMonth + "\t" + minTemperature + "\t" + minMonth + "\t" + difference + "\t" + minMonthPrec + "\t" + maxMonthPrec;
		}
		
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
	
//	public static class CombineFiles {
//		String[] files = {"/Users/bhavya/Desktop/fall/databases/data/2006.txt","/Users/bhavya/Desktop/fall/databases/data/2007.txt","/Users/bhavya/Desktop/fall/databases/data/2008.txt","/Users/bhavya/Desktop/fall/databases/data/2009.txt"};
//		
//		PrintWriter pw = new PrintWriter("temperature-data.txt");
//		
//		for (String f : files) {
//			BufferedReader br = new BufferedReader(new FileReader(f));
//			String line = "";
//			while ((line = br.readLine()) != null) {
//				pw.print(line);
//			}
//		}
//		br.close();
//		pw.flush();
//		pw.close();
//	}
	
	
	public static class TemperatureMapper extends Mapper<Object, Text, Text, Text> {
		
		HashMap<String, String> stationData = new HashMap<String, String>();
		
		// map station to state and store in stationData
		protected void setup(Context context) throws IOException, InterruptedException {
			//weather stations input file
			URI[] cacheFiles = context.getCacheFiles();
			
			if (cacheFiles != null && cacheFiles.length > 0) {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				Path getFilePath = new Path(cacheFiles[0].toString());
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
				String line = "";
				
				while((line = reader.readLine()) != null) {
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
				}
				reader.close();
			}
		}
		
		// a mapper for temperature files
		// if station is in stationData, then map state::month => temperature
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text outKey = new Text();
			Text outValue = new Text();
			String line = value.toString();
			if(line.indexOf("WBAN") >= 0) {
				return;
			}
			try {
				String[] cols = line.split("\\s+");
				String stnNo = cols[0].replaceAll("\"", "");
				String date = cols[2].replaceAll("\"", "");
				String avgTemp = cols[3].replaceAll("\"", "");
				String precepitation = cols[19].replace("\"", "");
				if(stationData.containsKey(stnNo)) {
					String state = stationData.get(stnNo);
					String month = date.substring(4,6);

					outKey.set(state + "::" + month);
					outValue.set(avgTemp + "::" + precepitation);
					context.write(outKey, outValue); // state::month => temperature::precipitation
				}	
			}
			catch(Exception e) {
				System.out.println("Exception for :: "+ line + " :: " + e.toString());
			}
		}
	}
	
	// get month name from its number
	public static String getMonthInString(int month) {
		String[] months = new String[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
		return months[month-1];
	}
	
	public static class TemperatureCombiner extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text outKey = new Text();
			Text outValue = new Text();
			double totalTemp = 0.0;
			double totalPrec = 0.0;
			double totalCount = 0.0;
			
			String[] stateVsMonth = key.toString().split("::");
			if (stateVsMonth.length < 2) {
				System.out.println("State Vs Month - not suficient data :: " + key.toString());
				return;
			}
			String state = stateVsMonth[0];
			String month = stateVsMonth[1];
			String monthInString = getMonthInString(Integer.parseInt(month));
			for (Text t : values) {
				String[] tempData = t.toString().split("::");
				if(tempData.length < 2) { //temperature and precipitation
					continue;
				}
				Double tempAtState = Double.valueOf(tempData[0]);
				Double prec = Double.valueOf(tempData[1].substring(0, tempData[1].length() - 1));
				
				totalTemp += tempAtState;
				totalPrec += prec;
				totalCount += 1;
			}
			if(totalCount == 0) {
				totalCount++;
			}
			Double averageTempAtState = totalTemp / totalCount;
			Double averagePrec = totalPrec / totalCount;
			
			outKey.set(state);
			outValue.set(averageTempAtState.toString() + "\t" + monthInString + "\t" + averagePrec);
			context.write(outKey, outValue); // state => average temperature	\t month \t precipitation
		}
	}
	
	public static class TemperatureReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void run(Context context) throws IOException, InterruptedException {
			setup(context);
			Text column = new Text("State");
//			Text values = new Text("MaxTemp \t MaxMonth \t MinTemp \t MinMonth \t TempDiff \t MinPrecipitation \t MaxPrecipitation");
			context.write(column, new Text("AvgTemperature \t Month \t AvgPrecipitation"));
			while (context.nextKeyValue()) {
		       reduce(context.getCurrentKey(), context.getValues(), context);
		    }
			cleanup(context);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//sample input -> AK	324.0::24.0
			Text finalKey = new Text();
			Text finalValue = new Text();
			for(Text t : values) {
				finalKey.set(key);
				finalValue.set(t);
				context.write(finalKey, finalValue); // state => average temperature \t month \t precipitation
			}
		}
	}
	
	public static class MinMaxMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//AK	8.46182197828121, January, 0.08
			Text outKey = new Text();
			Text outValue = new Text();
			String line = value.toString();
			String[] cols = line.split("\\s+");
			String valueStr = "";
			for(int i = 1; i < cols.length; i++) {
				valueStr += cols[i] + "\t";
			}
			
			outKey.set(cols[0]);
			outValue.set(valueStr);
			context.write(outKey, outValue); // state => average temperature \t month \t precipitation
		}
	}
	
	public static class MinMaxReducer extends Reducer<Text, Text, Text, MinMaxValue> {
		
		HashMap<String, MinMaxValue> map = new HashMap<String, MinMaxValue>();
		
		@Override
		public void run(Context context) throws IOException, InterruptedException {
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
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Double maxTemp = Double.MIN_VALUE;
			Double minTemp = Double.MAX_VALUE;
			String minMonth = "";
			String maxMonth = "";
//			Double averagePrecipitation = 0.0;
			double minPrec = 0.0;
			double maxPrec = 0.0;
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
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Text outKey = new Text();
			List<MinMaxValue> list = new ArrayList<MinMaxValue>(map.values());
			Collections.sort(list, new MinMaxComparator());
			for(MinMaxValue v : list) {
				if(v.getState().equals("State")) {
					continue;
				}
				outKey.set(v.getState());
				context.write(outKey, v);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		String inputPathFile = "/input_dir/WeatherStationLocations.csv";
		String stateTempOutputFile = "/states_temperature";
		String minMaxTempOutputFile = "/min_max_temperature";
		
		//delete existing output files before running
//		Configuration deleteConf = new Configuration();
//		deleteConf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//		deleteConf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
//	    FileSystem  hdfs = FileSystem.get(URI.create("hdfs://localhost:9820"), deleteConf);
//	    hdfs.delete(new Path(stateTempOutputFile), true);
//	    hdfs.delete(new Path(minMaxTempOutputFile), true);
		
	    //job to get average temperature for each states
		Long job1StartTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AverageTemperatureByStates");
		
		job.setJarByClass(AvgTempMonth.class);
		job.setMapperClass(TemperatureMapper.class);
		
		//adding the weather stations csv file in distributed cache
		job.addCacheFile(new URI(inputPathFile));
		job.setCombinerClass(TemperatureCombiner.class);
		job.setReducerClass(TemperatureReducer.class);
//		job.setCombinerClass(TemperatureReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path("/input_dir/2007.txt"), TextInputFormat.class, TemperatureMapper.class);
		MultipleInputs.addInputPath(job, new Path("/input_dir/2006.txt"), TextInputFormat.class, TemperatureMapper.class);
		MultipleInputs.addInputPath(job, new Path("/input_dir/2008.txt"), TextInputFormat.class, TemperatureMapper.class);
		MultipleInputs.addInputPath(job, new Path("/input_dir/2009.txt"), TextInputFormat.class, TemperatureMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(stateTempOutputFile));
		
		System.out.print("Job 1 completed in :: " + (System.currentTimeMillis() - job1StartTime));
		job.waitForCompletion(true);
		
		Long job2StartTime = System.currentTimeMillis();
		Configuration confMinMax = new Configuration();
		
		//job to get min amd max temperature by month
		Job minMaxJob = Job.getInstance(confMinMax, "MinMaxTemperature");
		minMaxJob.setJarByClass(AvgTempMonth.class);
		minMaxJob.setMapperClass(MinMaxMapper.class);
		
		minMaxJob.setReducerClass(MinMaxReducer.class);
//		minMaxJob.setCombinerClass(MinMaxReducer.class);
		minMaxJob.setMapOutputKeyClass(Text.class);
		minMaxJob.setMapOutputValueClass(Text.class);
		minMaxJob.setOutputKeyClass(Text.class);
		minMaxJob.setOutputValueClass(MinMaxValue.class);
//		minMaxJob.setSortComparatorClass(MinMaxComparator.class);
		
		FileInputFormat.addInputPath(minMaxJob, new Path(stateTempOutputFile));
		FileOutputFormat.setOutputPath(minMaxJob, new Path(minMaxTempOutputFile));		
		
		if (minMaxJob.waitForCompletion(true)) {
			System.out.print("Job 2 completed in :: " + (System.currentTimeMillis() - job2StartTime));
			System.exit(0);
		} else {
			System.exit(1);
		}
//		System.exit(minMaxJob.waitForCompletion(true) ? 0 : 1);
	}
}
