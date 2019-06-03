import java.io.IOException;
import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.text.SimpleDateFormat; 
import java.text.ParseException;
import java.text.DateFormat;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class AnalyzeStock extends Configured implements Tool{
		public static void main(String[] args) throws Exception {
					int res = ToolRunner.run(new Configuration(), new AnalyzeStock(), args);
					System.exit(res);
				    }

			

		@Override
    public int run(String[] args) throws Exception{
		

		Configuration conf = this.getConf();
		conf.set("ValueField", args[3]);
		conf.set("AggregationOperator", args[2]);
		conf.set("StartDate", args[0]);
		conf.set("EndDate", args[1]);
		Job myjob = Job.getInstance(conf, "step 1");
		myjob.setJarByClass(AnalyzeStock.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setNumReduceTasks(6);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPaths(myjob,args[4]);
		FileOutputFormat.setOutputPath(myjob,  new Path(args[5]));
		Path outputPath=new Path(args[5]);
		outputPath.getFileSystem(conf).delete(outputPath,true);
		return myjob.waitForCompletion(true) ? 0 : 1;
		
	}







	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private Text tickerName = new Text();
		private String ValueField;
		private String AggregationOperator;
		private String StartDate;
		private String EndDate;

		@Override
	    protected void setup(Context context) {
		Configuration c = context.getConfiguration();
		ValueField = c.get("ValueField");
		AggregationOperator = c.get("AggregationOperator");
		StartDate = c.get("StartDate");
		EndDate = c.get("EndDate");	
	    }
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
				String data = value.toString();
				String[] field = data.split(",");
				double column = 0;
				DateFormat aggregationDate = new SimpleDateFormat("yyyy-MM-dd");
				DateFormat startDate = new SimpleDateFormat("MM/dd/yyyy");
				DateFormat endDate = new SimpleDateFormat("MM/dd/yyyy");
				Date aggregationDt= new Date();
				Date startDt = new Date();
				Date endDt = new Date();
				try {
				aggregationDt=aggregationDate.parse(field[1]);
				startDt=startDate.parse(StartDate);
				endDt=endDate.parse(EndDate);
				
				}
				catch (ParseException ex) {
							    ex.printStackTrace();
							}

				
				if (!aggregationDt.before(startDt) && !aggregationDt.after(endDt))
				{
				if (ValueField.equals("low") )
				{
					if(!field[4].equals(null) && !field[4].isEmpty())
					{column=Double.parseDouble(field[4]);}
					tickerName.set(field[0]);
					context.write(tickerName, new DoubleWritable(column));	
				}
				else if (ValueField.equals("high"))
				{
					if(!field[3].equals(null) && !field[3].isEmpty())
					{column=Double.parseDouble(field[3]);}
					tickerName.set(field[0]);
					context.write(tickerName, new DoubleWritable(column));	
				}
				else if (ValueField.equals("close"))
				{
					if(!field[5].equals(null) && !field[5].isEmpty())
					{column=Double.parseDouble(field[5]);}
					tickerName.set(field[0]);
					context.write(tickerName, new DoubleWritable(column));	
				}
				else
				{
					if(!field[5].equals(null) && !field[5].isEmpty())
					{column=Double.parseDouble(field[5]);}
					tickerName.set(field[0]);
					context.write(tickerName, new DoubleWritable(column));	
				}
				
				}
				
				
				

				
		}
		
	

	
	
}	

	
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
			
			private String AggregationOperator;
			@Override
			    protected void setup(Context context) {
				Configuration c = context.getConfiguration();
				AggregationOperator = c.get("AggregationOperator");	
			    }

		private static double round (double value, int precision) {
					    int scale = (int) Math.pow(10, precision);
					    return (double) Math.round(value * scale) / scale;
					}
			
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		  	
			double max=0;
			double mean=0;
			double count=0;
			double min=Double.MAX_VALUE;
			double sum = 0;
			

			//Iterator<DoubleWritable> iterator = values.iterator(); //Iterating 

			for(DoubleWritable val:values){
			
			
			double value = val.get();
			sum=sum + value;
			if (value > max) { //Finding max value
							     max = value;
							    }
			if (value < min) { //Finding min value
					     min = value;}
			count=count + 1;
			
			}
			mean = sum/count;
			
			max=round(max,2);
			min=round(min,2);
			mean=round(mean,2);
			
			if (AggregationOperator.equals("max"))
			{
				context.write(new Text(key), new DoubleWritable(max));
			}

			if (AggregationOperator.equals("min"))
			{
				context.write(new Text(key), new DoubleWritable(min));
			}
			
			if (AggregationOperator.equals("avg"))
			{
				context.write(new Text(key), new DoubleWritable(mean));
			}

			
			
			}
			
			
	
	
	}
	

}

