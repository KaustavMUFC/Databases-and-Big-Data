import java.io.IOException;
import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Scanner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class MyWordCount extends Configured implements Tool{
		public static void main(String[] args) throws Exception {
					int res = ToolRunner.run(new Configuration(), new MyWordCount(), args);
					System.exit(res);
				    }

			

		@Override
    public int run(String[] args) throws Exception{
		if(args.length!=4){
					System.err.printf("\n Incorrect Number of Arguments. Please enter all 4 arguments \n");
					System.exit(1);
				  }
		if(Integer.parseInt(args[0])<1){
					System.err.printf("\n Number of Reducers has to be atleast 1 \n");
					System.exit(1);
				  }
		
		if(!("true".equals(args[1]) || "false".equals(args[1]))){
					System.err.printf("\n Case Sensitiveness can be either true or false \n");
					System.exit(1);
				  }


		Configuration conf = this.getConf();
		conf.set("CaseSensitive", args[1]);
		Job myjob = Job.getInstance(conf, "step 1");
		myjob.setJarByClass(MyWordCount.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		int reduce = Integer.parseInt(args[0]);
		myjob.setNumReduceTasks(reduce);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPaths(myjob,args[2]);
		FileOutputFormat.setOutputPath(myjob,  new Path(args[3]));
		Path outputPath=new Path(args[3]);
		outputPath.getFileSystem(conf).delete(outputPath,true);
		return myjob.waitForCompletion(true) ? 0 : 1;
		
	}







	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final LongWritable one = new LongWritable(2);
		String curr;
		private Text word = new Text();
		private String CaseSensitive;
		private Boolean case_sensitive;


		@Override
	    protected void setup(Context context) {
		Configuration c = context.getConfiguration();
		CaseSensitive = c.get("CaseSensitive");
	    }
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			curr = value.toString();
			rmStopWords();
			rmPunt(); 
			case_sensitive=Boolean.parseBoolean(CaseSensitive);
			if (case_sensitive==false) {
        					curr = curr.toLowerCase();
      					    }
			StringTokenizer tok = new StringTokenizer(curr,", .\t()[]{}");
			while (tok.hasMoreTokens()) {
				word.set(tok.nextToken());
				context.write(word, one);
			}
		}
		
	


	public void rmPunt() {
			
		curr = curr.replaceAll("[^a-zA-Z\\s+]", " ");
		curr = curr.trim();
			
		}
	
	public void rmStopWords() throws FileNotFoundException { 
		
		ArrayList<String> stopWords = new ArrayList<String>();
		Scanner inputfile;
		
		File txt_file = new File("/home/cloudera/eng_stopwords.txt"); 
		inputfile = new Scanner (txt_file); 
		
		while(inputfile.hasNext()) {
			String line = inputfile.next(); 
			stopWords.add(line); 
		} 
		
		inputfile.close();
		
		String formattedStr = "";
		for(String word: curr.split("[\\s\\n\\t]")) { 
			
			if(!stopWords.contains(word)) { 
				formattedStr = formattedStr+" "+word; } 
			} 
		curr = formattedStr.trim(); 
	} 
	
	
}	

	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			context.write(key, total);
		}
	}
	
	
	
	

}

