package com.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

//import java.math.BigDecimal;
	//import java.math.BigInteger;
	//import java.nio.file.Path;
	//import java.util.Iterator;
	//import java.util.List;
	//import java.util.Properties;

	//import javax.tools.Tool;

//	import org.apache.commons.configuration.Configuration;
	//import org.apache.hadoop.conf.Configured;
	import org.apache.hadoop.io.IntWritable;
	//import org.apache.hadoop.mapred.TextInputFormat;
	//import org.apache.hadoop.mapred.TextOutputFormat;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.util.ToolRunner;

import com.mapper.Wcmapper;
import com.reducer.Wcreducer;

//import com.mapper.Wcmapper;
	//import com.reducer.Wcreducer;




	//package com.WC.Driver;

	//import org.apache.hadoop.conf.Configuration;
	//import org.apache.hadoop.conf.Configured;
	//import org.apache.hadoop.fs.Path;
	//import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	//import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	import org.apache.hadoop.util.Tool;
	//import org.apache.hadoop.util.ToolRunner;

	//import com.WC.Mapper.*;
	//import com.WC.Reducer.*;


	//public class Wcdriver extends Configured implements {
	public class Wcdriver extends Configured implements Tool {
		
	       public static void main( String[] args)
	{
		//main()
		//step-0;
		//validating the input arguments
		if(args.length<2)
		{
			System.out.println("java usage"+Wcdriver.class.getName()+"[config]/hdfs/path/to/input /hdfs/path/to/output");
			/*system.out.println("java ussage:"+Wcdriver.class.getName()+"/home/cloudera/txns.txt /hdfs/cloudera/TextFileOP");
			 /*System.class.println("java ussage:"+Wcdriver.class.getName()+"file:///D:txns.txt/user/cloudera/TextFileOP");
			 */
			 return;
		
		}
		//loading the configuration create conf object
		Configuration conf=new Configuration(Boolean.TRUE);
     //  org.apache.hadoop.conf.Configuration conf =new configuration(Boolean.TRUE)			
			
			
			
		//call ToolRunner generic option Paser parsing command line arguments and set the in configuration
		try 
		{
			//int i=ToolRunner.run(conf,(Tool) new Wcdriver(), args); 
	          int i=ToolRunner.run(conf,(Tool) new Wcdriver(), args);
			
			if(i==0)
			{
				System.out.println("SUCCESS");
			}
			else {
				System.out.println("FAILURE");
				}
		}catch(Exception e) {
			System.out.println("FAILURE");
			e.printStackTrace();
		}
	}
	       
	       
	       //run()
	       @Override

	public int run(String[]args)throws Exception
	{
		
		//step-1
		//Get the configuration object
		Configuration conf=super.getConf();
		
		
		
		//step-2
		//set configuration parameter to configuration
		//create job instance toaccess cluser environment
		//create wordcountjob instance &calling conf & main class(wcdriver.class.getNmae())
		
		
	
		Job wordCountJob=Job.getInstance(conf, Wcdriver.class.getName());	
		//set jar class
		//wordcountJob.setJarByClass(Wcdriver.class);
		
		wordCountJob.setJarByClass(Wcdriver.class);
		
		//setting input
		
		final String input=args[0];
		
		
		//convert string into URL;
	//	final Path hdfsInputPath=new Path(input);
		final Path hdfsInputPath=new Path(input);
		
		
		
		//will be useful for computing input splits jobsubmitter/application master
		//Data type we declaring here text input format.
		
		
		//TextInputFormat.addInputPath(wordCountJob,hdfsInputPath);
		//wordcountJob.setInputFormatclass(TextInputFormat.class);
		
	TextInputFormat.addInputPath(wordCountJob, hdfsInputPath);
	wordCountJob.setInputFormatClass(TextInputFormat.class);
		
		
		
		
		
		//setting output
		final String output=args[1];
		
		
		//convert string into URI
		final Path hdfsOutputPath=new Path(output);
		
		
		//setting output format
		//output format
	    TextOutputFormat.setOutputPath(wordCountJob, hdfsOutputPath);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		
		//setting mapper
		
		wordCountJob.setMapperClass(Wcmapper.class);
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		
		
		//setting reducer
		//wordCountJob.setReducerClass(Wcreducer.class);
		
          wordCountJob.setReducerClass(Wcreducer.class);
		  wordCountJob.setOutputKeyClass(Text.class);
		  wordCountJob.setOutputValueClass(IntWritable.class);
		
		
		
		//trigger method
		wordCountJob.waitForCompletion(Boolean.TRUE);	
		return 0;
	}
	}


