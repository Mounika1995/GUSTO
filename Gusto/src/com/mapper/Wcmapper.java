package com.mapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;



//import com.google.inject.Key;

//import javafx.scene.text.Text;

public class Wcmapper extends Mapper<LongWritable,Text, Text, IntWritable>
{ 
 
	
	@Override
	protected void map(LongWritable key,Text value,Context  context)throws java.io.IOException,InterruptedException
	{
	
	//TODO business Logic-1
	
	//key---0
	//value--DEER BEAR RIVER
	
	long Lkey=key.get();
	final String strvalue=value.toString();
	//strvalue=DEER BEAR RIVER
	
	if(!StringUtils.isEmpty(strvalue))
	{
		final String[] words=StringUtils.splitPreserveAllTokens(strvalue,"");
		
		//word.length=3;
		//words[0]=DEER
		//words[1]=BEAR
		//words[2]=RIVER
		final Text outputWord=new Text();
		final IntWritable ONE=new IntWritable(1);
		for(String word :words) {
			outputWord.set(word);
			context.write(outputWord,ONE);
		}
	}
	}
}
