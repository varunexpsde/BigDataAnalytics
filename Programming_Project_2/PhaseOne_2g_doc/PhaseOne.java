import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
        
public class PhaseOne 
{
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
	private final static IntWritable one = new IntWritable(1);
	//private Text key = new Text();
	private Text word = new Text();
	private static String lw=""; 
	        
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	    String filePathString = ((FileSplit)context.getInputSplit()).getPath().getName().toString();
	    String doc = value.toString();
	    String docPart[] = doc.split(" "); //spliting input string to get individual words
	    //String docName = docPart[0]; //getting the document number or the document name
	    String tempStr1=""; //temp string to construct the key part
	    String tempStr2="";
	    String tempStr="";
	    //loop to collect all the words
	    //for loop counter i is starting as we have first element of each line as document number
	    for(int i=0;i<docPart.length-1;i++)
	    {
		tempStr1 = docPart[i].replaceAll("\\p{P}", ""); //removing special character and punctuation from the word
		tempStr2 = docPart[i+1].replaceAll("\\p{P}","");
		if (!tempStr1.isEmpty() && !tempStr2.isEmpty()){
		tempStr = tempStr1+"_"+tempStr2+","+filePathString;
		word.set(tempStr);//converting string to text writable
		context.write(word,one);
		}
	    }
	    //if (!lw.isEmpty())
	    {
		tempStr1 = lw;
		tempStr2 = docPart[0].replaceAll("\\p{P}", "");
		if (!tempStr1.isEmpty() && !tempStr2.isEmpty())
	   	{ 
		     tempStr = tempStr1+"_"+tempStr2+","+filePathString; 
	    	     word.set(tempStr);//converting string to text writable 
		     context.write(word,one); 
		}
	    }
	    lw = docPart[docPart.length-1].replaceAll("\\p{P}", "");
	}
    } 
        
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
	/*private MultipleOutputs<Text, IntWritable> multipleoutputs;

	public void setup(Context context) throws IOException, InterruptedException{
		multipleoutputs = new MultipleOutputs<Text, IntWritable>(context);
	}*/

	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	    throws IOException, InterruptedException
	    {
		int sum = 0;
		for (IntWritable val : values) 
		{
		    sum += val.get();
		}
		//if (sum>1)//to filter those words whose count is less than 1
		{
		context.write(key, new IntWritable(sum));
		}
	    }
	
	/*public void cleanup(Context context) throws IOException, InterruptedException{
		multipleoutputs.close();
	}*/
	
    }
        
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
        
        Job job = new Job(conf, "PhaseOne");
	
	/*FileSystem fs = FileSystem.get(new Configuration());
	fs.delete(new Path("/phase1_res/"), true);*/    

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setJarByClass(PhaseOne.class);
		

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
        
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
	job.waitForCompletion(true);
    }
        
}
