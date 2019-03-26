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
        
public class PhaseThree 
{
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
	private final static IntWritable one = new IntWritable(1);
	private Text outKey = new Text();
        
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	    String inputLine = value.toString(); //input is coming from the output file from phase one
	    String temp[] = inputLine.split("\t"); //spliting input string to get pair of word,document name and frequency
	    //int wordCntr = Integer.parseInt(temp[1]);//getting word frequency
	    String docPart[]=temp[0].split(",");//seperating document name and word
	    
	    String word = docPart[0];//getting the input word
	    outKey.set(word);
            context.write(outKey,one);
	    
	    //loop is not required in this mapper as we know that the input string will only have 3 parts
	}
    } 
        
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	    throws IOException, InterruptedException
	    {
		int sum = 0;
		for (IntWritable val : values) 
		{
		    sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	    }
    }
        
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
        
        Job job = new Job(conf, "PhaseThree");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setJarByClass(PhaseThree.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
        
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
	job.waitForCompletion(true);
    }
        
}
