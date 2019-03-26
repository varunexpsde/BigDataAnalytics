import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
        
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
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
	//private final static IntWritable one = new IntWritable(1);
	private Text outKey = new Text();
        private Text outVal = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	    String inputLine = value.toString(); //input is coming from the output file from phase one
	    String temp[] = inputLine.split("\t"); //spliting input string to get pair of word,document name and frequency
	    //int wordCntr = Integer.parseInt(temp[1]);//getting word frequency
	    String docPart[]=temp[0].split(",");//seperating document name and word
	    
	    String word = docPart[0];//getting the input word
	    outKey.set(word);
            outVal.set(docPart[1]+"~"+temp[1].replace(",","~")+"~"+"1");

	    context.write(outKey,outVal);
	    
	    //loop is not required in this mapper as we know that the input string will only have 3 parts
	}
    } 
        
    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable>
    {

	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException
	    {
		int sum = 0;
		List<String> vals_list = new ArrayList<>();
		for (Text val : values) 
		{
		    String val_split[] = val.toString().split("~");
		    int count = Integer.parseInt(val_split[3]);
		    vals_list.add(val.toString());
		    sum += count;
		}
		Iterator<String> iter=vals_list.iterator();
		while(iter.hasNext())
		{
		    String vals[]= iter.next().split("~");
		    String outkey = key.toString()+","+vals[0]+","+vals[1]+","+vals[2]+","+sum;
		    double TFIDF = Double.parseDouble(vals[1])/Double.parseDouble(vals[2]) * Math.log10(Double.valueOf(4)/Double.valueOf(sum));
		    context.write( new Text(outkey), new DoubleWritable(TFIDF));
		}
	    }
    }
        
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
	conf.set("mapred.textoutputformat.separator",",");        
        Job job = new Job(conf, "PhaseThree");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
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
