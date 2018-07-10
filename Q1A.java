import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Q1A {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		Text myKey = new Text();
		IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			
			String job_title = record[4];
			
			String year = record[7];
			
			if(job_title.contains("DATA ENGINEER") && job_title != null)
			{
				myKey.set(year);
				
				context.write(myKey, one);
			}
		}
	}
		
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		protected void reduce(Text key,Iterable<IntWritable> value,Context context)throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable val:value)
			{
				sum+=val.get();
			}
			
			context.write(key, new IntWritable(sum));
					
			}
		}
	public static class GrowthMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			context.write(new Text("common"),value);
		}
	}

	public static class GrowthReducer extends Reducer<Text,Text,NullWritable,Text>
	{

		protected void reduce(Text key,Iterable<Text> value,Context context)throws IOException, InterruptedException {
			int count2011 = 1;
			int count2012 = 1;
			int count2013 = 1;
			int count2014 = 1;
			int count2015 = 1;
			int count2016 = 1;
			
			double growthavg = 0.00;
			for(Text v:value)
			{
				String[] val=v.toString().split("\t");

				if (val[0].equals("2011"))
				{
					count2011 = Integer.parseInt(val[1]);
				}
				if (val[0].equals("2012"))
				{
					count2012 = Integer.parseInt(val[1]);
				}
				if (val[0].equals("2013"))
				{
					count2013 = Integer.parseInt(val[1]);
				}
				if (val[0].equals("2014"))
				{
					count2014 = Integer.parseInt(val[1]);
				}
				if (val[0].equals("2015"))
				{
					count2015 = Integer.parseInt(val[1]);
				}
				if (val[0].equals("2016"))
				{
					count2016 = Integer.parseInt(val[1]);
				}
			}
			growthavg = ((((count2012-count2011)*100)/count2011) + (((count2013-count2012)*100)/count2012) + (((count2014-count2013)*100)/count2013) + (((count2015-count2014)*100)/count2014) + (((count2016-count2015)*100)/count2015))/5 ;
			

			if (growthavg>0.00)
			{
			 context.write(NullWritable.get(), new Text("The growth is positive : " + String.format("%f", growthavg)));	
			
			}
		}
		
	}
	
	
public static void main(String[] args) throws Exception {
	
	
	
	
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf," ");
	  job.setJarByClass(Q1A.class);

	  job.setMapperClass(MyMapper.class);
	  job.setReducerClass(MyReducer.class);
	  
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(IntWritable.class);
	  
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(IntWritable.class);

	  Path outputPath1 = new Path("FirstMapper");
	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, outputPath1);
	  FileSystem.get(conf).delete(outputPath1, true);

	  job.waitForCompletion(true);

	  Job job2 = Job.getInstance(conf," ");
	  job2.setJarByClass(Q1A.class);

	  job2.setMapperClass(GrowthMapper.class);
	  job2.setReducerClass(GrowthReducer.class);
	  //job2.setNumReduceTasks(0);
	  
	  job2.setMapOutputKeyClass(Text.class);
	  job2.setMapOutputValueClass(Text.class);

	  job2.setOutputKeyClass(NullWritable.class);
	  job2.setOutputValueClass(Text.class);

	  FileInputFormat.addInputPath(job2,outputPath1);
	  FileOutputFormat.setOutputPath(job2, new Path(args[1]));

	  System.exit(job2.waitForCompletion(true) ? 0 :1);
	}
	}