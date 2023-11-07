package stubs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ProcessLogs {

  public static void main(String[] args) throws Exception {

	//Checking if the user has provided two arguments the input directory and output directory
    if (args.length != 2) {
      System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
      System.exit(-1);
    }
 // Creating a new configuration object
    Configuration conf = new Configuration();
 // set jar file that contains
    Job job = Job.getInstance(conf, "Process Logs");
 // set jar file that contains the driver class
    job.setJarByClass(ProcessLogs.class);

    
    //Setting the input and ouput file paths
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    
    // Setting the mapper and reducer classes
    job.setMapperClass(LogMonthMapper.class);
    job.setReducerClass(CountReducer.class);
    
    //sets the output key class of the mapper
    job.setMapOutputKeyClass(Text.class);
    //sets the output value class of the mapper
    job.setMapOutputValueClass(Text.class);

  //Set the output key and value types for the job
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    // sets the number of reducers for the job to 11.
    job.setNumReduceTasks(11);
    //sets the partitioner class for the job to MonthPartitioner.class
    job.setPartitionerClass(MonthPartitioner.class);
  
   
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}
