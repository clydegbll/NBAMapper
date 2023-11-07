package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, Text, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    
	  //initializing a count variable 
	  int count = 0;
	    
	    // Iterate through the values 
	    for (Text value : values) {
	    //increment the count 
	      count++;
	    }
	    
	    //writing the output ip address and count 
	    context.write(key, new IntWritable(count));
  }
}
