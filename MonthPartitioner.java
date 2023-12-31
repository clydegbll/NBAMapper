
package stubs;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class MonthPartitioner<K2, V2> extends Partitioner<Text, Text> implements
    Configurable {

  private Configuration configuration;
  HashMap<String, Integer> months = new HashMap<String, Integer>();

  /**
   * Set up the months hash map in the setConf method.
   */
  //months is populated with each month name to its matching value.
  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
    
    months.put("Jan", -1);
    months.put("Feb", 0);
    months.put("Mar", 1);
    months.put("Apr", 2);
    months.put("May", 3);
    months.put("Jun", 4);
    months.put("Jul", 5);
    months.put("Aug", 6);
    months.put("Sep", 7);
    months.put("Oct", 8);
    months.put("Nov", 9);
    months.put("Dec", 10);
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the three-letter abbreviation for the month
   * as its value. (It is the output value from the mapper.)
   * It should return an integer representation of the month.
   * Note that January is represented as 0 rather than 1.
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 12 reducers.
   */
  public int getPartition(Text key, Text value, int numReduceTasks) {
	  /*
	     * TODO implement
	     * Change the return 0 statement below to return the number of the month
	     * represented by the value parameter.  Use to months hashtable to map
	     * the string to the month number: months.get(value.toString()) and cast it to int.
	     */
	  
	  //returns an integer  the partition number to which the key-value pair should be assigned
	  // value corresponding to the month  of the given value is found using months
      /* if the month is January  the key-value pair 
       * is sent to a dummy reducer.
       *  If the month is any other month, the partition is found by 
       *  modulo of the month with number of reducers specified.
       */
	  int monthNum = months.get(value.toString());
      if (monthNum == -1) {
          return numReduceTasks; // Send to final (dummy) reducer to be ignored
      }
      return monthNum % numReduceTasks;
  }
}

