import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// For understanding purposes, read the paragraph listed below. Helped me a lot :)
/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class SumReducer extends Reducer<Text, Text, Text, Text> {
// Also this paragraph ...
  /*
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives a key of type Text, a set of values of type
   * IntWritable, and a Context object.
   */
  @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
	  String reduced = "";  //This variable stores the concatenated list of URL's
	  String previous = "";
	  ArrayList<String> a = new ArrayList<String>();
	  for(Text value : values){
		  a.add(value.toString());
	  }
	  Collections.sort(a);  //Sorting the list for link frequency
	  previous = a.get(0);
	  int count = 0;
	  for(int i=0; i<a.size(); i++){
		  if(a.get(i).equals(previous)){
			  count++;
			  previous = a.get(i);
		  }
		  else{
			  reduced = reduced + " " + previous + " "+count;
			  count = 1;
			  previous = a.get(i);
		  }
	  }
	  // For the list URL to be added 
	  reduced = reduced + " " +previous.toString() + " " +  count;
	  // For debugging purposes, go to http://localhost:50030/jobtracker.jsp
	  System.out.println(key.toString());
	  System.out.println(reduced);
	  // Call the write method on the Context object to emit a key and a value from the reduce method. 
	  context.write(key, new Text(reduced));
  }
}

