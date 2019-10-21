package cs555.hadoop.empty;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 * 
 * @author stock
 *
 */
public class Reduce extends Reducer<Text, Text, Text, IntWritable> {

  /**
   * 
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    int count = 0;
    for ( @SuppressWarnings( "unused" )
    Text v : values )
    {
      ++count;
    }
    context.write( key, new IntWritable( count ) );
  }
}
