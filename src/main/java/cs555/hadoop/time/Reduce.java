package cs555.hadoop.time;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 * 
 * @author stock
 *
 */
public class Reduce
    extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  private final static Text maxTime = new Text();
  private static double maxTimeVal;

  private final static Text minTime = new Text();
  private static double minTimeVal = Double.MAX_VALUE;

  /**
   * 
   */
  @Override
  protected void reduce(Text key, Iterable<DoubleWritable> values,
      Context context) throws IOException, InterruptedException {

    double total = 0;
    int count = 0;
    for ( DoubleWritable v : values )
    {
      total += v.get();
      ++count;
    }
    total /= count;
    context.write( key, new DoubleWritable( total ) );

    if ( total > maxTimeVal )
    {
      maxTimeVal = total;
      maxTime.set( key );
    }
    if ( total < minTimeVal )
    {
      minTimeVal = total;
      minTime.set( key );
    }
  }

  /**
   * Once the <code>reduce</code> method has completed execution, the
   * <code>cleanup</code> method is called.
   *
   * This method will write associated values in the desired manner to
   * <code>Context</code> to be viewed in HDFS.
   */
  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    // context.write( new Text( "\n----Q1. BEST TIME TO MINIMIZE DELAYS"
    // ),
    // new DoubleWritable() );
    // context.write( minTime, new DoubleWritable( minTimeVal ) );
    //
    // context.write( new Text( "\n----Q2. WORST TIME TO MINIMIZE DELAYS"
    // ),
    // new DoubleWritable() );
    // context.write( maxTime, new DoubleWritable( maxTimeVal ) );
  }
}
