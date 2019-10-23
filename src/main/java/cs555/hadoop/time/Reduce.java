package cs555.hadoop.time;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs555.hadoop.util.Constants;
import cs555.hadoop.util.DocumentUtilities;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 * 
 * @author stock
 *
 */
public class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

  private final static Text maxTime = new Text();
  private static double maxTimeVal;

  private final static Text minTime = new Text();
  private static double minTimeVal = Double.MAX_VALUE;

  /**
   * 
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    int totalDelay = 0;
    int count = 0;

    for ( Text t : values )
    {
      String[] split = t.toString().split( Constants.SEPERATOR );
      totalDelay += DocumentUtilities.parseDouble( split[ 0 ] );
      count += DocumentUtilities.parseDouble( split[ 1 ] );
    }

    double avg = totalDelay / ( double ) count;

    if ( avg > maxTimeVal )
    {
      maxTimeVal = avg;
      maxTime.set( key );
    }
    if ( avg < minTimeVal )
    {
      minTimeVal = avg;
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
    context.write( new Text( "\n----Q1. BEST TIME TO MINIMIZE DELAYS" ),
        new DoubleWritable() );
    context.write( minTime, new DoubleWritable( minTimeVal ) );

    context.write( new Text( "\n----Q2. WORST TIME TO MINIMIZE DELAYS" ),
        new DoubleWritable() );
    context.write( maxTime, new DoubleWritable( maxTimeVal ) );
  }
}
