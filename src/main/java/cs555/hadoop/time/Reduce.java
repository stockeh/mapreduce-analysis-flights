package cs555.hadoop.time;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs555.hadoop.util.Constants;

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

  private final static Text maxWeek = new Text();
  private static double maxWeekVal;

  private final static Text minWeek = new Text();
  private static double minWeekVal = Double.MAX_VALUE;

  private final static Text maxMonth = new Text();
  private static double maxMonthVal;

  private final static Text minMonth = new Text();
  private static double minMonthVal = Double.MAX_VALUE;

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
    switch ( key.toString().split( "\t" )[ 0 ] )
    {
      case Constants.TIME :
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
        break;

      case Constants.WEEK :
        if ( total > maxWeekVal )
        {
          maxWeekVal = total;
          maxWeek.set( key );
        }
        if ( total < minWeekVal )
        {
          minWeekVal = total;
          minWeek.set( key );
        }
        break;

      case Constants.MONTH :
        if ( total > maxMonthVal )
        {
          maxMonthVal = total;
          maxMonth.set( key );
        }
        if ( total < minMonthVal )
        {
          minMonthVal = total;
          minMonth.set( key );
        }
        break;
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
    context.write( minWeek, new DoubleWritable( minWeekVal ) );
    context.write( minMonth, new DoubleWritable( minMonthVal ) );

    context.write( new Text( "\n----Q2. WORST TIME TO MINIMIZE DELAYS" ),
        new DoubleWritable() );
    context.write( maxTime, new DoubleWritable( maxTimeVal ) );
    context.write( maxWeek, new DoubleWritable( maxWeekVal ) );
    context.write( maxMonth, new DoubleWritable( maxMonthVal ) );
  }
}
