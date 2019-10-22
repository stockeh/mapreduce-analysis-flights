package cs555.hadoop.carriers;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
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

  private final TreeMap<Integer, String> topTotalDelays = new TreeMap<>();

  private final TreeMap<Integer, String> topTotalCount = new TreeMap<>();

  private final TreeMap<Double, String> topAverage = new TreeMap<>();

  private final StringBuilder sb = new StringBuilder();

  /**
   * 
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    int totalDelay = 0;
    int count = 0;
    sb.setLength( 0 );

    String[] split;
    for ( Text t : values )
    {
      split = t.toString().split( Constants.SEPERATOR );
      switch ( split[ 0 ] )
      {
        case Constants.DATA :
          totalDelay += DocumentUtilities.parseDouble( split[ 1 ] );
          count += DocumentUtilities.parseDouble( split[ 2 ] );
          break;

        case Constants.CARRIER :
          sb.append( key.toString() ).append( Constants.SEPERATOR )
              .append( split[ 1 ] );
          break;
      }
    }
    if ( totalDelay > 0 )
    {
      String s = sb.append( Constants.SEPERATOR ).append( "CUMULATIVE: " ).append( totalDelay )
          .append( Constants.SEPERATOR ).append( "TOTAL COUNT: " ).append( count ).toString();

      topTotalCount.put( count, s );
      if ( topTotalCount.size() > 10 )
      {
        topTotalCount.remove( topTotalCount.firstKey() );
      }

      topTotalDelays.put( totalDelay, s );
      if ( topTotalDelays.size() > 10 )
      {
        topTotalDelays.remove( topTotalDelays.firstKey() );
      }

      topAverage.put( totalDelay / ( double ) count, s );
      if ( topAverage.size() > 10 )
      {
        topAverage.remove( topAverage.firstKey() );
      }
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
    context.write( new Text( "\n----Q5. CARRIERS WITH TOP NUMBER OF DELAYS" ),
        new DoubleWritable() );

    for ( Entry<Integer, String> e : topTotalCount.descendingMap().entrySet() )
    {
      context.write( new Text( e.getValue() ),
          new DoubleWritable( e.getKey() ) );
    }

    context.write( new Text( "\n----    CARRIERS WITH TOP ACCUMULATIVE DELAY" ),
        new DoubleWritable() );
    for ( Entry<Integer, String> e : topTotalDelays.descendingMap().entrySet() )
    {
      context.write( new Text( e.getValue() ),
          new DoubleWritable( e.getKey() ) );
    }

    context.write( new Text( "\n----    CARRIERS WITH TOP AVERAGE DELAY" ),
        new DoubleWritable() );
    for ( Entry<Double, String> e : topAverage.descendingMap().entrySet() )
    {
      context.write( new Text( e.getValue() ),
          new DoubleWritable( e.getKey() ) );
    }
  }
}
