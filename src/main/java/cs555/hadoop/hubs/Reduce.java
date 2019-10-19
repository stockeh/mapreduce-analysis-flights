package cs555.hadoop.hubs;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
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
public class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

  private final static TreeMap<Integer, String> globalTop = new TreeMap<>();

  private final static Map<String, Integer> years = new HashMap<>();

  private final StringBuilder sb = new StringBuilder();

  /**
   * 
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    int count = 0;
    String airport = null, year;
    String[] split;
    for ( Text t : values )
    {
      split = t.toString().split( "\t" );
      // context.write( new Text( key.toString() + " " + t.toString() ),
      // new DoubleWritable( 0 ) );
      switch ( split[ 0 ] )
      {
        case Constants.DATA :
          // year = split[ 1 ];
          // int c = years.containsKey( year ) ? years.get( year ) : 0;
          // years.put( year, c + 1 );
          ++count;
          break;

        case Constants.AIRPORTS :
          if ( airport == null )
          {
            airport = split[ 1 ];
            sb.append( key.toString() ).append( "\t" ).append( airport );
          }
          break;
      }
    }
    globalTop.put( count, sb.toString() );
    sb.setLength( 0 );

    if ( globalTop.size() > 10 )
    {
      globalTop.remove( globalTop.firstKey() );
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
    context.write( new Text( "\n----Q3. TOP BUSIEST DOMESTIC AIRPORTS" ),
        new DoubleWritable() );

    for ( Entry<Integer, String> e : globalTop.descendingMap().entrySet() )
    {
      context.write( new Text( e.getValue() ),
          new DoubleWritable( e.getKey() ) );
    }

    context.write( new Text( "\n----    BUSIEST DOMESTIC AIRPORTS OVER TIME" ),
        new DoubleWritable() );

    for ( Entry<String, Integer> e : years.entrySet() )
    {
      context.write( new Text( e.getKey() ),
          new DoubleWritable( e.getValue() ) );
    }
  }
}
