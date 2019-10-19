package cs555.hadoop.hubs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
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
public class Reduce extends Reducer<Text, Text, Text, IntWritable> {

  private final TreeMap<Integer, String> globalTop = new TreeMap<>();

  private final TreeMap<String, Map<String, Integer>> years = new TreeMap<>();

  private final StringBuilder sb = new StringBuilder();

  /**
   * 
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    int count, total = 0;
    String airport = null, year, id = "";
    String[] split;
    for ( Text t : values )
    {
      split = t.toString().split( "\t" );
      switch ( split[ 0 ] )
      {
        case Constants.DATA :
          year = split[ 1 ];
          count = DocumentUtilities.parseInt( split[ 2 ] );
          total += count;
          Map<String, Integer> hubs = years.get( year );
          if ( hubs == null )
          {
            hubs = new HashMap<>();
            years.put( year, hubs );
          }
          if ( airport == null )
          {
            hubs.put( key.toString(),
                hubs.getOrDefault( key.toString(), 0 ) + count );
          } else if ( hubs.containsKey( key.toString() ) )
          {
            hubs.put( id, hubs.remove( key.toString() ) );
          } else
          {
            hubs.put( id, hubs.getOrDefault( id, 0 ) + count );
          }
          break;

        case Constants.AIRPORTS :
          if ( airport == null )
          {
            airport = split[ 1 ];
            id = sb.append( key.toString() ).append( "\t" ).append( airport )
                .toString();
            sb.setLength( 0 );
          }
          break;
      }
    }
    globalTop.put( total, id );

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
    context.write( new Text( "\n----Q3. GLOBAL BUSIEST DOMESTIC AIRPORTS" ),
        new IntWritable() );

    for ( Entry<Integer, String> e : globalTop.descendingMap().entrySet() )
    {
      context.write( new Text( e.getValue() ), new IntWritable( e.getKey() ) );
    }
    context.write( new Text( "\n----    BUSIEST DOMESTIC AIRPORTS OVER TIME" ),
        new IntWritable() );
    for ( Entry<String, Map<String, Integer>> e : years.entrySet() )
    {
      int i = 0;
      for ( Entry<String, Integer> hubs : DocumentUtilities
          .sortMapByValue( e.getValue(), true ).entrySet() )
      {
        sb.setLength( 0 );
        if ( i++ < 10 )
        {
          context.write(
              new Text( sb.append( e.getKey() ).append( "\t" )
                  .append( hubs.getKey() ).toString() ),
              new IntWritable( hubs.getValue() ) );
        } else
        {
          break;
        }
      }
    }
  }
}
