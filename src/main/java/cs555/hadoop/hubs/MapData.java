package cs555.hadoop.hubs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import cs555.hadoop.util.Constants;
import cs555.hadoop.util.DocumentUtilities;

/**
 * Mapper class for the analysis data files.
 * 
 * @author stock
 *
 */
public class MapData extends Mapper<LongWritable, Text, Text, Text> {

  private final Map<String, Integer> years = new HashMap<>();

  private final Text keyText = new Text();

  private final Text val = new Text();

  private final StringBuilder sb = new StringBuilder();

  /**
   * for each iata-year combination.
   * 
   * k: iata, v: DATA year count
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> line = DocumentUtilities.splitString( value.toString() );

    String year = line.get( 0 );

    String origin = line.get( 16 );

    if ( origin.length() > 0 )
    {
      sb.append( origin ).append( Constants.SEPERATOR )
          .append( year.length() > 0 ? year : -9999 );
      years.merge( sb.toString(), 1, Integer::sum );
      sb.setLength( 0 );
    }

    String dest = line.get( 17 );

    if ( dest.length() > 0 )
    {
      sb.append( dest ).append( Constants.SEPERATOR )
          .append( year.length() > 0 ? year : -9999 );
      years.merge( sb.toString(), 1, Integer::sum );
      sb.setLength( 0 );
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
    for ( Entry<String, Integer> e : years.entrySet() )
    {
      String[] s = e.getKey().split( Constants.SEPERATOR );
      keyText.set( s[ 0 ] );
      val.set( sb.append( Constants.DATA ).append( Constants.SEPERATOR ).append( s[ 1 ] )
          .append( Constants.SEPERATOR ).append( e.getValue() ).toString() );
      sb.setLength( 0 );
      context.write( keyText, val );
    }
  }
}
