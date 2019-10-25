package cs555.hadoop.late;

import java.io.IOException;
import java.util.ArrayList;
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
public class Map extends Mapper<LongWritable, Text, Text, Text> {

  private final Text keyText = new Text();

  private final Text val = new Text();

  private final StringBuilder sb = new StringBuilder();

  /**
   * destination, source arr_delay_count late_delay_count_for_source
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> line = DocumentUtilities.splitString( value.toString() );

    String origin = line.get( 16 );
    String dest = line.get( 17 );

    double arrivalDelay = DocumentUtilities.parseDouble( line.get( 14 ) );
    double lateDelay = 0;
    if ( line.size() > 28 )
    {
      String tmp = line.get( 28 );
      if ( tmp.length() > 0 )
      {
        lateDelay = DocumentUtilities.parseDouble( tmp );
      }
    }

    sb.setLength( 0 );

    if ( origin.length() > 0 && dest.length() > 0 )
    {
      keyText.set( dest );
      val.set( sb.append( origin ).append( Constants.SEPERATOR )
          .append( arrivalDelay > 0 ? 1 : 0 ).append( Constants.SEPERATOR )
          .append( lateDelay > 0 ? 1 : 0 ).toString() );
      context.write( keyText, val );
    }
  }

}
