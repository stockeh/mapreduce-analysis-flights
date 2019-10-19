package cs555.hadoop.hubs;

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
public class MapAirports extends Mapper<LongWritable, Text, Text, Text> {

  private final Text keyText = new Text();

  private final Text val = new Text();

  private final StringBuilder sb = new StringBuilder();

  /**
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> line = DocumentUtilities.splitString( value.toString() );

    String iata = line.get( 0 );
    if ( iata.length() > 0 )
    {
      String airport = line.get( 1 );

      sb.append( Constants.AIRPORTS ).append( "\t" )
          .append( airport.length() > 0 ? airport : "---" );

      keyText.set( iata );
      val.set( sb.toString() );
      sb.setLength( 0 );

      context.write( keyText, val );
    }
  }
}
