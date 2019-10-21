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
   * k: iata, v: AIRPORTS airport_name EAST/WEST/NONE city_name
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> line = DocumentUtilities.splitString( value.toString() );

    String iata = line.get( 0 );
    if ( iata.length() > 0 )
    {

      String coast = Constants.NONE;

      if ( Constants.EAST_COAST.contains( line.get( 3 ) ) )
      {
        coast = Constants.EAST;
      } else if ( Constants.WEST_COAST.contains( line.get( 3 ) ) )
      {
        coast = Constants.WEST;
      }

      String airport = line.get( 1 );
      String city = line.get( 2 );

      sb.append( Constants.AIRPORTS ).append( Constants.SEPERATOR )
          .append( airport.length() > 0 ? airport : "airport" )
          .append( Constants.SEPERATOR ).append( coast )
          .append( Constants.SEPERATOR )
          .append( city.length() > 0 ? city : "city" );

      keyText.set( iata );
      val.set( sb.toString() );
      sb.setLength( 0 );

      context.write( keyText, val );
    }
  }
}
