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
public class MapData extends Mapper<LongWritable, Text, Text, Text> {

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

    String year = line.get( 0 );

    sb.append( Constants.DATA ).append( "\t" )
        .append( year.length() > 0 ? year : "-9999" );
    val.set( sb.toString() );
    sb.setLength( 0 );

    String origin = line.get( 16 );

    if ( origin.length() > 0 )
    {
      keyText.set( origin );
      context.write( keyText, val );
    }

    String dest = line.get( 17 );

    if ( dest.length() > 0 )
    {
      keyText.set( dest );
      context.write( keyText, val );
    }
  }
}
