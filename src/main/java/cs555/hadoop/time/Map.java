package cs555.hadoop.time;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
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
public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  private final Text keyText = new Text();

  private final DoubleWritable val = new DoubleWritable();

  private final StringBuilder sb = new StringBuilder();

  /**
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> line = DocumentUtilities.splitString( value.toString() );

    double delay = DocumentUtilities.parseDouble( line.get( 14 ).trim() )
        + DocumentUtilities.parseDouble( line.get( 15 ).trim() );

    val.set( delay );

    String tmp = line.get( 4 );
    if ( tmp.length() > 0 )
    {
      sb.setLength( 0 );
      sb.append( Constants.TIME ).append( Constants.SEPERATOR ).append( tmp );
      keyText.set( sb.toString() );
      context.write( keyText, val );
    }

    tmp = line.get( 3 );
    if ( tmp.length() > 0 )
    {
      sb.setLength( 0 );
      sb.append( Constants.WEEK ).append( Constants.SEPERATOR ).append( tmp );
      keyText.set( sb.toString() );
      context.write( keyText, val );
    }

    tmp = line.get( 1 );
    if ( tmp.length() > 0 )
    {
      sb.setLength( 0 );
      sb.append( Constants.MONTH ).append( Constants.SEPERATOR ).append( tmp );
      keyText.set( sb.toString() );
      context.write( keyText, val );
    }
  }

  /**
   * 
   * @param sb
   * @param time hhmm
   * @return the string rounded to the nearest 15 minute interval.
   */
  private String transformDepTime(StringBuilder sb, String time) {
    time = String.format( "%4s", time ).replace( ' ', '0' );
    int hours = Integer.parseInt( time.substring( 0, 2 ) );
    int i = Integer.parseInt( time.substring( 2, 4 ) );
    int minutes = i % 15 < 8 ? i / 15 * 15 : ( i / 15 + 1 ) * 15;
    return sb.append( String.format( "%02d", hours ) )
        .append( String.format( "%02d", minutes ) ).toString();
  }

}
