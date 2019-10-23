package cs555.hadoop.time;

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
   * TIME/WEEK/MONTH time, delay count
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> line = DocumentUtilities.splitString( value.toString() );

    double delay = DocumentUtilities.parseDouble( line.get( 14 ) )
        + DocumentUtilities.parseDouble( line.get( 15 ) );

    sb.setLength( 0 );
    sb.append( delay ).append( Constants.SEPERATOR ).append( 1 );
    val.set( sb.toString() );

    String tmp = line.get( 5 );
    if ( tmp.length() > 0 )
    {
      sb.setLength( 0 );
      sb.append( Constants.TIME ).append( Constants.SEPERATOR )
          .append( transformDepTime( tmp ) );
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
   * @param time hhmm
   * @return the string rounded to the nearest 15 minute interval.
   */
  private String transformDepTime(String time) {
    time = String.format( "%4s", time ).replace( ' ', '0' );
    int hours = Integer.parseInt( time.substring( 0, 2 ) );
    int i = Integer.parseInt( time.substring( 2, 4 ) );
    int minutes = i % 15 < 8 ? i / 15 * 15 : ( i / 15 + 1 ) * 15;
    if ( minutes >= 60 )
    {
      if ( hours >= 23 )
      {
        hours = 0;
      } else
      {
        ++hours;
      }
      minutes = 0;
    }
    return String.format( "%02d", hours ) + ":"
        + String.format( "%02d", minutes );
  }

}
