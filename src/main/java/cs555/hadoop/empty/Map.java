package cs555.hadoop.empty;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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

  /**
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> line = DocumentUtilities.splitString( value.toString() );

    String tmp = line.get( 10 );
    if ( tmp.length() > 0 )
    {
      keyText.set( "TailNum" );
      val.set( tmp );
      context.write( keyText, val );
    }

    tmp = line.get( 13 );
    if ( tmp.length() > 0 )
    {
      keyText.set( "AirTime" );
      val.set( tmp );
      context.write( keyText, val );
    }
    tmp = line.get( 19 );
    if ( tmp.length() > 0 )
    {
      keyText.set( "TaxiIn" );
      val.set( tmp );
      context.write( keyText, val );
    }
    tmp = line.get( 20 );
    if ( tmp.length() > 0 )
    {
      keyText.set( "TaxiOut" );
      val.set( tmp );
      context.write( keyText, val );
    }
    tmp = line.get( 22 );
    if ( tmp.length() > 0 )
    {
      keyText.set( "CancellationCode" );
      val.set( tmp );
      context.write( keyText, val );
    }
    tmp = line.get( 24 );
    if ( tmp.length() > 0 )
    {
      keyText.set( "CarrierDelay" );
      val.set( tmp );
      context.write( keyText, val );
    }
    tmp = line.get( 25 );
    if ( tmp.length() > 0 )
    {
      keyText.set( "WeatherDelay" );
      val.set( tmp );
      context.write( keyText, val );
    }
    tmp = line.get( 26 );
    if ( tmp.length() > 0 )
    {
      keyText.set( "NASDelay" );
      val.set( tmp );
      context.write( keyText, val );
    }
    tmp = line.get( 27 );
    if ( tmp.length() > 0 )
    {
      keyText.set( "SecurityDelay" );
      val.set( tmp );
      context.write( keyText, val );
    }
    if ( line.size() > 29 )
    {
      tmp = line.get( 28 );
      if ( tmp.length() > 0 )
      {
        keyText.set( "LateAircraftDelay" );
        val.set( tmp );
        context.write( keyText, val );
      }
    }
  }

}
