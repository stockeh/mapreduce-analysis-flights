package cs555.hadoop.carriers;

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
public class DataMap extends Mapper<LongWritable, Text, Text, Text> {

  private final Text keyText = new Text();

  private final Text val = new Text();

  private final StringBuilder sb = new StringBuilder();

  /**
   * Carrier, numberOfDelays totalTimeInDelay
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> line = DocumentUtilities.splitString( value.toString() );

    String carrier = line.get( 8 );

    double carrierDelay = DocumentUtilities.parseDouble( line.get( 24 ) );

    if ( carrier.length() > 0 && carrierDelay > 0 )
    {
      keyText.set( carrier );
      sb.append( Constants.DATA ).append( Constants.SEPERATOR )
          .append( carrierDelay ).append( Constants.SEPERATOR ).append( 1 );
      val.set( sb.toString() );
      sb.setLength( 0 );
      context.write( keyText, val );
    }
  }

}

