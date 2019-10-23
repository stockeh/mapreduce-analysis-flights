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
public class CarrierMap extends Mapper<LongWritable, Text, Text, Text> {

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

    String carrier = line.get( 0 );

    String description = line.get( 1 ).length() > 0 ? line.get( 1 ) : "desc";

    if ( carrier.length() > 0 && description.length() > 0 )
    {
      keyText.set( carrier );
      sb.append( Constants.CARRIER ).append( Constants.SEPERATOR )
          .append( description );
      val.set( sb.toString() );
      sb.setLength( 0 );
      context.write( keyText, val );
    }
  }

}

