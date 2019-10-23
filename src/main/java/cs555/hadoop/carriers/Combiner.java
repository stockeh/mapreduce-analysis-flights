package cs555.hadoop.carriers;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs555.hadoop.util.Constants;
import cs555.hadoop.util.DocumentUtilities;

public class Combiner extends Reducer<Text, Text, Text, Text> {

  private final Text val = new Text();

  private final StringBuilder sb = new StringBuilder();

  public void reduce(Text key, Iterable<Text> value, Context context)
      throws IOException, InterruptedException {

    int totalDelay = 0;
    int count = 0;
    for ( Text v : value )
    {
      String[] split = v.toString().split( Constants.SEPERATOR );

      switch ( split[ 0 ] )
      {
        case Constants.DATA :
          totalDelay += DocumentUtilities.parseDouble( split[ 1 ] );
          count += DocumentUtilities.parseDouble( split[ 2 ] );
          break;

        case Constants.CARRIER :
          context.write( key, v );
          break;
      }
    }
    if ( count > 0 )
    {
      sb.append( Constants.DATA ).append( Constants.SEPERATOR )
          .append( totalDelay ).append( Constants.SEPERATOR ).append( count );
      val.set( sb.toString() );
      sb.setLength( 0 );
      context.write( key, val );
    }
  }

}
