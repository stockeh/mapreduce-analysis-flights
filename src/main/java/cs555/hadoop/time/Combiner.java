package cs555.hadoop.time;

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
      totalDelay += DocumentUtilities.parseDouble( split[ 0 ] );
      count += DocumentUtilities.parseDouble( split[ 1 ] );
    }
    sb.setLength( 0 );
    sb.append( totalDelay ).append( Constants.SEPERATOR ).append( count );
    val.set( sb.toString() );

    context.write( key, val );
  }

}
