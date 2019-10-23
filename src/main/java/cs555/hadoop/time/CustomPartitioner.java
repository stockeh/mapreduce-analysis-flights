package cs555.hadoop.time;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import cs555.hadoop.util.Constants;

public class CustomPartitioner extends Partitioner<Text, Text> {

  @Override
  public int getPartition(Text key, Text value, int numReduceTasks) {
    String s = key.toString().split( Constants.SEPERATOR )[ 0 ];
    if ( s.equals( Constants.TIME ) )
    {
      return 0;
    } else if ( s.equals( Constants.WEEK ) )
    {
      return 1;
    } else
    {
      return 2;
    }
  }
}
