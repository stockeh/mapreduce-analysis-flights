package cs555.hadoop.time;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import cs555.hadoop.util.Constants;

public class CustomPartitioner extends Partitioner<Text, DoubleWritable> {

  @Override
  public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
    if ( key.toString().contains( Constants.TIME ) )
    {
      return 0;
    } else if ( key.toString().contains( Constants.WEEK ) )
    {
      return 1;
    } else
    {
      return 2;
    }
  }
}
