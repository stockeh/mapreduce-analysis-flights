package cs455.hadoop.basic;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for the metadata files.
 * 
 * @author stock
 *
 */
public class MetadataMap extends Mapper<LongWritable, Text, Text, Text> {

  /**
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

  }
}
