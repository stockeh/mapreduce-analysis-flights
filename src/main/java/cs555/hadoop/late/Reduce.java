package cs555.hadoop.late;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import cs555.hadoop.util.Constants;
import cs555.hadoop.util.DocumentUtilities;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 * 
 * @author stock
 *
 */
public class Reduce extends Reducer<Text, Text, Text, Text> {

  // <Destination, <Origin, Delay_Information>>
  private final HashMap<String, HashMap<String, Delays>> flights =
      new HashMap<>();

  /**
   * 
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    String dest = key.toString();

    String[] split;
    for ( Text t : values )
    {
      split = t.toString().split( Constants.SEPERATOR );

      String source = split[ 0 ];
      HashMap<String, Delays> flight = flights.get( dest );
      if ( flight == null )
      {
        flight = new HashMap<>();
        flights.put( dest, flight );
      }

      Delays delay = flight.get( source );
      if ( delay == null )
      {
        flight.put( source, new Delays() );
      }

      delay.incFlightsFromOrigin();

      int arrivalDelay = DocumentUtilities.parseInt( split[ 1 ] );

      int lateDelay = DocumentUtilities.parseInt( split[ 2 ] );

      if ( arrivalDelay > 0 )
      {
        delay.incArrivalDelayCount();
      }
      if ( lateDelay > 0 )
      {
        delay.incLateDelayCount();
      }
    }
  }
  
  /**
   * Once the <code>reduce</code> method has completed execution, the
   * <code>cleanup</code> method is called.
   *
   * This method will write associated values in the desired manner to
   * <code>Context</code> to be viewed in HDFS.
   */
  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    
  }

  private static class Delays {

    private int flightsFromOrigin;

    private int arrivalDelayCount;

    private int lateDelayCount;

    public Delays() {
      this.flightsFromOrigin = 0;
      this.arrivalDelayCount = 0;
      this.lateDelayCount = 0;
    }

    public void incFlightsFromOrigin() {
      this.flightsFromOrigin++;
    }

    public void incArrivalDelayCount() {
      this.arrivalDelayCount++;
    }

    public void incLateDelayCount() {
      this.lateDelayCount++;
    }

    @Override
    public String toString() {
      return "flightsFromOrigin: " + flightsFromOrigin + ", arrivalDelayCount: "
          + arrivalDelayCount + ", lateDelayCount: " + lateDelayCount;
    }

  }

}
