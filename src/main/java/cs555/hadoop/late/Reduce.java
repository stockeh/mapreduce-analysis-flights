package cs555.hadoop.late;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs555.hadoop.util.Constants;
import cs555.hadoop.util.DocumentUtilities;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 * 
 * @author stock
 *
 */
public class Reduce extends Reducer<Text, Text, Text, NullWritable> {

  // <Destination, <Origin, Delay_Information>>
  private final HashMap<String, Flight> flights = new HashMap<>();

  // <Origin, LateDelayCount>
  private final Map<String, Integer> late = new HashMap<>();

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
      Flight flight = flights.get( dest );
      if ( flight == null )
      {
        flight = new Flight();
        flights.put( dest, flight );
      }
      int arrivalDelay = DocumentUtilities.parseInt( split[ 1 ] );
      flight.updateFlight( source, arrivalDelay );

      int lateDelay = DocumentUtilities.parseInt( split[ 2 ] );
      if ( lateDelay > 0 )
      {
        late.merge( source, 1, Integer::sum );
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
    StringBuilder sb = new StringBuilder();
    context.write(
        new Text( "sortMapByValue\n----Q7. ORIGIN  N_DELAY_DUE_TO_LATE" ),
        NullWritable.get() );
    int i = 0;
    for ( Entry<String, Integer> l : DocumentUtilities
        .sortMapByValue( late, true ).entrySet() )
    {
      sb.setLength( 0 );
      if ( ++i > 10 )
      {
        late.remove( l.getKey() );
      } else
      {
        context.write( new Text( sb.append( l.getKey() )
            .append( Constants.SEPERATOR ).append( l.getValue() ).toString() ),
            NullWritable.get() );
      }
    }
    // context.write( new Text(
    // "\n----Q7. DEST ORIGIN N_FLIGHTS N_ARRIVAL_DELAY
    // AVG_DELAYED_FLIGHTS "
    // + "N_GBOBAL_DELAY_FLIGHTS_TO_DEST AVG_GLOBAL_DELAY_FLIGHTS" ),
    // NullWritable.get() );

    context.write( new Text(
        "\n----Q7. DEST    N_LATE_DELAY ORIGIN    AVG_GLOBAL_DELAY_FLIGHTS  CONTRIB_LATE_DELAY" ),
        NullWritable.get() );

    // Over each destination
    for ( Entry<String, Flight> e : flights.entrySet() )
    {
      if ( late.containsKey( e.getKey() ) )
      {
        // AVG_GLOBAL_DELAY_FLIGHTS, ORIGIN
        final TreeMap<Double, String> topLocations = new TreeMap<>();
        for ( Entry<String, Delays> flight : e.getValue().getFlight()
            .entrySet() )
        {
          // sb.setLength( 0 );
          // context.write(
          // new Text( sb.append( e.getKey() ).append( Constants.SEPERATOR )
          // .append( flight.getKey() ).append( Constants.SEPERATOR )
          // .append( flight.getValue().toString() )
          // .append( Constants.SEPERATOR )
          // .append( e.getValue().getTotalFlightsFromOrigin() )
          // .append( Constants.SEPERATOR )
          // .append( flight.getValue().getArrivalDelayCount()
          // / ( double ) e.getValue().getTotalFlightsFromOrigin() )
          // .toString() ),
          // NullWritable.get() );

          topLocations.put(
              flight.getValue().getArrivalDelayCount()
                  / ( double ) e.getValue().getTotalFlightsFromOrigin(),
              flight.getKey() );

          if ( topLocations.size() > 10 )
          {
            topLocations.remove( topLocations.firstKey() );
          }
        }
        for ( Entry<Double, String> l : topLocations.descendingMap()
            .entrySet() )
        {
          double arrivalDelayContrib = l.getKey();
          String source = l.getValue();

          int originLateDelayCount = late.get( e.getKey() );

          int contrib_late_delay =
              ( int ) ( arrivalDelayContrib * originLateDelayCount );

          sb.setLength( 0 );
          context.write(
              new Text( sb.append( e.getKey() ).append( Constants.SEPERATOR )
                  .append( originLateDelayCount ).append( Constants.SEPERATOR )
                  .append( source ).append( Constants.SEPERATOR )
                  .append( arrivalDelayContrib ).append( Constants.SEPERATOR )
                  .append( contrib_late_delay ).toString() ),
              NullWritable.get() );
        }
      }
    }

  }

  private static class Flight {

    private final HashMap<String, Delays> flight;

    private int totalFlightsFromOrigin;

    public Flight() {
      this.flight = new HashMap<>();
      this.totalFlightsFromOrigin = 0;
    }

    public HashMap<String, Delays> getFlight() {
      return flight;
    }

    public void incTotalFlightsFromOrigin() {
      ++totalFlightsFromOrigin;
    }

    public int getTotalFlightsFromOrigin() {
      return totalFlightsFromOrigin;
    }

    public void updateFlight(String source, int arrivalDelay) {

      Delays delay = flight.get( source );
      if ( delay == null )
      {
        delay = new Delays();
        flight.put( source, delay );
      }

      delay.incFlightsFromOrigin();

      if ( arrivalDelay > 0 )
      {
        delay.incArrivalDelayCount();
      }

      incTotalFlightsFromOrigin();
    }

  }

  private static class Delays {

    private int flightsFromOrigin;

    private int arrivalDelayCount;

    public Delays() {
      this.flightsFromOrigin = 0;
      this.arrivalDelayCount = 0;
    }

    public int getArrivalDelayCount() {
      return arrivalDelayCount;
    }

    public void incFlightsFromOrigin() {
      this.flightsFromOrigin++;
    }

    public void incArrivalDelayCount() {
      this.arrivalDelayCount++;
    }

    @Override
    public String toString() {
      return flightsFromOrigin + "\t" + arrivalDelayCount + "\t"
          + arrivalDelayCount / ( double ) flightsFromOrigin;
    }

  }

}
