package cs555.hadoop.hubs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
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
public class Reduce extends Reducer<Text, Text, Text, IntWritable> {

  // Total Num of Flights In & Out, Airport Name
  private final TreeMap<Integer, String> globalTopFlights = new TreeMap<>();

  // Total Num of Wx Related Delays, City Name
  private final TreeMap<Integer, String> globalTopWxDelays = new TreeMap<>();

  // Average Num of Wx Related Delays, City Name
  private final TreeMap<Double, String> globalAverageWxDelays = new TreeMap<>();

  // Year, (IATA / Airport Name, Num Flights In & Out)
  private final TreeMap<String, Map<String, Integer>> years = new TreeMap<>();

  private final StringBuilder sb = new StringBuilder();

  private int westCoastDelays = 0;

  private int totalWestCostFlights = 0;

  private int eastCoastDelays = 0;

  private int totalEastCostFlights = 0;

  /**
   * 
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    int flightCount, totalFlightsPerIATA = 0, numberOfDelaysPerIATA = 0,
        numberOfWxDelays = 0;
    String year, city = null, airport = null, id = "", coast = Constants.NONE;
    String[] split;
    for ( Text t : values )
    {
      split = t.toString().split( Constants.SEPERATOR );
      switch ( split[ 0 ] )
      {
        case Constants.DATA :
          year = split[ 1 ];
          flightCount = DocumentUtilities.parseInt( split[ 2 ] );
          totalFlightsPerIATA += flightCount;

          updateFlightCount( key.toString(), id, year, airport, flightCount );

          numberOfWxDelays += DocumentUtilities.parseInt( split[ 4 ] );

          numberOfDelaysPerIATA += DocumentUtilities.parseInt( split[ 3 ] );
          break;

        case Constants.AIRPORTS :
          airport = split[ 1 ];
          id = sb.append( key.toString() ).append( Constants.SEPERATOR )
              .append( airport ).toString();
          sb.setLength( 0 );

          city = split[ 3 ];

          if ( split[ 2 ].equals( Constants.EAST ) )
          {
            coast = Constants.EAST;
          } else if ( split[ 2 ].equals( Constants.WEST ) )
          {
            coast = Constants.WEST;
          }
          break;
      }
    }
    globalTopFlights.put( totalFlightsPerIATA, id );

    if ( globalTopFlights.size() > 10 )
    {
      globalTopFlights.remove( globalTopFlights.firstKey() );
    }

    if ( numberOfWxDelays > 0 )
    {
      globalTopWxDelays.put( numberOfWxDelays, city );

      if ( globalTopWxDelays.size() > 10 )
      {
        globalTopWxDelays.remove( globalTopWxDelays.firstKey() );
      }

      // total number of flights in and out of the airport
      sb.append( city ).append( "\tNumberOfDelays: " )
          .append( numberOfWxDelays ).append( "\tNumberOfFlights: " )
          .append( totalFlightsPerIATA ).append( "\tAverage %:" );

      globalAverageWxDelays.put(
          numberOfWxDelays / ( double ) totalFlightsPerIATA, sb.toString() );
      sb.setLength( 0 );

      if ( globalAverageWxDelays.size() > 10 )
      {
        globalAverageWxDelays.remove( globalAverageWxDelays.firstKey() );
      }
    }

    if ( coast.equals( Constants.EAST ) )
    {
      eastCoastDelays += numberOfDelaysPerIATA;
      totalEastCostFlights += totalFlightsPerIATA;
    } else if ( coast.equals( Constants.WEST ) )
    {
      westCoastDelays += numberOfDelaysPerIATA;
      totalWestCostFlights += totalFlightsPerIATA;
    }
  }

  /**
   * Add the flight count for this IATA & the input year
   * 
   * @param key
   * @param id
   * @param year
   * @param airport
   * @param flightCount
   */
  private void updateFlightCount(String key, String id, String year,
      String airport, int flightCount) {
    Map<String, Integer> hubs = years.get( year );
    if ( hubs == null )
    {
      hubs = new HashMap<>();
      years.put( year, hubs );
    }
    if ( airport == null )
    {
      hubs.put( key, hubs.getOrDefault( key, 0 ) + flightCount );
    } else if ( hubs.containsKey( key ) )
    {
      hubs.put( id, hubs.remove( key ) );
    } else
    {
      hubs.put( id, hubs.getOrDefault( id, 0 ) + flightCount );
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
    context.write( new Text( "\n----Q4. TOP MOST WX DELAYS BY CITY" ),
        new IntWritable() );

    for ( Entry<Integer, String> e : globalTopWxDelays.descendingMap()
        .entrySet() )
    {
      context.write( new Text( e.getValue() ), new IntWritable( e.getKey() ) );
    }
    context.write( new Text( "\n----    AVERAGE MOST WX DELAYS BY CITY" ),
        new IntWritable() );

    for ( Entry<Double, String> e : globalAverageWxDelays.descendingMap()
        .entrySet() )
    {
      context.write(
          new Text( e.getValue() + "\t" + String.format( "%.3f", e.getKey() ) ),
          new IntWritable( e.getKey().intValue() ) );
    }

    context.write( new Text( "\n----Q6. EAST VS WEST COAST NUMBER OF DELAYS" ),
        new IntWritable() );

    context.write(
        new Text( "EAST\tNumberOfDelays: " + eastCoastDelays
            + "\tNumberOfFlights: " + totalEastCostFlights + "\tAverage %: " ),
        new IntWritable( ( int ) ( eastCoastDelays
            / ( double ) totalEastCostFlights * 100 ) ) );
    context.write(
        new Text( "WEST\tNumberOfDelays: " + westCoastDelays
            + "\tNumberOfFlights: " + totalWestCostFlights + "\tAverage %: " ),
        new IntWritable( ( int ) ( westCoastDelays
            / ( double ) totalWestCostFlights * 100 ) ) );

    context.write( new Text( "\n----Q3. GLOBAL BUSIEST DOMESTIC AIRPORTS" ),
        new IntWritable() );

    for ( Entry<Integer, String> e : globalTopFlights.descendingMap()
        .entrySet() )
    {
      context.write( new Text( e.getValue() ), new IntWritable( e.getKey() ) );
    }
    context.write( new Text( "\n----    BUSIEST DOMESTIC AIRPORTS OVER TIME" ),
        new IntWritable() );
    for ( Entry<String, Map<String, Integer>> e : years.entrySet() )
    {
      int i = 0;
      for ( Entry<String, Integer> hubs : DocumentUtilities
          .sortMapByValue( e.getValue(), true ).entrySet() )
      {
        sb.setLength( 0 );
        if ( i++ < 10 )
        {
          context.write(
              new Text( sb.append( e.getKey() ).append( Constants.SEPERATOR )
                  .append( hubs.getKey() ).toString() ),
              new IntWritable( hubs.getValue() ) );
        } else
        {
          break;
        }
      }
    }
  }
}
