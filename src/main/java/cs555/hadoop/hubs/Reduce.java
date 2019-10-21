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

  // Airport Name, Total Num of Flights In & Out
  private final TreeMap<Integer, String> globalTop = new TreeMap<>();

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

    int flightCount, totalFlightsPerIATA = 0, numberOfDelaysPerIATA = 0;
    String airport = null, year, id = "", coast = Constants.NONE;
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
          Map<String, Integer> hubs = years.get( year );
          if ( hubs == null )
          {
            hubs = new HashMap<>();
            years.put( year, hubs );
          }
          // Add the flight count for this IATA & the input year
          if ( airport == null )
          {
            hubs.put( key.toString(),
                hubs.getOrDefault( key.toString(), 0 ) + flightCount );
          } else if ( hubs.containsKey( key.toString() ) )
          {
            hubs.put( id, hubs.remove( key.toString() ) );
          } else
          {
            hubs.put( id, hubs.getOrDefault( id, 0 ) + flightCount );
          }
          numberOfDelaysPerIATA += DocumentUtilities.parseInt( split[ 3 ] );
          break;

        case Constants.AIRPORTS :
          airport = split[ 1 ];
          id = sb.append( key.toString() ).append( Constants.SEPERATOR )
              .append( airport ).toString();
          sb.setLength( 0 );
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
    globalTop.put( totalFlightsPerIATA, id );

    if ( globalTop.size() > 10 )
    {
      globalTop.remove( globalTop.firstKey() );
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
   * Once the <code>reduce</code> method has completed execution, the
   * <code>cleanup</code> method is called.
   *
   * This method will write associated values in the desired manner to
   * <code>Context</code> to be viewed in HDFS.
   */
  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
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

    for ( Entry<Integer, String> e : globalTop.descendingMap().entrySet() )
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
