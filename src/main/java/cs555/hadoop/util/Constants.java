package cs555.hadoop.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Constants that will not change in the program.
 * 
 * @author stock
 *
 */
public interface Constants {

  final String TIME = "TIME";

  final String WEEK = "WEEK";

  final String MONTH = "MONTH";

  final String DATA = "DATA";

  final String AIRPORTS = "AIRPORTS";

  final String SEPERATOR = "\t";

  final Set<String> WEST_COAST = Collections.unmodifiableSet(
      new HashSet<>( Arrays.asList( "AK", "WA", "OR", "CA" ) ) );

  final Set<String> EAST_COAST = Collections
      .unmodifiableSet( new HashSet<>( Arrays.asList( "ME", "NH", "MA", "RI",
          "CT", "NY", "NJ", "DE", "MD", "VA", "NC", "SC", "GA", "FL", "PA" ) ) );

  final String WEST = "WEST";

  final String EAST = "East";

  final String NONE = "NONE";
}
