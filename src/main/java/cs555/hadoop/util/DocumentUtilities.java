package cs555.hadoop.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class DocumentUtilities {

  /**
   * Split the input document with embedded commas ( , ) in data.
   * 
   * This is equivalent to the regular expression:
   * ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
   * 
   * @param s input to split based on comma
   * @return an <code>ArrayList</code> of the split string
   */
  public static ArrayList<String> splitString(String s) {
    ArrayList<String> words = new ArrayList<String>();

    boolean notInsideComma = true;
    int start = 0;

    for ( int i = 0; i < s.length() - 1; i++ )
    {
      if ( s.charAt( i ) == ',' && notInsideComma )
      {
        words.add( s.substring( start, i ).replace( "\"", "" ).trim() );
        start = i + 1;
      } else if ( s.charAt( i ) == '"' )
        notInsideComma = !notInsideComma;
    }

    words.add( s.substring( start ).replace( "\"", "" ).trim() );

    return words;
  }

  /**
   * Sort the map depending on the specified song data type in ascending
   * or descending order.
   * 
   * @param map HashMap of the values
   * @param type type of data
   * @param descending true for descending, false for ascending
   * @return a new map of sorted <K, V> pairs.=
   */
  public static Map<String, Integer> sortMapByValue(Map<String, Integer> map,
      boolean descending) {

    Comparator<Entry<String, Integer>> comparator =
        Map.Entry.<String, Integer>comparingByValue();

    if ( descending )
    {
      comparator = Collections.reverseOrder( comparator );
    }
    return map.entrySet().stream().sorted( comparator )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e1, LinkedHashMap::new ) );
  }

  /**
   * Returns a new double initialized to the value represented by the
   * specified <code>String</code>.
   * 
   * @param str
   * @return returns number if valid, 0 otherwise.
   */
  public static double parseDouble(String str) {
    try
    {
      return Double.parseDouble( str );
    } catch ( NumberFormatException e )
    {
      return 0;
    }
  }

  /**
   * Returns a new int initialized to the value represented by the
   * specified <code>String</code>.
   * 
   * @param str
   * @return returns number if valid, 0 otherwise.
   */
  public static int parseInt(String str) {
    try
    {
      return Integer.parseInt( str );
    } catch ( NumberFormatException e )
    {
      return 0;
    }
  }
}
