package cs555.hadoop.util;

import java.util.ArrayList;

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
        words.add( s.substring( start, i ) );
        start = i + 1;
      } else if ( s.charAt( i ) == '"' )
        notInsideComma = !notInsideComma;
    }

    words.add( s.substring( start ) );

    return words;
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
