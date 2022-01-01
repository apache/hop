/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.variables.IVariables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Utils {

  // Fairly simple algorithm implemented from pseudo-code algorithm documented on Wikipedia:
  //
  //   https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance
  //
  public static int getDamerauLevenshteinDistance(String one, String two) {
    // A few utility variables
    //
    int oneLength = one == null ? 0 : one.length();
    int twoLength = two == null ? 0 : two.length();
    int oneTwoLength = oneLength + twoLength;

    // A bit of house-keeping for null values...
    //
    if (StringUtils.isEmpty(one)) {
      if (StringUtils.isEmpty(two)) {
        return 0;
      } else {
        return twoLength;
      }
    } else if (StringUtils.isEmpty(two)) {
      return oneLength;
    }

    // A distancesMatrix to keep track of distances between characters.
    // It explores all valid distances in the loop below
    //
    int[][] distancesMatrix = new int[oneLength + 2][twoLength + 2];

    // Initialize the matrix
    // The maximum possible length is the sum of both String lengths
    // The minimum is obviously 0
    //
    distancesMatrix[0][0] = oneTwoLength;
    for (int x = 0; x <= oneLength; x++) {
      distancesMatrix[x + 1][1] = x;
      distancesMatrix[x + 1][0] = oneTwoLength;
    }
    for (int y = 0; y <= twoLength; y++) {
      distancesMatrix[1][y + 1] = y;
      distancesMatrix[0][y + 1] = oneTwoLength;
    }
    Map<Character, Integer> characterMap = new HashMap<>();

    // Loop over string one and calculate distances with characters in string two
    //
    for (int x = 1; x <= oneLength; x++) {
      // A temporary index to have a reference point to compare with later
      // to compare deletion, insertion, substitution and transposition
      //
      int tmpIndex = 0;

      // Loop over string two
      //
      for (int y = 1; y <= twoLength; y++) {
        // 1-based index from pseudo-code
        //
        char charOne = one.charAt(x - 1);
        char charTwo = two.charAt(y - 1);

        int x1 = characterMap.getOrDefault(charTwo, 0);
        int y1 = tmpIndex;

        // Same characters: distance is 0
        int distance = charOne == charTwo ? 0 : 1;
        if (distance == 0) {
          tmpIndex = y;
        }

        // Get the minimum of the 4 distances for deletion, insertion, substitution and
        // transposition
        //
        distancesMatrix[x + 1][y + 1] =
            NumberUtils.min(
                distancesMatrix[x][y] + distance,
                distancesMatrix[x + 1][y] + 1,
                distancesMatrix[x][y + 1] + 1,
                distancesMatrix[x1][y1] + (x - x1 - 1) + 1 + (y - y1 - 1));
      }
      characterMap.put(one.charAt(x - 1), x);
    }
    return distancesMatrix[oneLength + 1][twoLength + 1];
  }

  /**
   * Check if the CharSequence supplied is empty. A CharSequence is empty when it is null or when
   * the length is 0
   *
   * @param val The string to check
   * @return true if the string supplied is empty
   */
  public static boolean isEmpty(CharSequence val) {
    return val == null || val.length() == 0;
  }

  /**
   * Check if the CharSequence array supplied is empty. A CharSequence array is empty when it is
   * null or when the number of elements is 0
   *
   * @param strings The string array to check
   * @return true if the string array supplied is empty
   */
  public static boolean isEmpty(CharSequence[] strings) {
    return strings == null || strings.length == 0;
  }

  /**
   * Check if the array supplied is empty. An array is empty when it is null or when the length is 0
   *
   * @param array The array to check
   * @return true if the array supplied is empty
   */
  public static boolean isEmpty(Object[] array) {
    return array == null || array.length == 0;
  }

  /**
   * Check if the list supplied is empty. An array is empty when it is null or when the length is 0
   *
   * @param list the list to check
   * @return true if the supplied list is empty
   */
  public static boolean isEmpty(List<?> list) {
    return list == null || list.size() == 0;
  }

  /**
   * Resolves password from variable if it's necessary and decrypts if the password was encrypted
   *
   * @param variables IVariables is used for resolving
   * @param password the password for resolving and decrypting
   * @return resolved decrypted password
   */
  public static String resolvePassword(IVariables variables, String password) {
    String resolvedPassword = variables.resolve(password);
    if (resolvedPassword != null) {
      // returns resolved decrypted password
      return Encr.decryptPasswordOptionallyEncrypted(resolvedPassword);
    } else {
      // actually null
      return resolvedPassword;
    }
  }

  /** Normalize String array lengths for synchronization of arrays within transforms */
  public static String[][] normalizeArrays(int normalizeToLength, String[]... arraysToNormalize) {
    if (arraysToNormalize == null) {
      return null;
    }
    int arraysToProcess = arraysToNormalize.length;
    String[][] rtn = new String[arraysToProcess][];
    for (int i = 0; i < arraysToNormalize.length; i++) {
      String[] nextArray = arraysToNormalize[i];
      if (nextArray != null) {
        if (nextArray.length < normalizeToLength) {
          String[] newArray = new String[normalizeToLength];
          System.arraycopy(nextArray, 0, newArray, 0, nextArray.length);
          rtn[i] = newArray;
        } else {
          rtn[i] = nextArray;
        }
      } else {
        rtn[i] = new String[normalizeToLength];
      }
    }
    return rtn;
  }

  /** Normalize long array lengths for synchronization of arrays within transforms */
  public static long[][] normalizeArrays(int normalizeToLength, long[]... arraysToNormalize) {
    if (arraysToNormalize == null) {
      return null;
    }
    int arraysToProcess = arraysToNormalize.length;
    long[][] rtn = new long[arraysToProcess][];
    for (int i = 0; i < arraysToNormalize.length; i++) {
      long[] nextArray = arraysToNormalize[i];
      if (nextArray != null) {
        if (nextArray.length < normalizeToLength) {
          long[] newArray = new long[normalizeToLength];
          System.arraycopy(nextArray, 0, newArray, 0, nextArray.length);
          rtn[i] = newArray;
        } else {
          rtn[i] = nextArray;
        }
      } else {
        rtn[i] = new long[normalizeToLength];
      }
    }
    return rtn;
  }

  /** Normalize int array lengths for synchronization of arrays within transforms */
  public static int[][] normalizeArrays(int normalizeToLength, int[]... arraysToNormalize) {
    if (arraysToNormalize == null) {
      return null;
    }
    int arraysToProcess = arraysToNormalize.length;
    int[][] rtn = new int[arraysToProcess][];
    for (int i = 0; i < arraysToNormalize.length; i++) {
      int[] nextArray = arraysToNormalize[i];
      if (nextArray != null) {
        if (nextArray.length < normalizeToLength) {
          int[] newArray = new int[normalizeToLength];
          System.arraycopy(nextArray, 0, newArray, 0, nextArray.length);
          rtn[i] = newArray;
        } else {
          rtn[i] = nextArray;
        }
      } else {
        rtn[i] = new int[normalizeToLength];
      }
    }
    return rtn;
  }

  /** Normalize boolean array lengths for synchronization of arrays within transforms */
  public static boolean[][] normalizeArrays(int normalizeToLength, boolean[]... arraysToNormalize) {
    if (arraysToNormalize == null) {
      return null;
    }
    int arraysToProcess = arraysToNormalize.length;
    boolean[][] rtn = new boolean[arraysToProcess][];
    for (int i = 0; i < arraysToNormalize.length; i++) {
      boolean[] nextArray = arraysToNormalize[i];
      if (nextArray != null) {
        if (nextArray.length < normalizeToLength) {
          boolean[] newArray = new boolean[normalizeToLength];
          System.arraycopy(nextArray, 0, newArray, 0, nextArray.length);
          rtn[i] = newArray;
        } else {
          rtn[i] = nextArray;
        }
      } else {
        rtn[i] = new boolean[normalizeToLength];
      }
    }
    return rtn;
  }

  /** Normalize short array lengths for synchronization of arrays within transforms */
  public static short[][] normalizeArrays(int normalizeToLength, short[]... arraysToNormalize) {
    if (arraysToNormalize == null) {
      return null;
    }
    int arraysToProcess = arraysToNormalize.length;
    short[][] rtn = new short[arraysToProcess][];
    for (int i = 0; i < arraysToNormalize.length; i++) {
      short[] nextArray = arraysToNormalize[i];
      if (nextArray != null) {
        if (nextArray.length < normalizeToLength) {
          short[] newArray = new short[normalizeToLength];
          System.arraycopy(nextArray, 0, newArray, 0, nextArray.length);
          rtn[i] = newArray;
        } else {
          rtn[i] = nextArray;
        }
      } else {
        rtn[i] = new short[normalizeToLength];
      }
    }
    return rtn;
  }

  public static String getDurationHMS(double seconds) {
    int day = (int) TimeUnit.SECONDS.toDays((long) seconds);
    long hours = TimeUnit.SECONDS.toHours((long) seconds) - (day * 24);
    long minute =
        TimeUnit.SECONDS.toMinutes((long) seconds)
            - (TimeUnit.SECONDS.toHours((long) seconds) * 60);
    long second =
        TimeUnit.SECONDS.toSeconds((long) seconds)
            - (TimeUnit.SECONDS.toMinutes((long) seconds) * 60);
    long ms = (long) ((seconds - ((long) seconds)) * 1000);

    StringBuilder hms = new StringBuilder();
    if (day > 0) {
      hms.append(day + "d ");
    }
    if (day > 0 || hours > 0) {
      hms.append(hours + "h ");
    }
    if (day > 0 || hours > 0 || minute > 0) {
      hms.append(String.format("%2d", minute) + "' ");
    }
    hms.append(String.format("%2d", second));

    // After a few minutes we don't need the microseconds
    //
    if (day == 0 && hours == 0 && minute < 3) {
      hms.append('.').append(String.format("%03d", ms));
    }
    hms.append('"');

    return hms.toString();
  }
}
