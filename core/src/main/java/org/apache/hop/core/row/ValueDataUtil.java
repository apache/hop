/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.row;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopFileNotFoundException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.fileinput.CharsetToolkit;
import org.apache.hop.core.util.HopJaroWinklerDistance;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlCheck;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.security.MessageDigest;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class ValueDataUtil {

  private static final Log log = LogFactory.getLog( ValueDataUtil.class );

  /**
   * System property sets rounding mode of calculator's function ROUND(A,B)
   * <ul>
   * <li>
   * -DROUND_2_MODE=ROUND_HALF_EVEN - provides backward compatibility.</li>
   * <li>
   * -DROUND_2_MODE=ROUND_HALF_CEILING or not specified - makes the effect of ROUND(A,B) like ROUND(A).</li>
   * <li>
   * If incorrect value set - default value used (ROUND_CEILING).</li>
   * </ul>
   */
  private static final String SYS_PROPERTY_ROUND_2_MODE = "ROUND_2_MODE";
  /**
   * Value of system property ROUND_2_MODE
   * Provides correct rounding
   */
  private static final String SYS_PROPERTY_ROUND_2_MODE_DEFAULT_VALUE = "ROUND_HALF_CEILING";
  private static final int ROUND_2_MODE_DEFAULT_VALUE = Const.ROUND_HALF_CEILING;
  /**
   * Value of system property ROUND_2_MODE
   * Provides backward compatibility
   */
  private static final String SYS_PROPERTY_ROUND_2_MODE_BACKWARD_COMPATIBILITY_VALUE = "ROUND_HALF_EVEN";
  private static final int ROUND_2_MODE_BACKWARD_COMPATIBILITY_VALUE = BigDecimal.ROUND_HALF_EVEN;

  /**
   * Rounding mode of the ROUND function with 2 arguments (value, precision).
   * <ul>
   * <li>
   * {@code org.apache.hop.core.Const.ROUND_HALF_CEILING} - ditto as ROUND(value).</li>
   * <li>{@code java.math.BigDecimal.ROUND_HALF_EVEN} - backward compatibility</li>
   * </ul>
   */
  private static int ROUND_2_MODE = readRound2Mode();

  private static int readRound2Mode() {
    int round2Mode = ROUND_2_MODE_DEFAULT_VALUE;
    final String rpaValue = System.getProperty( SYS_PROPERTY_ROUND_2_MODE );
    if ( Utils.isEmpty( rpaValue ) ) {
      round2Mode = ROUND_2_MODE_DEFAULT_VALUE;
      log.debug( "System property is omitted: ROUND_2_MODE. Default value used: " + SYS_PROPERTY_ROUND_2_MODE_DEFAULT_VALUE + "." );
    } else if ( SYS_PROPERTY_ROUND_2_MODE_DEFAULT_VALUE.equals( rpaValue ) ) {
      round2Mode = ROUND_2_MODE_DEFAULT_VALUE;
      log.debug( "System property read: ROUND_2_MODE=" + ROUND_2_MODE_DEFAULT_VALUE + " (default value)" );
    } else if ( SYS_PROPERTY_ROUND_2_MODE_BACKWARD_COMPATIBILITY_VALUE.equalsIgnoreCase( rpaValue ) ) {
      round2Mode = ROUND_2_MODE_BACKWARD_COMPATIBILITY_VALUE;
      log.debug( "System property read: ROUND_2_MODE=" + SYS_PROPERTY_ROUND_2_MODE_BACKWARD_COMPATIBILITY_VALUE
        + " (backward compatibility value)" );
    } else {
      log.warn( "Incorrect value of system property read: ROUND_2_MODE=" + rpaValue + ". Set to " + SYS_PROPERTY_ROUND_2_MODE_DEFAULT_VALUE
        + " instead." );
    }
    return round2Mode;
  }

  private static void throwsErrorOnFileNotFound( FileObject file ) throws HopFileNotFoundException, FileSystemException {
    if ( file == null || !file.exists() ) {
      throw new HopFileNotFoundException( "File not found", file.getName().getPath() );
    }
  }

  /**
   * @deprecated Use {@link Const#ltrim(String)} instead
   */
  @Deprecated
  public static final String leftTrim( String string ) {
    return Const.ltrim( string );
  }

  /**
   * @deprecated Use {@link Const#rtrim(String)} instead
   */
  @Deprecated
  public static final String rightTrim( String string ) {
    return Const.rtrim( string );
  }

  /**
   * Determines whether or not a character is considered a variables. A character is considered a variables in Hop if it is a
   * variables, a tab, a newline or a cariage return.
   *
   * @param c The character to verify if it is a variables.
   * @return true if the character is a variables. false otherwise.
   * @deprecated Use {@link Const#isSpace(char)} instead
   */
  @Deprecated
  public static final boolean isSpace( char c ) {
    return Const.isSpace( c );
  }

  /**
   * Trims a string: removes the leading and trailing spaces of a String.
   *
   * @param string The string to trim
   * @return The trimmed string.
   * @deprecated Use {@link Const#trim(String)} instead
   */
  @Deprecated
  public static final String trim( String string ) {
    return Const.trim( string );
  }

  /**
   * Levenshtein distance (LD) is a measure of the similarity between two strings, which we will refer to as the source
   * string (s) and the target string (t). The distance is the number of deletions, insertions, or substitutions
   * required to transform s into t.
   */
  public static Long getLevenshtein_Distance( IValueMeta metaA, Object dataA, IValueMeta metaB,
                                              Object dataB ) {
    if ( dataA == null || dataB == null ) {
      return null;
    }
    return new Long( StringUtils.getLevenshteinDistance( dataA.toString(), dataB.toString() ) );
  }

  /**
   * DamerauLevenshtein distance is a measure of the similarity between two strings, which we will refer to as the
   * source string (s) and the target string (t). The distance is the number of deletions, insertions, or substitutions
   * required to transform s into t.
   */
  public static Long getDamerauLevenshtein_Distance( IValueMeta metaA, Object dataA,
                                                     IValueMeta metaB, Object dataB ) {
    if ( dataA == null || dataB == null ) {
      return null;
    }
    return new Long( Utils.getDamerauLevenshteinDistance( dataA.toString(), dataB.toString() ) );
  }


  /**
   * Jaro similitude is a measure of the similarity between two strings, which we will refer to as the source string (s)
   * and the target string (t).
   */
  public static Double getJaro_Similitude( IValueMeta metaA, Object dataA, IValueMeta metaB,
                                           Object dataB ) {
    if ( dataA == null || dataB == null ) {
      return null;
    }
    HopJaroWinklerDistance pjwd = new HopJaroWinklerDistance();
    pjwd.apply( dataA.toString(), dataB.toString() );
    return pjwd.getJaroDistance();
  }

  /**
   * JaroWinkler similitude is a measure of the similarity between two strings, which we will refer to as the source
   * string (s) and the target string (t).
   */
  public static Double getJaroWinkler_Similitude( IValueMeta metaA, Object dataA,
                                                  IValueMeta metaB, Object dataB ) {
    if ( dataA == null || dataB == null ) {
      return null;
    }
    HopJaroWinklerDistance pjwd = new HopJaroWinklerDistance();
    pjwd.apply( dataA.toString(), dataB.toString() );
    return pjwd.getJaroWinklerDistance();
  }

  public static String get_Metaphone( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return ( new Metaphone() ).metaphone( dataA.toString() );
  }

  public static String get_Double_Metaphone( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return ( new DoubleMetaphone() ).doubleMetaphone( dataA.toString() );
  }

  public static String get_SoundEx( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return ( new Soundex() ).encode( dataA.toString() );
  }

  public static String get_RefinedSoundEx( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return ( new RefinedSoundex() ).encode( dataA.toString() );
  }

  public static String initCap( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return WordUtils.capitalize( dataA.toString(), new char[] { ' ', '_', ',', ':', ';', '-' } );
  }

  public static String upperCase( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return dataA.toString().toUpperCase();
  }

  public static String lowerCase( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return dataA.toString().toLowerCase();
  }

  public static String escapeXml(IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.escapeXml( dataA.toString() );
  }

  public static String unEscapeXml(IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.unEscapeXml( dataA.toString() );
  }

  public static String escapeHtml(IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.escapeHtml( dataA.toString() );
  }

  public static String unEscapeHtml(IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.unEscapeHtml( dataA.toString() );
  }

  public static String escapeSql(IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.escapeSql( dataA.toString() );
  }

  public static String useCDATA( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return "<![CDATA[" + dataA.toString() + "]]>";

  }

  public static String removeCR( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.removeCR( dataA.toString() );
  }

  public static String removeLF( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.removeLF( dataA.toString() );
  }

  public static String removeCRLF( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.removeCRLF( dataA.toString() );
  }

  public static String removeTAB( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.removeTAB( dataA.toString() );
  }

  public static String getDigits( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.getDigitsOnly( dataA.toString() );
  }

  public static String removeDigits( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return null;
    }
    return Const.removeDigits( dataA.toString() );
  }

  public static long stringLen( IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return 0;
    }
    return dataA.toString().length();
  }

  /**
   * @deprecated Use {@link ValueDataUtil#createChecksum(IValueMeta, Object, String, boolean)} instead
   */
  @Deprecated
  public static String createChecksum( IValueMeta metaA, Object dataA, String type ) {
    String checksum = null;
    try {
      checksum = createChecksum( metaA, dataA, type, false );
    } catch ( HopFileNotFoundException e ) {
      // Ignore
    }
    return checksum;
  }

  /**
   * @param metaA        The IValueMeta
   * @param dataA        Filename
   * @param type         Algorithm to be used when computing the checksum (MD5 or SHA-1)
   * @param failIfNoFile Indicates if the pipeline should fail if no file is found
   * @return File's checksum
   * @throws HopFileNotFoundException
   */
  public static String createChecksum( IValueMeta metaA, Object dataA, String type, boolean failIfNoFile )
    throws HopFileNotFoundException {
    if ( dataA == null ) {
      return null;
    }

    String md5Hash = null;
    FileObject file = null;
    InputStream in = null;
    try {
      file = HopVfs.getFileObject( dataA.toString() );
      throwsErrorOnFileNotFound( file );
      in = HopVfs.getInputStream( file );
      int bytes = in.available();
      byte[] buffer = new byte[ bytes ];
      in.read( buffer );

      StringBuffer md5HashBuff = new StringBuffer( 32 );
      byte[] b = MessageDigest.getInstance( type ).digest( buffer );
      int len = b.length;
      for ( int x = 0; x < len; x++ ) {
        md5HashBuff.append( String.format( "%02x", b[ x ] ) );
      }

      md5Hash = md5HashBuff.toString();

    } catch ( HopFileNotFoundException e ) {
      if ( failIfNoFile ) {
        throw e;
      }
      log.debug( e.getMessage() );
    } catch ( Exception e ) {
      log.debug( e.getMessage() );
    } finally {
      IOUtils.closeQuietly( file );
      IOUtils.closeQuietly( in );
    }
    return md5Hash;
  }

  /**
   * @deprecated Use {@link ValueDataUtil#checksumCRC32(IValueMeta, Object, boolean)} instead
   */
  @Deprecated
  public static Long ChecksumCRC32( IValueMeta metaA, Object dataA ) {
    long checksum = 0;
    try {
      checksum = checksumCRC32( metaA, dataA, false );
    } catch ( HopFileNotFoundException e ) {
      // Ignore
    }
    return checksum;
  }

  /**
   * @param metaA        The IValueMeta
   * @param dataA        Filename
   * @param failIfNoFile Indicates if the pipeline should fail if no file is found
   * @return File's CRC32 checksum
   * @throws HopFileNotFoundException
   */
  public static Long checksumCRC32( IValueMeta metaA, Object dataA, boolean failIfNoFile )
    throws HopFileNotFoundException {
    long checksum = 0;

    if ( dataA == null ) {
      return checksum;
    }

    FileObject file = null;
    CheckedInputStream cis = null;
    try {
      file = HopVfs.getFileObject( dataA.toString() );
      throwsErrorOnFileNotFound( file );
      cis = null;

      // Computer CRC32 checksum
      cis = new CheckedInputStream( HopVfs.getInputStream( file ), new CRC32() );
      byte[] buf = new byte[ 128 ];
      int readSize = 0;
      do {
        readSize = cis.read( buf );
      } while ( readSize >= 0 );

      checksum = cis.getChecksum().getValue();

    } catch ( HopFileNotFoundException e ) {
      if ( failIfNoFile ) {
        throw e;
      }
      log.debug( e.getMessage() );
    } catch ( Exception e ) {
      log.debug( e.getMessage() );
    } finally {
      IOUtils.closeQuietly( file );
      IOUtils.closeQuietly( cis );
    }
    return checksum;
  }

  /**
   * @deprecated Use {@link ValueDataUtil#checksumAdler32(IValueMeta, Object, boolean)} instead
   */
  @Deprecated
  public static Long ChecksumAdler32( IValueMeta metaA, Object dataA ) {
    long checksum = 0;
    try {
      checksum = checksumAdler32( metaA, dataA, false );
    } catch ( HopFileNotFoundException e ) {
      // Ignore
    }
    return checksum;
  }

  /**
   * @param metaA        The IValueMeta
   * @param dataA        Filename
   * @param failIfNoFile Indicates if the pipeline should fail if no file is found
   * @return File's Adler32 checksum
   * @throws HopFileNotFoundException
   */
  public static Long checksumAdler32( IValueMeta metaA, Object dataA, boolean failIfNoFile )
    throws HopFileNotFoundException {
    long checksum = 0;

    if ( dataA == null ) {
      return checksum;
    }

    FileObject file = null;
    CheckedInputStream cis = null;
    try {
      file = HopVfs.getFileObject( dataA.toString() );
      throwsErrorOnFileNotFound( file );
      cis = null;

      // Computer Adler-32 checksum
      cis = new CheckedInputStream( HopVfs.getInputStream( file ), new Adler32() );

      byte[] buf = new byte[ 128 ];
      int readSize = 0;
      do {
        readSize = cis.read( buf );
      } while ( readSize >= 0 );
      checksum = cis.getChecksum().getValue();

    } catch ( HopFileNotFoundException e ) {
      if ( failIfNoFile ) {
        throw e;
      }
      log.debug( e.getMessage() );
    } catch ( Exception e ) {
      log.debug( e.getMessage() );
    } finally {
      IOUtils.closeQuietly( file );
      IOUtils.closeQuietly( cis );
    }
    return checksum;
  }

  public static Object plus( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    if ( dataA == null || dataB == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_STRING:
        return metaA.getString( dataA ) + metaB.getString( dataB );
      case IValueMeta.TYPE_NUMBER: {
        Double valueA = metaA.getNumber( dataA );
        Double valueB = metaB.getNumber( dataB );
        if ( valueB == null ) {
          return valueA;
        } else if ( valueA == null ) {
          return valueB;
        } else {
          return new Double( valueA.doubleValue() + valueB.doubleValue() );
        }
      }
      case IValueMeta.TYPE_INTEGER: {
        Long valueA = metaA.getInteger( dataA );
        Long valueB = metaB.getInteger( dataB );
        if ( valueB == null ) {
          return valueA;
        } else if ( valueA == null ) {
          return valueB;
        } else {
          return new Long( valueA.longValue() + valueB.longValue() );
        }
      }
      case IValueMeta.TYPE_BOOLEAN: {
        Boolean valueA = metaA.getBoolean( dataA );
        Boolean valueB = metaB.getBoolean( dataB );
        if ( valueB == null ) {
          return valueA;
        } else if ( valueA == null ) {
          return valueB;
        } else {
          return Boolean.valueOf( valueA.booleanValue() || valueB.booleanValue() );
        }
      }
      case IValueMeta.TYPE_BIGNUMBER: {
        BigDecimal valueA = metaA.getBigNumber( dataA );
        BigDecimal valueB = metaB.getBigNumber( dataB );
        if ( valueB == null ) {
          return valueA;
        } else if ( valueA == null ) {
          return valueB;
        } else {
          return valueA.add( valueB );
        }
      }
      default:
        throw new HopValueException( "The 'plus' function only works on numeric data and Strings." );
    }
  }

  public static Object plus3( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB,
                              IValueMeta metaC, Object dataC ) throws HopValueException {
    if ( dataA == null || dataB == null || dataC == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_STRING:
        return metaA.getString( dataA ) + metaB.getString( dataB ) + metaC.getString( dataC );
      case IValueMeta.TYPE_NUMBER:
        return new Double( metaA.getNumber( dataA ).doubleValue()
          + metaB.getNumber( dataB ).doubleValue() + metaC.getNumber( dataC ).doubleValue() );
      case IValueMeta.TYPE_INTEGER:
        return new Long( metaA.getInteger( dataA ).longValue()
          + metaB.getInteger( dataB ).longValue() + metaC.getInteger( dataC ).longValue() );
      case IValueMeta.TYPE_BOOLEAN:
        return Boolean.valueOf( metaA.getBoolean( dataA ).booleanValue()
          || metaB.getBoolean( dataB ).booleanValue() || metaB.getBoolean( dataC ).booleanValue() );
      case IValueMeta.TYPE_BIGNUMBER:
        return metaA.getBigNumber( dataA ).add( metaB.getBigNumber( dataB ).add( metaC.getBigNumber( dataC ) ) );

      default:
        throw new HopValueException( "The 'plus' function only works on numeric data and Strings." );
    }
  }

  public static Object sum( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    if ( dataA == null && dataB == null ) {
      return null;
    }
    if ( dataA == null && dataB != null ) {
      Object value = metaA.convertData( metaB, dataB );
      metaA.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
      return value;
    }
    if ( dataA != null && dataB == null ) {
      return dataA;
    }

    return plus( metaA, dataA, metaB, dataB );
  }

  /**
   * @deprecated Use {@link ValueDataUtil#loadFileContentInBinary(IValueMeta, Object, boolean)} instead
   */
  @Deprecated
  public static Object loadFileContentInBinary( IValueMeta metaA, Object dataA ) throws HopValueException {
    Object content = null;
    try {
      content = loadFileContentInBinary( metaA, dataA, true );
    } catch ( HopFileNotFoundException e ) {
      throw new HopValueException();
    }
    return content;
  }

  /**
   * @param metaA        The IValueMeta
   * @param dataA        Filename
   * @param failIfNoFile Indicates if the pipeline should fail if no file is found
   * @return File's content in binary
   * @throws HopValueException
   * @throws HopFileNotFoundException
   */
  public static byte[] loadFileContentInBinary( IValueMeta metaA, Object dataA, boolean failIfNoFile )
    throws HopValueException, HopFileNotFoundException {
    if ( dataA == null ) {
      return null;
    }

    byte[] content = null;
    FileObject file = null;
    InputStream is = null;

    try {
      file = HopVfs.getFileObject( dataA.toString() );
      throwsErrorOnFileNotFound( file );
      is = HopVfs.getInputStream( file );
      int fileSize = (int) file.getContent().getSize();
      content = new byte[ fileSize ];
      is.read( content, 0, fileSize );
    } catch ( HopFileNotFoundException e ) {
      if ( failIfNoFile ) {
        throw e;
      }
      log.debug( e.getMessage() );
    } catch ( Exception e ) {
      throw new HopValueException( e );
    } finally {
      IOUtils.closeQuietly( file );
      IOUtils.closeQuietly( is );
    }
    return content;
  }

  public static Object minus( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    if ( dataA == null || dataB == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( metaA.getNumber( dataA ).doubleValue() - metaB.getNumber( dataB ).doubleValue() );
      case IValueMeta.TYPE_INTEGER:
        return new Long( metaA.getInteger( dataA ).longValue() - metaB.getInteger( dataB ).longValue() );
      case IValueMeta.TYPE_BIGNUMBER:
        return metaA.getBigNumber( dataA ).subtract( metaB.getBigNumber( dataB ) );
      default:
        return new Long( metaA.getInteger( dataA ).longValue() - metaB.getInteger( dataB ).longValue() );
    }
  }

  public static Object multiply( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    if ( dataA == null || dataB == null ) {
      return null;
    }

    if ( ( metaB.isString() && metaA.isNumeric() ) || ( metaB.isNumeric() && metaA.isString() ) ) {
      return multiplyString( metaA, dataA, metaB, dataB );
    }

    return multiplyNumeric( metaA, dataA, metaB, dataB );
  }

  protected static Object multiplyNumeric( IValueMeta metaA, Object dataA, IValueMeta metaB,
                                           Object dataB ) throws HopValueException {
    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return multiplyDoubles( metaA.getNumber( dataA ), metaB.getNumber( dataB ) );
      case IValueMeta.TYPE_INTEGER:
        return multiplyLongs( metaA.getInteger( dataA ), metaB.getInteger( dataB ) );
      case IValueMeta.TYPE_BIGNUMBER:
        return multiplyBigDecimals( metaA.getBigNumber( dataA ), metaB.getBigNumber( dataB ), null );

      default:
        throw new HopValueException(
          "The 'multiply' function only works on numeric data optionally multiplying strings." );
    }
  }

  public static Double multiplyDoubles( Double a, Double b ) {
    return new Double( a.doubleValue() * b.doubleValue() );
  }

  public static Long multiplyLongs( Long a, Long b ) {
    return new Long( a.longValue() * b.longValue() );
  }

  // Get BigNumber size to be considered in mathematical operations
  private static int getMaxPrecision( BigDecimal a, BigDecimal b ) {
    return a.precision() >= b.precision() ? a.precision() : b.precision();
  }

  // Get BigNumber max scale (length of decimal part)
  private static int getMaxScale( BigDecimal a, BigDecimal b ) {
    return a.scale() >= b.scale() ? a.scale() : b.scale();
  }

  // If decimal part has only zeros, remove it. Otherwise scale it to maxScale
  private static BigDecimal removeTrailingZeroFractionOrScale( BigDecimal a, int maxScale ) {
    if ( a.remainder( BigDecimal.ONE ).compareTo( BigDecimal.ZERO ) == 0 ) {
      return a.setScale( 0 );
    }

    return a.setScale( maxScale, RoundingMode.HALF_EVEN );
  }

  public static BigDecimal multiplyBigDecimals( BigDecimal a, BigDecimal b, MathContext mc ) {
    if ( mc == null ) {
      mc = new MathContext( getMaxPrecision( a, b ), RoundingMode.HALF_EVEN );
    }

    return removeTrailingZeroFractionOrScale( a.multiply( b, mc ), getMaxScale( a, b ) );
  }

  protected static Object multiplyString( IValueMeta metaA, Object dataA, IValueMeta metaB,
                                          Object dataB ) throws HopValueException {
    StringBuffer s;
    String append = "";
    int n;
    if ( metaB.isString() ) {
      s = new StringBuffer( metaB.getString( dataB ) );
      append = metaB.getString( dataB );
      n = metaA.getInteger( dataA ).intValue();
    } else {
      s = new StringBuffer( metaA.getString( dataA ) );
      append = metaA.getString( dataA );
      n = metaB.getInteger( dataB ).intValue();
    }

    if ( n == 0 ) {
      s.setLength( 0 );
    } else {
      for ( int i = 1; i < n; i++ ) {
        s.append( append );
      }
    }

    return s.toString();
  }

  public static Object divide( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    if ( dataA == null || dataB == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return divideDoubles( metaA.getNumber( dataA ), metaB.getNumber( dataB ) );
      case IValueMeta.TYPE_INTEGER:
        return divideLongs( metaA.getInteger( dataA ), metaB.getInteger( dataB ) );
      case IValueMeta.TYPE_BIGNUMBER:
        return divideBigDecimals( metaA.getBigNumber( dataA ), metaB.getBigNumber( dataB ), null );

      default:
        throw new HopValueException( "The 'divide' function only works on numeric data." );
    }
  }

  public static Double divideDoubles( Double a, Double b ) {
    return new Double( a.doubleValue() / b.doubleValue() );
  }

  public static Long divideLongs( Long a, Long b ) {
    return new Long( a.longValue() / b.longValue() );
  }

  public static BigDecimal divideBigDecimals( BigDecimal a, BigDecimal b, MathContext mc ) {
    if ( mc == null ) {
      mc = new MathContext( getMaxPrecision( a, b ), RoundingMode.HALF_EVEN );
    }

    BigDecimal result = a.divide( b, mc );
    return removeTrailingZeroFractionOrScale( result, result.scale() );
  }

  public static Object sqrt( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( Math.sqrt( metaA.getNumber( dataA ).doubleValue() ) );
      case IValueMeta.TYPE_INTEGER:
        return new Long( Math.round( Math.sqrt( metaA.getNumber( dataA ).doubleValue() ) ) );
      case IValueMeta.TYPE_BIGNUMBER:
        return BigDecimal.valueOf( Math.sqrt( metaA.getNumber( dataA ).doubleValue() ) );

      default:
        throw new HopValueException( "The 'sqrt' function only works on numeric data." );
    }
  }

  /**
   * 100 * A / B
   *
   * @param metaA
   * @param dataA
   * @param metaB
   * @param dataB
   * @return
   * @throws HopValueException
   */
  public static Object percent1( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    if ( dataA == null || dataB == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return divideDoubles( multiplyDoubles( 100.0D, metaA.getNumber( dataA ) ), metaB.getNumber( dataB ) );
      case IValueMeta.TYPE_INTEGER:
        return divideLongs( multiplyLongs( 100L, metaA.getInteger( dataA ) ), metaB.getInteger( dataB ) );
      case IValueMeta.TYPE_BIGNUMBER:
        return divideBigDecimals(
          multiplyBigDecimals( metaA.getBigNumber( dataA ), new BigDecimal( 100 ), null ), metaB
            .getBigNumber( dataB ), null );

      default:
        throw new HopValueException( "The 'A/B in %' function only works on numeric data" );
    }
  }

  /**
   * A - ( A * B / 100 )
   *
   * @param metaA
   * @param dataA
   * @param metaB
   * @param dataB
   * @return
   * @throws HopValueException
   */
  public static Object percent2( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    if ( dataA == null || dataB == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( metaA.getNumber( dataA ).doubleValue()
          - divideDoubles( multiplyDoubles( metaA.getNumber( dataA ), metaB.getNumber( dataB ) ), 100.0D ) );
      case IValueMeta.TYPE_INTEGER:
        return new Long( metaA.getInteger( dataA ).longValue()
          - divideLongs( multiplyLongs( metaA.getInteger( dataA ), metaB.getInteger( dataB ) ), 100L ) );
      case IValueMeta.TYPE_BIGNUMBER:
        return metaA.getBigNumber( dataA ).subtract(
          divideBigDecimals( multiplyBigDecimals(
            metaB.getBigNumber( dataB ), metaA.getBigNumber( dataA ), null ), new BigDecimal( 100 ), null ) );
      default:
        throw new HopValueException( "The 'A-B%' function only works on numeric data" );
    }
  }

  /**
   * A + ( A * B / 100 )
   *
   * @param metaA
   * @param dataA
   * @param metaB
   * @param dataB
   * @return
   * @throws HopValueException
   */
  public static Object percent3( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    if ( dataA == null || dataB == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( metaA.getNumber( dataA ).doubleValue()
          + divideDoubles( multiplyDoubles( metaA.getNumber( dataA ), metaB.getNumber( dataB ) ), 100.0D ) );
      case IValueMeta.TYPE_INTEGER:
        return new Long( metaA.getInteger( dataA ).longValue()
          + divideLongs( multiplyLongs( metaA.getInteger( dataA ), metaB.getInteger( dataB ) ), 100L ) );
      case IValueMeta.TYPE_BIGNUMBER:
        return metaA.getBigNumber( dataA ).add(
          divideBigDecimals( multiplyBigDecimals(
            metaB.getBigNumber( dataB ), metaA.getBigNumber( dataA ), null ), new BigDecimal( 100 ), null ) );
      default:
        throw new HopValueException( "The 'A+B%' function only works on numeric data" );
    }
  }

  /**
   * A + B * C
   *
   * @param metaA
   * @param dataA
   * @param metaB
   * @param dataB
   * @return
   * @throws HopValueException
   */
  public static Object combination1( IValueMeta metaA, Object dataA, IValueMeta metaB,
                                     Object dataB, IValueMeta metaC, Object dataC ) throws HopValueException {
    if ( dataA == null || dataB == null || dataC == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( metaA.getNumber( dataA ).doubleValue()
          + ( metaB.getNumber( dataB ).doubleValue() * metaC.getNumber( dataC ).doubleValue() ) );
      case IValueMeta.TYPE_INTEGER:
        return new Long( metaA.getInteger( dataA ).longValue()
          + ( metaB.getInteger( dataB ).longValue() * metaC.getInteger( dataC ).longValue() ) );
      case IValueMeta.TYPE_BIGNUMBER:
        return metaA.getBigNumber( dataA ).add(
          multiplyBigDecimals( metaB.getBigNumber( dataB ), metaC.getBigNumber( dataC ), null ) );

      default:
        throw new HopValueException( "The 'combination1' function only works on numeric data" );
    }
  }

  /**
   * SQRT( A*A + B*B )
   *
   * @param metaA
   * @param dataA
   * @param metaB
   * @param dataB
   * @return
   * @throws HopValueException
   */
  public static Object combination2( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    if ( dataA == null || dataB == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( Math.sqrt( metaA.getNumber( dataA ).doubleValue()
          * metaA.getNumber( dataA ).doubleValue() + metaB.getNumber( dataB ).doubleValue()
          * metaB.getNumber( dataB ).doubleValue() ) );

      case IValueMeta.TYPE_INTEGER:
        return new Long( Math.round( Math.sqrt( metaA.getInteger( dataA ).longValue()
          * metaA.getInteger( dataA ).longValue() + metaB.getInteger( dataB ).longValue()
          / metaB.getInteger( dataB ).longValue() ) ) );

      case IValueMeta.TYPE_BIGNUMBER:
        return BigDecimal.valueOf( Math.sqrt( metaA.getNumber( dataA ).doubleValue()
          * metaA.getNumber( dataA ).doubleValue() + metaB.getNumber( dataB ).doubleValue()
          * metaB.getNumber( dataB ).doubleValue() ) );

      default:
        throw new HopValueException( "The 'combination2' function only works on numeric data" );
    }
  }

  /**
   * Rounding with no decimal places (using default rounding method ROUND_HALF_CEILING)
   *
   * @param metaA Metadata of value to round
   * @param dataA Value to round
   * @return The rounded value
   * @throws HopValueException
   */
  public static Object round( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( Math.round( metaA.getNumber( dataA ).doubleValue() ) );
      case IValueMeta.TYPE_INTEGER:
        return metaA.getInteger( dataA );
      case IValueMeta.TYPE_BIGNUMBER:
        return new BigDecimal( Math.round( metaA.getNumber( dataA ).doubleValue() ) );

      default:
        throw new HopValueException( "The 'round' function only works on numeric data" );
    }
  }

  /**
   * Rounding with no decimal places with a given rounding method
   *
   * @param metaA        Metadata of value to round
   * @param dataA        Value to round
   * @param roundingMode The mode for rounding, e.g. java.math.BigDecimal.ROUND_HALF_EVEN
   * @return The rounded value
   * @throws HopValueException
   */
  public static Object round( IValueMeta metaA, Object dataA, int roundingMode ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      // Use overloaded Const.round(value, precision, mode)
      case IValueMeta.TYPE_NUMBER:
        return new Double( Const.round( metaA.getNumber( dataA ), 0, roundingMode ) );
      case IValueMeta.TYPE_INTEGER:
        return new Long( Const.round( metaA.getInteger( dataA ), 0, roundingMode ) );
      case IValueMeta.TYPE_BIGNUMBER:
        return Const.round( metaA.getBigNumber( dataA ), 0, roundingMode );
      default:
        throw new HopValueException( "The 'round' function only works on numeric data" );
    }
  }

  /**
   * Rounding with decimal places (using default rounding method ROUND_HALF_EVEN)
   *
   * @param metaA Metadata of value to round
   * @param dataA Value to round
   * @param metaB Metadata of decimal places
   * @param dataB decimal places
   * @return The rounded value
   * @throws HopValueException
   */
  public static Object round( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB )
    throws HopValueException {
    final Object r = round( metaA, dataA, metaB, dataB, ROUND_2_MODE );
    return r;
  }

  /**
   * Rounding with decimal places with a given rounding method
   *
   * @param metaA        Metadata of value to round
   * @param dataA        Value to round
   * @param metaB        Metadata of decimal places
   * @param dataB        decimal places
   * @param roundingMode roundingMode The mode for rounding, e.g. java.math.BigDecimal.ROUND_HALF_EVEN
   * @return The rounded value
   * @throws HopValueException
   */
  public static Object round( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB,
                              int roundingMode ) throws HopValueException {
    if ( dataA == null || dataB == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( Const.round( metaA.getNumber( dataA ).doubleValue(), metaB.getInteger( dataB ).intValue(),
          roundingMode ) );
      case IValueMeta.TYPE_INTEGER:
        return new Long( Const.round( metaA.getInteger( dataA ).longValue(), metaB.getInteger( dataB ).intValue(),
          roundingMode ) );
      case IValueMeta.TYPE_BIGNUMBER:
        return Const.round( metaA.getBigNumber( dataA ), metaB.getInteger( dataB ).intValue(), roundingMode );
      default:
        throw new HopValueException( "The 'round' function only works on numeric data" );
    }
  }

  /**
   * Rounding with decimal places with a given rounding method
   *
   * @param metaA Metadata of value to round
   * @param dataA Value to round
   * @param metaB Metadata of decimal places
   * @param dataB decimal places
   * @param metaC Metadata of rounding mode
   * @param dataC rounding mode, e.g. java.math.BigDecimal.ROUND_HALF_EVEN
   * @return The rounded value
   * @throws HopValueException
   */
  public static Object round( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB,
                              IValueMeta metaC, Object dataC ) throws HopValueException {
    if ( dataA == null || dataB == null || dataC == null ) {
      return null;
    }
    Long valueC = metaC.getInteger( dataC );
    if ( valueC == null || valueC < Const.ROUND_HALF_CEILING || valueC > BigDecimal.ROUND_HALF_EVEN ) {
      throw new HopValueException( "The 'round_custom' arg C has incorrect value: " + valueC );
    }
    int roundingMode = valueC.intValue();
    return round( metaA, dataA, metaB, dataB, roundingMode );
  }

  public static Object ceil( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }
    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( Math.ceil( metaA.getNumber( dataA ).doubleValue() ) );
      case IValueMeta.TYPE_INTEGER:
        return metaA.getInteger( dataA );
      case IValueMeta.TYPE_BIGNUMBER:
        return new BigDecimal( Math.ceil( metaA.getNumber( dataA ).doubleValue() ) );

      default:
        throw new HopValueException( "The 'ceil' function only works on numeric data" );
    }
  }

  public static Object floor( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }
    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( Math.floor( metaA.getNumber( dataA ).doubleValue() ) );
      case IValueMeta.TYPE_INTEGER:
        return metaA.getInteger( dataA );
      case IValueMeta.TYPE_BIGNUMBER:
        return new BigDecimal( Math.floor( metaA.getNumber( dataA ).doubleValue() ) );

      default:
        throw new HopValueException( "The 'floor' function only works on numeric data" );
    }
  }

  public static Object abs( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( Math.abs( metaA.getNumber( dataA ).doubleValue() ) );
      case IValueMeta.TYPE_INTEGER:
        return metaA.getInteger( Math.abs( metaA.getNumber( dataA ).longValue() ) );
      case IValueMeta.TYPE_BIGNUMBER:
        return new BigDecimal( Math.abs( metaA.getNumber( dataA ).doubleValue() ) );

      default:
        throw new HopValueException( "The 'abs' function only works on numeric data" );
    }
  }

  /**
   * Returns the remainder (modulus) of A / B.
   *
   * @param metaA
   * @param dataA The dividend
   * @param metaB
   * @param dataB The divisor
   * @return The remainder
   * @throws HopValueException
   */
  public static Object remainder( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    if ( dataA == null || dataB == null ) {
      return null;
    }

    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_NUMBER:
        return new Double( metaA.getNumber( dataA ).doubleValue() % metaB.getNumber( dataB ).doubleValue() );
      case IValueMeta.TYPE_INTEGER:
        return new Long( metaA.getInteger( dataA ) % metaB.getInteger( dataB ) );
      case IValueMeta.TYPE_BIGNUMBER:
        BigDecimal aValue = metaA.getBigNumber( dataA );
        BigDecimal bValue = metaA.getBigNumber( dataB );
        BigDecimal result = aValue.remainder( bValue, new MathContext( getMaxPrecision( aValue, bValue ), RoundingMode.HALF_EVEN ) );
        return removeTrailingZeroFractionOrScale( result, result.scale() );
      default:
        throw new HopValueException( "The 'remainder' function only works on numeric data" );
    }
  }

  public static Object nvl( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {
    switch ( metaA.getType() ) {
      case IValueMeta.TYPE_STRING:
        if ( dataA == null ) {
          return metaB.getString( dataB );
        } else {
          return metaA.getString( dataA );
        }

      case IValueMeta.TYPE_NUMBER:
        if ( dataA == null ) {
          return metaB.getNumber( dataB );
        } else {
          return metaA.getNumber( dataA );
        }

      case IValueMeta.TYPE_INTEGER:
        if ( dataA == null ) {
          return metaB.getInteger( dataB );
        } else {
          return metaA.getInteger( dataA );
        }

      case IValueMeta.TYPE_BIGNUMBER:
        if ( dataA == null ) {
          return metaB.getBigNumber( dataB );
        } else {
          return metaA.getBigNumber( dataA );
        }

      case IValueMeta.TYPE_DATE:
        if ( dataA == null ) {
          return metaB.getDate( dataB );
        } else {
          return metaA.getDate( dataA );
        }

      case IValueMeta.TYPE_BOOLEAN:
        if ( dataA == null ) {
          return metaB.getBoolean( dataB );
        } else {
          return metaA.getBoolean( dataA );
        }

      case IValueMeta.TYPE_BINARY:
        if ( dataA == null ) {
          return metaB.getBinary( dataB );
        } else {
          return metaA.getBinary( dataA );
        }

      default:
        if ( dataA == null ) {
          return metaB.getNativeDataType( dataB );
        } else {
          return metaA.getNativeDataType( dataA );
        }
    }
  }

  public static Object removeTimeFromDate( IValueMeta metaA, Object dataA ) throws HopValueException {
    Calendar cal = Calendar.getInstance();
    Date date = metaA.getDate( dataA );
    if ( date != null ) {
      cal.setTime( date );
      return Const.removeTimeFromDate( date );
    } else {
      return null;
    }
  }

  public static Object addTimeToDate( IValueMeta metaA, Object dataA, IValueMeta metaB,
                                      Object dataB, IValueMeta metaC, Object dataC ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    try {
      if ( dataC == null ) {
        return Const.addTimeToDate( metaA.getDate( dataA ), metaB.getString( dataB ), null );
      } else {
        return Const.addTimeToDate( metaA.getDate( dataA ), metaB.getString( dataB ), metaC.getString( dataC ) );
      }
    } catch ( Exception e ) {
      throw new HopValueException( e );
    }
  }

  public static Object addDays( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {

    Calendar cal = Calendar.getInstance();
    cal.setTime( metaA.getDate( dataA ) );
    cal.add( Calendar.DAY_OF_YEAR, metaB.getInteger( dataB ).intValue() );

    return cal.getTime();
  }

  public static Object addHours( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {

    Calendar cal = Calendar.getInstance();
    cal.setTime( metaA.getDate( dataA ) );
    cal.add( Calendar.HOUR_OF_DAY, metaB.getInteger( dataB ).intValue() );

    return cal.getTime();
  }

  public static Object addMinutes( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {

    Calendar cal = Calendar.getInstance();
    cal.setTime( metaA.getDate( dataA ) );
    cal.add( Calendar.MINUTE, metaB.getInteger( dataB ).intValue() );

    return cal.getTime();
  }

  public static Object addSeconds( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {

    Calendar cal = Calendar.getInstance();
    cal.setTime( metaA.getDate( dataA ) );
    cal.add( Calendar.SECOND, metaB.getInteger( dataB ).intValue() );

    return cal.getTime();
  }

  public static Object addMonths( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB ) throws HopValueException {

    if ( dataA != null && dataB != null ) {
      Calendar cal = Calendar.getInstance();
      cal.setTime( metaA.getDate( dataA ) );
      int year = cal.get( Calendar.YEAR );
      int month = cal.get( Calendar.MONTH );
      int day = cal.get( Calendar.DAY_OF_MONTH );

      month += metaB.getInteger( dataB ).intValue();

      int newyear = year + (int) Math.floor( month / 12 );
      int newmonth = month % 12;

      cal.set( newyear, newmonth, 1 );
      int newday = cal.getActualMaximum( Calendar.DAY_OF_MONTH );
      if ( newday < day ) {
        cal.set( Calendar.DAY_OF_MONTH, newday );
      } else {
        cal.set( Calendar.DAY_OF_MONTH, day );
      }

      return ( cal.getTime() );
    } else {
      throw new HopValueException( "Unable to add months with a null value" );
    }

  }

  /**
   * Returns the number of days that have elapsed between dataA and dataB.
   *
   * @param metaA
   * @param dataA      The "end date"
   * @param metaB
   * @param dataB      The "start date"
   * @param resultType The "result type" (ms, s, mn, h, d)
   * @return Number of days
   * @throws HopValueException
   */

  public static Object DateDiff( IValueMeta metaA, Object dataA, IValueMeta metaB, Object dataB,
                                 String resultType ) throws HopValueException {

    if ( dataA != null && dataB != null ) {
      Date startDate = metaB.getDate( dataB );
      Date endDate = metaA.getDate( dataA );

      Calendar stDateCal = Calendar.getInstance();
      Calendar endDateCal = Calendar.getInstance();
      stDateCal.setTime( startDate );
      endDateCal.setTime( endDate );

      long endL = endDateCal.getTimeInMillis() + endDateCal.getTimeZone().getOffset( endDateCal.getTimeInMillis() );
      long startL = stDateCal.getTimeInMillis() + stDateCal.getTimeZone().getOffset( stDateCal.getTimeInMillis() );
      long diff = endL - startL;

      if ( Utils.isEmpty( resultType ) ) {
        return new Long( diff / 86400000 );
      } else if ( resultType.equals( "ms" ) ) {
        return new Long( diff );
      } else if ( resultType.equals( "s" ) ) {
        return new Long( diff / 1000 ); // second
      } else if ( resultType.equals( "mn" ) ) {
        return new Long( diff / 60000 ); // minute
      } else if ( resultType.equals( "h" ) ) {
        return new Long( diff / 3600000 ); // hour
      } else if ( resultType.equals( "d" ) ) {
        return new Long( diff / 86400000 );
      } else {
        throw new HopValueException( "Unknown result type option '" + resultType + "'" );
      }
    } else {
      return null;
    }
  }

  public static Object DateWorkingDiff( IValueMeta metaA, Object dataA, IValueMeta metaB,
                                        Object dataB ) throws HopValueException {
    if ( dataA != null && dataB != null ) {
      Date fromDate = metaB.getDate( dataB );
      Date toDate = metaA.getDate( dataA );
      boolean singminus = false;

      if ( fromDate.after( toDate ) ) {
        singminus = true;
        Date temp = fromDate;
        fromDate = toDate;
        toDate = temp;
      }
      Calendar calFrom = Calendar.getInstance();
      calFrom.setTime( fromDate );
      Calendar calTo = Calendar.getInstance();
      calTo.setTime( toDate );
      int iNoOfWorkingDays = 0;
      do {
        if ( calFrom.get( Calendar.DAY_OF_WEEK ) != Calendar.SATURDAY
          && calFrom.get( Calendar.DAY_OF_WEEK ) != Calendar.SUNDAY ) {
          iNoOfWorkingDays += 1;
        }
        calFrom.add( Calendar.DATE, 1 );
      } while ( calFrom.getTimeInMillis() <= calTo.getTimeInMillis() );
      return new Long( singminus ? -iNoOfWorkingDays : iNoOfWorkingDays );
    } else {
      return null;
    }
  }

  public static Object yearOfDate( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime( metaA.getDate( dataA ) );
    return new Long( calendar.get( Calendar.YEAR ) );

  }

  public static Object monthOfDate( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime( metaA.getDate( dataA ) );
    return new Long( calendar.get( Calendar.MONTH ) + 1 );

  }

  public static Object quarterOfDate( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime( metaA.getDate( dataA ) );
    return new Long( ( calendar.get( Calendar.MONTH ) + 3 ) / 3 );
  }

  public static Object dayOfYear( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime( metaA.getDate( dataA ) );
    return new Long( calendar.get( Calendar.DAY_OF_YEAR ) );
  }

  public static Object dayOfMonth( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime( metaA.getDate( dataA ) );
    return new Long( calendar.get( Calendar.DAY_OF_MONTH ) );
  }

  public static Object hourOfDay( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime( metaA.getDate( dataA ) );

    Boolean oldDateCalculation = Boolean.parseBoolean(
      Const.getEnvironmentVariable( Const.HOP_COMPATIBILITY_CALCULATION_TIMEZONE_DECOMPOSITION, "false" ) );
    if ( !oldDateCalculation ) {
      calendar.setTimeZone( metaA.getDateFormatTimeZone() );
    }

    return new Long( calendar.get( Calendar.HOUR_OF_DAY ) );
  }

  public static Object minuteOfHour( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime( metaA.getDate( dataA ) );
    return new Long( calendar.get( Calendar.MINUTE ) );
  }

  public static Object secondOfMinute( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime( metaA.getDate( dataA ) );
    return new Long( calendar.get( Calendar.SECOND ) );
  }

  public static Object dayOfWeek( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime( metaA.getDate( dataA ) );
    return new Long( calendar.get( Calendar.DAY_OF_WEEK ) );
  }

  public static Object weekOfYear( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance();
    calendar.setTime( metaA.getDate( dataA ) );
    return new Long( calendar.get( Calendar.WEEK_OF_YEAR ) );
  }

  public static Object weekOfYearISO8601( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance( Locale.ENGLISH );
    calendar.setMinimalDaysInFirstWeek( 4 );
    calendar.setFirstDayOfWeek( Calendar.MONDAY );
    calendar.setTime( metaA.getDate( dataA ) );
    return new Long( calendar.get( Calendar.WEEK_OF_YEAR ) );

  }

  public static Object yearOfDateISO8601( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    Calendar calendar = Calendar.getInstance( Locale.ENGLISH );
    calendar.setMinimalDaysInFirstWeek( 4 );
    calendar.setFirstDayOfWeek( Calendar.MONDAY );
    calendar.setTime( metaA.getDate( dataA ) );

    int week = calendar.get( Calendar.WEEK_OF_YEAR );
    int month = calendar.get( Calendar.MONTH );
    int year = calendar.get( Calendar.YEAR );

    // fix up for the year taking into account ISO8601 weeks
    if ( week >= 52 && month == 0 ) {
      year--;
    }
    if ( week <= 2 && month == 11 ) {
      year++;
    }

    return new Long( year );
  }

  /**
   * Change a hexadecimal string into normal ASCII representation. E.g. if Value contains string "61" afterwards it
   * would contain value "a". If the hexadecimal string is of odd length a leading zero will be used.
   * <p>
   * Note that only the low byte of a character will be processed, this is for binary pipelines.
   *
   * @return Value itself
   * @throws HopValueException
   */
  public static String hexToByteDecode( IValueMeta meta, Object data ) throws HopValueException {
    if ( meta.isNull( data ) ) {
      return null;
    }

    String hexString = meta.getString( data );

    int len = hexString.length();
    char[] chArray = new char[ ( len + 1 ) / 2 ];
    boolean evenByte = true;
    int nextByte = 0;

    // we assume a leading 0 if the length is not even.
    if ( ( len % 2 ) == 1 ) {
      evenByte = false;
    }

    int nibble;
    int i, j;
    for ( i = 0, j = 0; i < len; i++ ) {
      char c = hexString.charAt( i );

      if ( ( c >= '0' ) && ( c <= '9' ) ) {
        nibble = c - '0';
      } else if ( ( c >= 'A' ) && ( c <= 'F' ) ) {
        nibble = c - 'A' + 0x0A;
      } else if ( ( c >= 'a' ) && ( c <= 'f' ) ) {
        nibble = c - 'a' + 0x0A;
      } else {
        throw new HopValueException( "invalid hex digit '" + c + "'." );
      }

      if ( evenByte ) {
        nextByte = ( nibble << 4 );
      } else {
        nextByte += nibble;
        chArray[ j ] = (char) nextByte;
        j++;
      }

      evenByte = !evenByte;
    }
    return new String( chArray );
  }

  /**
   * Change a string into its hexadecimal representation. E.g. if Value contains string "a" afterwards it would contain
   * value "0061".
   * <p>
   * Note that pipelines happen in groups of 4 hex characters, so the value of a characters is always in the range
   * 0-65535.
   *
   * @return
   * @throws HopValueException
   */
  public static String byteToHexEncode( IValueMeta metaA, Object dataA ) throws HopValueException {
    if ( dataA == null ) {
      return null;
    }

    final char[] hexDigits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    // depending on the use case, this code might deliver the wrong values due to extra conversion with toCharArray
    // see Checksum transform
    // "Add Checksum transform gives incorrect results (MD5, CRC32, ADLER32, SHA-1 are affected)"
    String hex = metaA.getString( dataA );

    char[] s = hex.toCharArray();
    StringBuffer hexString = new StringBuffer( 2 * s.length );

    for ( int i = 0; i < s.length; i++ ) {
      hexString.append( hexDigits[ ( s[ i ] & 0x00F0 ) >> 4 ] ); // hi nibble
      hexString.append( hexDigits[ s[ i ] & 0x000F ] ); // lo nibble
    }

    return hexString.toString();
  }

  /**
   * Change a string into its hexadecimal representation. E.g. if Value contains string "a" afterwards it would contain
   * value "0061".
   * <p>
   * Note that pipelines happen in groups of 4 hex characters, so the value of a characters is always in the range
   * 0-65535.
   *
   * @return A string with Hex code
   * @throws HopValueException In case of a data conversion problem.
   */
  public static String charToHexEncode( IValueMeta meta, Object data ) throws HopValueException {
    final char[] hexDigits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    if ( meta.isNull( data ) ) {
      return null;
    }

    String hex = meta.getString( data );

    char[] s = hex.toCharArray();
    StringBuffer hexString = new StringBuffer( 2 * s.length );

    for ( int i = 0; i < s.length; i++ ) {
      hexString.append( hexDigits[ ( s[ i ] & 0xF000 ) >> 12 ] ); // hex 1
      hexString.append( hexDigits[ ( s[ i ] & 0x0F00 ) >> 8 ] ); // hex 2
      hexString.append( hexDigits[ ( s[ i ] & 0x00F0 ) >> 4 ] ); // hex 3
      hexString.append( hexDigits[ s[ i ] & 0x000F ] ); // hex 4
    }

    return hexString.toString();
  }

  /**
   * Change a hexadecimal string into normal ASCII representation. E.g. if Value contains string "61" afterwards it
   * would contain value "a". If the hexadecimal string is of a wrong length leading zeroes will be used.
   * <p>
   * Note that pipelines happen in groups of 4 hex characters, so the value of a characters is always in the range
   * 0-65535.
   *
   * @return A hex-to-char decoded String
   * @throws HopValueException
   */
  public static String hexToCharDecode( IValueMeta meta, Object data ) throws HopValueException {
    if ( meta.isNull( data ) ) {
      return null;
    }

    String hexString = meta.getString( data );

    int len = hexString.length();
    char[] chArray = new char[ ( len + 3 ) / 4 ];
    int charNr;
    int nextChar = 0;

    // we assume a leading 0s if the length is not right.
    charNr = ( len % 4 );
    if ( charNr == 0 ) {
      charNr = 4;
    }

    int nibble;
    int i, j;
    for ( i = 0, j = 0; i < len; i++ ) {
      char c = hexString.charAt( i );

      if ( ( c >= '0' ) && ( c <= '9' ) ) {
        nibble = c - '0';
      } else if ( ( c >= 'A' ) && ( c <= 'F' ) ) {
        nibble = c - 'A' + 0x0A;
      } else if ( ( c >= 'a' ) && ( c <= 'f' ) ) {
        nibble = c - 'a' + 0x0A;
      } else {
        throw new HopValueException( "invalid hex digit '" + c + "'." );
      }

      if ( charNr == 4 ) {
        nextChar = ( nibble << 12 );
        charNr--;
      } else if ( charNr == 3 ) {
        nextChar += ( nibble << 8 );
        charNr--;
      } else if ( charNr == 2 ) {
        nextChar += ( nibble << 4 );
        charNr--;
      } else {
        // charNr == 1
        nextChar += nibble;
        chArray[ j ] = (char) nextChar;
        charNr = 4;
        j++;
      }
    }

    return new String( chArray );
  }

  /**
   * Right pad a string: adds spaces to a string until a certain length. If the length is smaller then the limit
   * specified, the String is truncated.
   *
   * @param ret   The string to pad
   * @param limit The desired length of the padded string.
   * @return The padded String.
   */
  public static final String rightPad( String ret, int limit ) {
    return Const.rightPad( ret, limit );
  }

  /**
   * Right pad a StringBuffer: adds spaces to a string until a certain length. If the length is smaller then the limit
   * specified, the String is truncated.
   *
   * @param ret   The StringBuffer to pad
   * @param limit The desired length of the padded string.
   * @return The padded String.
   */
  public static final String rightPad( StringBuffer ret, int limit ) {
    return Const.rightPad( ret, limit );
  }

  /**
   * Replace value occurances in a String with another value.
   *
   * @param string The original String.
   * @param repl   The text to replace
   * @param with   The new text bit
   * @return The resulting string with the text pieces replaced.
   */
  public static final String replace( String string, String repl, String with ) {
    StringBuffer str = new StringBuffer( string );
    for ( int i = str.length() - 1; i >= 0; i-- ) {
      if ( str.substring( i ).startsWith( repl ) ) {
        str.delete( i, i + repl.length() );
        str.insert( i, with );
      }
    }
    return str.toString();
  }

  /**
   * Alternate faster version of string replace using a stringbuffer as input.
   *
   * @param str  The string where we want to replace in
   * @param code The code to search for
   * @param repl The replacement string for code
   */
  public static void replaceBuffer( StringBuffer str, String code, String repl ) {
    int clength = code.length();

    int i = str.length() - clength;

    while ( i >= 0 ) {
      String look = str.substring( i, i + clength );
      if ( look.equalsIgnoreCase( code ) ) {
        // Look for a match!
        str.replace( i, i + clength, repl );
      }
      i--;
    }
  }

  /**
   * Count the number of spaces to the left of a text. (leading)
   *
   * @param field The text to examine
   * @return The number of leading spaces found.
   */
  public static final int nrSpacesBefore( String field ) {
    int nr = 0;
    int len = field.length();
    while ( nr < len && field.charAt( nr ) == ' ' ) {
      nr++;
    }
    return nr;
  }

  /**
   * Count the number of spaces to the right of a text. (trailing)
   *
   * @param field The text to examine
   * @return The number of trailing spaces found.
   */
  public static final int nrSpacesAfter( String field ) {
    int nr = 0;
    int len = field.length();
    while ( nr < len && field.charAt( field.length() - 1 - nr ) == ' ' ) {
      nr++;
    }
    return nr;
  }

  /**
   * Checks whether or not a String consists only of spaces.
   *
   * @param str The string to check
   * @return true if the string has nothing but spaces.
   */
  public static final boolean onlySpaces( String str ) {
    for ( int i = 0; i < str.length(); i++ ) {
      if ( !isSpace( str.charAt( i ) ) ) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks an xml file is well formed.
   *
   * @param metaA        The IValueMeta
   * @param dataA        The value (filename)
   * @param failIfNoFile Indicates if the pipeline should fail if no file is found
   * @return true if the file is well formed.
   * @throws HopFileNotFoundException
   */
  public static boolean isXmlFileWellFormed(IValueMeta metaA, Object dataA, boolean failIfNoFile )
    throws HopFileNotFoundException {
    if ( dataA == null ) {
      return false;
    }

    String filename = dataA.toString();
    FileObject file = null;
    try {
      file = HopVfs.getFileObject( filename );
      throwsErrorOnFileNotFound( file );
      return XmlCheck.isXmlFileWellFormed( file );
    } catch ( HopFileNotFoundException e ) {
      if ( failIfNoFile ) {
        throw e;
      }
      log.debug( e.getMessage() );
    } catch ( Exception e ) {
      log.debug( e.getMessage() );
    } finally {
      IOUtils.closeQuietly( file );
    }
    return false;
  }

  /**
   * Checks an xml string is well formed.
   *
   * @param metaA The IValueMeta
   * @param dataA The value (filename)
   * @return true if the file is well formed.
   */
  public static boolean isXmlWellFormed(IValueMeta metaA, Object dataA ) {
    if ( dataA == null ) {
      return false;
    }
    try {
      return XmlCheck.isXmlWellFormed( new ByteArrayInputStream( metaA.getBinary( dataA ) ) );
    } catch ( Exception e ) {
      log.debug( e.getMessage() );
    }
    return false;
  }

  /**
   * Get file encoding.
   *
   * @param metaA The IValueMeta
   * @param dataA The value (filename)
   * @return file encoding.
   * @deprecated Use {@link ValueDataUtil#getFileEncoding(IValueMeta, Object, boolean)} instead
   */
  @Deprecated
  public static String getFileEncoding( IValueMeta metaA, Object dataA ) throws HopValueException {
    String encoding = null;
    try {
      encoding = getFileEncoding( metaA, dataA, true );
    } catch ( HopFileNotFoundException e ) {
      throw new HopValueException();
    }
    return encoding;
  }

  /**
   * Get file encoding.
   *
   * @param metaA        The IValueMeta
   * @param dataA        The value (filename)
   * @param failIfNoFile Indicates if the pipeline should fail if no file is found
   * @return file encoding.
   * @throws HopFileNotFoundException
   * @throws HopValueException
   */
  public static String getFileEncoding( IValueMeta metaA, Object dataA, boolean failIfNoFile )
    throws HopValueException, HopFileNotFoundException {
    if ( dataA == null ) {
      return null;
    }

    String encoding = null;
    FileObject file = null;
    try {
      file = HopVfs.getFileObject( metaA.getString( dataA ) );
      throwsErrorOnFileNotFound( file );
      encoding = CharsetToolkit.guessEncodingName( file );
    } catch ( HopFileNotFoundException e ) {
      if ( failIfNoFile ) {
        throw e;
      }
      log.debug( e.getMessage() );
    } catch ( Exception e ) {
      throw new HopValueException( e );
    } finally {
      IOUtils.closeQuietly( file );
    }
    return encoding;
  }

  /**
   * Default utility method to get exact zero value according to IValueMeta. Using
   * this utility method saves from ClassCastExceptions later.
   *
   * @param type
   * @return
   * @throws HopValueException
   */
  public static Object getZeroForValueMetaType( IValueMeta type ) throws HopValueException {
    if ( type == null ) {
      throw new HopValueException( "API error. IValueMeta can't be null!" );
    }

    switch ( type.getType() ) {
      case ( IValueMeta.TYPE_INTEGER ): {
        return new Long( 0 );
      }
      case ( IValueMeta.TYPE_NUMBER ): {
        return new Double( 0 );
      }
      case ( IValueMeta.TYPE_BIGNUMBER ): {
        return new BigDecimal( 0 );
      }
      case ( IValueMeta.TYPE_STRING ): {
        return "";
      }
      default: {
        throw new HopValueException( "get zero function undefined for data type: " + type.getType() );
      }
    }
  }
}
