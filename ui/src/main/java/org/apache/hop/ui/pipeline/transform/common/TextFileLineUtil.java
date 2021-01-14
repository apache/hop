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

package org.apache.hop.ui.pipeline.transform.common;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.file.EncodingType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class TextFileLineUtil {
  private static final Class<?> PKG = TextFileLineUtil.class; // For Translator

  public static final int FILE_FORMAT_DOS = 0;
  public static final int FILE_FORMAT_UNIX = 1;
  public static final int FILE_FORMAT_MIXED = 2;
  public static final int FILE_TYPE_CSV = 0;
  public static final int FILE_TYPE_FIXED = 1;

  public static final String getLine( ILogChannel log, InputStreamReader reader, int formatNr,
                                      StringBuilder line ) throws HopFileException {
    EncodingType type = EncodingType.guessEncodingType( reader.getEncoding() );
    return getLine( log, reader, type, formatNr, line );
  }

  public static final String getLine( ILogChannel log, InputStreamReader reader, EncodingType encodingType,
                                      int formatNr, StringBuilder line ) throws HopFileException {
    int c = 0;
    line.setLength( 0 );
    try {
      switch ( formatNr ) {
        case FILE_FORMAT_DOS:
          while ( c >= 0 ) {
            c = reader.read();

            if ( encodingType.isReturn( c ) || encodingType.isLinefeed( c ) ) {
              c = reader.read(); // skip \n and \r
              if ( !encodingType.isReturn( c ) && !encodingType.isLinefeed( c ) ) {
                // make sure its really a linefeed or cariage return
                // raise an error this is not a DOS file
                // so we have pulled a character from the next line
                throw new HopFileException( BaseMessages.getString( PKG, "TextFileLineUtil.Log.SingleLineFound" ) );
              }
              return line.toString();
            }
            if ( c >= 0 ) {
              line.append( (char) c );
            }
          }
          break;
        case FILE_FORMAT_UNIX:
          while ( c >= 0 ) {
            c = reader.read();

            if ( encodingType.isLinefeed( c ) || encodingType.isReturn( c ) ) {
              return line.toString();
            }
            if ( c >= 0 ) {
              line.append( (char) c );
            }
          }
          break;
        case FILE_FORMAT_MIXED:
          // in mixed mode we suppose the LF is the last char and CR is ignored
          // not for MAC OS 9 but works for Mac OS X. Mac OS 9 can use UNIX-Format
          while ( c >= 0 ) {
            c = reader.read();

            if ( encodingType.isLinefeed( c ) ) {
              return line.toString();
            } else if ( !encodingType.isReturn( c ) ) {
              if ( c >= 0 ) {
                line.append( (char) c );
              }
            }
          }
          break;
        default:
          break;
      }
    } catch ( HopFileException e ) {
      throw e;
    } catch ( Exception e ) {
      if ( line.length() == 0 ) {
        throw new HopFileException( BaseMessages.getString( PKG, "TextFileLineUtil.Log.Error.ExceptionReadingLine", e
          .toString() ), e );
      }
      return line.toString();
    }
    if ( line.length() > 0 ) {
      return line.toString();
    }

    return null;
  }

  /**
   * This method is borrowed from TextFileInput
   *
   * @param log             logger
   * @param line            line to analyze
   * @param delimiter       delimiter used
   * @param enclosure       enclosure used
   * @param escapeCharacter escape character used
   * @return list of string detected
   * @throws HopException
   */
  public static String[] guessStringsFromLine( ILogChannel log, String line, String delimiter,
                                               String enclosure, String escapeCharacter ) throws HopException {
    List<String> strings = new ArrayList<>();

    String pol; // piece of line

    try {
      if ( line == null ) {
        return null;
      }

      // Split string in pieces, only for CSV!

      int pos = 0;
      int length = line.length();
      boolean dencl = false;

      int len_encl = ( enclosure == null ? 0 : enclosure.length() );
      int lenEsc = ( escapeCharacter == null ? 0 : escapeCharacter.length() );

      while ( pos < length ) {
        int from = pos;
        int next;

        boolean encl_found;
        boolean containsEscaped_enclosures = false;
        boolean containsEscapedSeparators = false;

        // Is the field beginning with an enclosure?
        // "aa;aa";123;"aaa-aaa";000;...
        if ( len_encl > 0 && line.substring( from, from + len_encl ).equalsIgnoreCase( enclosure ) ) {
          if ( log.isRowLevel() ) {
            log.logRowlevel( BaseMessages.getString( PKG, "TextFileLineUtil.Log.ConvertLineToRowTitle" ), BaseMessages
              .getString( PKG, "TextFileLineUtil.Log.ConvertLineToRow", line.substring( from, from + len_encl ) ) );
          }
          encl_found = true;
          int p = from + len_encl;

          boolean is_enclosure =
            len_encl > 0
              && p + len_encl < length && line.substring( p, p + len_encl ).equalsIgnoreCase( enclosure );
          boolean isEscape =
            lenEsc > 0
              && p + lenEsc < length && line.substring( p, p + lenEsc ).equalsIgnoreCase( escapeCharacter );

          boolean enclosure_after = false;

          // Is it really an enclosure? See if it's not repeated twice or escaped!
          if ( ( is_enclosure || isEscape ) && p < length - 1 ) {
            String strnext = line.substring( p + len_encl, p + 2 * len_encl );
            if ( strnext.equalsIgnoreCase( enclosure ) ) {
              p++;
              enclosure_after = true;
              dencl = true;

              // Remember to replace them later on!
              if ( isEscape ) {
                containsEscaped_enclosures = true;
              }
            }
          }

          // Look for a closing enclosure!
          while ( ( !is_enclosure || enclosure_after ) && p < line.length() ) {
            p++;
            enclosure_after = false;
            is_enclosure =
              len_encl > 0 && p + len_encl < length && line.substring( p, p + len_encl ).equals( enclosure );
            isEscape =
              lenEsc > 0 && p + lenEsc < length && line.substring( p, p + lenEsc ).equals( escapeCharacter );

            // Is it really an enclosure? See if it's not repeated twice or escaped!
            if ( ( is_enclosure || isEscape ) && p < length - 1 ) {
              String strnext = line.substring( p + len_encl, p + 2 * len_encl );
              if ( strnext.equals( enclosure ) ) {
                p++;
                enclosure_after = true;
                dencl = true;

                // Remember to replace them later on!
                if ( isEscape ) {
                  containsEscaped_enclosures = true; // remember
                }
              }
            }
          }

          if ( p >= length ) {
            next = p;
          } else {
            next = p + len_encl;
          }

          if ( log.isRowLevel() ) {
            log.logRowlevel( BaseMessages.getString( PKG, "TextFileLineUtil.Log.ConvertLineToRowTitle" ), BaseMessages
              .getString( PKG, "TextFileLineUtil.Log.EndOfEnclosure", "" + p ) );
          }
        } else {
          encl_found = false;
          boolean found = false;
          int startpoint = from;
          do {
            next = line.indexOf( delimiter, startpoint );

            // See if this position is preceded by an escape character.
            if ( lenEsc > 0 && next - lenEsc > 0 ) {
              String before = line.substring( next - lenEsc, next );

              if ( escapeCharacter != null && escapeCharacter.equals( before ) ) {
                // take the next separator, this one is escaped...
                startpoint = next + 1;
                containsEscapedSeparators = true;
              } else {
                found = true;
              }
            } else {
              found = true;
            }
          } while ( !found && next >= 0 );
        }
        if ( next == -1 ) {
          next = length;
        }

        if ( encl_found ) {
          pol = line.substring( from + len_encl, next - len_encl );
          if ( log.isRowLevel() ) {
            log
              .logRowlevel(
                BaseMessages.getString( PKG, "TextFileLineUtil.Log.ConvertLineToRowTitle" ),
                BaseMessages.getString( PKG, "TextFileLineUtil.Log.EnclosureFieldFound", "" + pol ) );
          }
        } else {
          pol = line.substring( from, next );
          if ( log.isRowLevel() ) {
            log
              .logRowlevel(
                BaseMessages.getString( PKG, "TextFileLineUtil.Log.ConvertLineToRowTitle" ),
                BaseMessages.getString( PKG, "TextFileLineUtil.Log.NormalFieldFound", "" + pol ) );
          }
        }

        if ( dencl ) {
          StringBuilder sbpol = new StringBuilder( pol );
          int idx = sbpol.indexOf( enclosure + enclosure );
          while ( idx >= 0 ) {
            sbpol.delete( idx, idx + ( enclosure == null ? 0 : enclosure.length() ) );
            idx = sbpol.indexOf( enclosure + enclosure );
          }
          pol = sbpol.toString();
        }

        // replace the escaped enclosures with enclosures...
        if ( containsEscaped_enclosures ) {
          String replace = escapeCharacter + enclosure;
          pol = Const.replace( pol, replace, enclosure );
        }

        // replace the escaped separators with separators...
        if ( containsEscapedSeparators ) {
          String replace = escapeCharacter + delimiter;
          pol = Const.replace( pol, replace, delimiter );
        }

        // Now add pol to the strings found!
        strings.add( pol );

        pos = next + delimiter.length();
      }
      if ( pos == length ) {
        if ( log.isRowLevel() ) {
          log.logRowlevel( BaseMessages.getString( PKG, "TextFileLineUtil.Log.ConvertLineToRowTitle" ),
            BaseMessages.getString( PKG, "TextFileLineUtil.Log.EndOfEmptyLineFound" ) );
        }
        strings.add( "" );
      }
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "TextFileLineUtil.Log.Error.ErrorConvertingLine", e.toString() ), e );
    }

    return strings.toArray( new String[ strings.size() ] );
  }
}
