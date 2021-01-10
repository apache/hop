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

package org.apache.hop.pipeline.transforms.calculator;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Node;

public class CalculatorMetaFunction implements Cloneable {
  private static final Class<?> PKG = CalculatorMeta.class; // For Translator

  public static final String XML_TAG = "calculation";

  public static final int CALC_NONE = 0;
  public static final int CALC_CONSTANT = 1;
  public static final int CALC_COPY_OF_FIELD = 2;
  public static final int CALC_ADD = 3;
  public static final int CALC_SUBTRACT = 4;
  public static final int CALC_MULTIPLY = 5;
  public static final int CALC_DIVIDE = 6;
  public static final int CALC_SQUARE = 7;
  public static final int CALC_SQUARE_ROOT = 8;
  public static final int CALC_PERCENT_1 = 9;
  public static final int CALC_PERCENT_2 = 10;
  public static final int CALC_PERCENT_3 = 11;
  public static final int CALC_COMBINATION_1 = 12;
  public static final int CALC_COMBINATION_2 = 13;
  public static final int CALC_ROUND_1 = 14;
  public static final int CALC_ROUND_2 = 15;
  public static final int CALC_ROUND_STD_1 = 16;
  public static final int CALC_ROUND_STD_2 = 17;
  public static final int CALC_CEIL = 18;
  public static final int CALC_FLOOR = 19;
  public static final int CALC_NVL = 20;
  public static final int CALC_ADD_DAYS = 21;
  public static final int CALC_YEAR_OF_DATE = 22;
  public static final int CALC_MONTH_OF_DATE = 23;
  public static final int CALC_DAY_OF_YEAR = 24;
  public static final int CALC_DAY_OF_MONTH = 25;
  public static final int CALC_DAY_OF_WEEK = 26;
  public static final int CALC_WEEK_OF_YEAR = 27;
  public static final int CALC_WEEK_OF_YEAR_ISO8601 = 28;
  public static final int CALC_YEAR_OF_DATE_ISO8601 = 29;
  public static final int CALC_BYTE_TO_HEX_ENCODE = 30;
  public static final int CALC_HEX_TO_BYTE_DECODE = 31;
  public static final int CALC_CHAR_TO_HEX_ENCODE = 32;
  public static final int CALC_HEX_TO_CHAR_DECODE = 33;
  public static final int CALC_CRC32 = 34;
  public static final int CALC_ADLER32 = 35;
  public static final int CALC_MD5 = 36;
  public static final int CALC_SHA1 = 37;
  public static final int CALC_LEVENSHTEIN_DISTANCE = 38;
  public static final int CALC_METAPHONE = 39;
  public static final int CALC_DOUBLE_METAPHONE = 40;
  public static final int CALC_ABS = 41;
  public static final int CALC_REMOVE_TIME_FROM_DATE = 42;
  public static final int CALC_DATE_DIFF = 43;
  public static final int CALC_ADD3 = 44;
  public static final int CALC_INITCAP = 45;
  public static final int CALC_UPPER_CASE = 46;
  public static final int CALC_LOWER_CASE = 47;
  public static final int CALC_MASK_XML = 48;
  public static final int CALC_USE_CDATA = 49;
  public static final int CALC_REMOVE_CR = 50;
  public static final int CALC_REMOVE_LF = 51;
  public static final int CALC_REMOVE_CRLF = 52;
  public static final int CALC_REMOVE_TAB = 53;
  public static final int CALC_GET_ONLY_DIGITS = 54;
  public static final int CALC_REMOVE_DIGITS = 55;
  public static final int CALC_STRING_LEN = 56;
  public static final int CALC_LOAD_FILE_CONTENT_BINARY = 57;
  public static final int CALC_ADD_TIME_TO_DATE = 58;
  public static final int CALC_QUARTER_OF_DATE = 59;
  public static final int CALC_SUBSTITUTE_VARIABLE = 60;
  public static final int CALC_UNESCAPE_XML = 61;
  public static final int CALC_ESCAPE_HTML = 62;
  public static final int CALC_UNESCAPE_HTML = 63;
  public static final int CALC_ESCAPE_SQL = 64;
  public static final int CALC_DATE_WORKING_DIFF = 65;
  public static final int CALC_ADD_MONTHS = 66;
  public static final int CALC_CHECK_XML_FILE_WELL_FORMED = 67;
  public static final int CALC_CHECK_XML_WELL_FORMED = 68;
  public static final int CALC_GET_FILE_ENCODING = 69;
  public static final int CALC_DAMERAU_LEVENSHTEIN = 70;
  public static final int CALC_NEEDLEMAN_WUNSH = 71;
  public static final int CALC_JARO = 72;
  public static final int CALC_JARO_WINKLER = 73;
  public static final int CALC_SOUNDEX = 74;
  public static final int CALC_REFINED_SOUNDEX = 75;
  public static final int CALC_ADD_HOURS = 76;
  public static final int CALC_ADD_MINUTES = 77;
  public static final int CALC_DATE_DIFF_MSEC = 78;
  public static final int CALC_DATE_DIFF_SEC = 79;
  public static final int CALC_DATE_DIFF_MN = 80;
  public static final int CALC_DATE_DIFF_HR = 81;
  public static final int CALC_HOUR_OF_DAY = 82;
  public static final int CALC_MINUTE_OF_HOUR = 83;
  public static final int CALC_SECOND_OF_MINUTE = 84;
  public static final int CALC_ROUND_CUSTOM_1 = 85;
  public static final int CALC_ROUND_CUSTOM_2 = 86;
  public static final int CALC_ADD_SECONDS = 87;
  public static final int CALC_REMAINDER = 88;

  public static final String[] calcDesc = {
    "-", "CONSTANT", "COPY_FIELD", "ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "SQUARE", "SQUARE_ROOT",
    "PERCENT_1", "PERCENT_2", "PERCENT_3", "COMBINATION_1", "COMBINATION_2", "ROUND_1", "ROUND_2",
    "ROUND_STD_1", "ROUND_STD_2", "CEIL", "FLOOR", "NVL", "ADD_DAYS", "YEAR_OF_DATE", "MONTH_OF_DATE",
    "DAY_OF_YEAR", "DAY_OF_MONTH", "DAY_OF_WEEK", "WEEK_OF_YEAR", "WEEK_OF_YEAR_ISO8601",
    "YEAR_OF_DATE_ISO8601", "BYTE_TO_HEX_ENCODE", "HEX_TO_BYTE_DECODE", "CHAR_TO_HEX_ENCODE",
    "HEX_TO_CHAR_DECODE", "CRC32", "ADLER32", "MD5", "SHA1", "LEVENSHTEIN_DISTANCE", "METAPHONE",
    "DOUBLE_METAPHONE", "ABS", "REMOVE_TIME_FROM_DATE", "DATE_DIFF", "ADD3", "INIT_CAP", "UPPER_CASE",
    "LOWER_CASE", "MASK_XML", "USE_CDATA", "REMOVE_CR", "REMOVE_LF", "REMOVE_CRLF", "REMOVE_TAB",
    "GET_ONLY_DIGITS", "REMOVE_DIGITS", "STRING_LEN", "LOAD_FILE_CONTENT_BINARY", "ADD_TIME_TO_DATE",
    "QUARTER_OF_DATE", "SUBSTITUTE_VARIABLE", "UNESCAPE_XML", "ESCAPE_HTML", "UNESCAPE_HTML", "ESCAPE_SQL",
    "DATE_WORKING_DIFF", "ADD_MONTHS", "CHECK_XML_FILE_WELL_FORMED", "CHECK_XML_WELL_FORMED",
    "GET_FILE_ENCODING", "DAMERAU_LEVENSHTEIN", "NEEDLEMAN_WUNSH", "JARO", "JARO_WINKLER", "SOUNDEX",
    "REFINED_SOUNDEX", "ADD_HOURS", "ADD_MINUTES", "DATE_DIFF_MSEC", "DATE_DIFF_SEC", "DATE_DIFF_MN",
    "DATE_DIFF_HR", "HOUR_OF_DAY", "MINUTE_OF_HOUR", "SECOND_OF_MINUTE",
    "ROUND_CUSTOM_1", "ROUND_CUSTOM_2", "ADD_SECONDS", "REMAINDER" };

  public static final String[] calcLongDesc = {
    "-", BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.SetFieldToConstant" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.CreateCopyOfField" ), "A + B", "A - B",
    "A * B", "A / B", "A * A", BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.SQRT" ),
    "100 * A / B", "A - ( A * B / 100 )", "A + ( A * B / 100 )", "A + B * C",
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.Hypotenuse" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.Round" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.Round2" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RoundStd" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RoundStd2" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.Ceil" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.Floor" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.NVL" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DatePlusDays" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.YearOfDate" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.MonthOfDate" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DayOfYear" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DayOfMonth" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DayOfWeek" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.WeekOfYear" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.WeekOfYearISO8601" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.YearOfDateISO8601" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.ByteToHexEncode" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.HexToByteDecode" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.CharToHexEncode" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.HexToCharDecode" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.CRC32" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.Adler32" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.MD5" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.SHA1" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.LevenshteinDistance" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.Metaphone" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DoubleMetaphone" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.Abs" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RemoveTimeFromDate" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DateDiff" ), "A + B + C",
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.InitCap" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.UpperCase" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.LowerCase" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.MaskXML" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.UseCDATA" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RemoveCR" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RemoveLF" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RemoveCRLF" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RemoveTAB" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.GetOnlyDigits" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RemoveDigits" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.StringLen" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.LoadFileContentInBinary" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.AddTimeToDate" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.QuarterOfDate" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.SubstituteVariable" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.UnescapeXML" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.EscapeHTML" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.UnescapeHTML" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.EscapeSQL" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DateDiffWorking" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DatePlusMonths" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.CheckXmlFileWellFormed" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.CheckXmlWellFormed" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.GetFileEncoding" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DamerauLevenshtein" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.NeedlemanWunsch" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.Jaro" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.JaroWinkler" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.SoundEx" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RefinedSoundEx" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DatePlusHours" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DatePlusMinutes" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DateDiffMsec" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DateDiffSec" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DateDiffMn" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.DateDiffHr" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.HourOfDay" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.MinuteOfHour" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.SecondOfMinute" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RoundCustom" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.RoundCustom2" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.AddSeconds" ),
    BaseMessages.getString( PKG, "CalculatorMetaFunction.CalcFunctions.Remainder" ), };

  public static final int[] calcDefaultResultType = new int[ calcDesc.length ];

  static {
    calcDefaultResultType[ CalculatorMetaFunction.CALC_NONE ] = IValueMeta.TYPE_NONE;
    // Set field to constant value...
    calcDefaultResultType[ CalculatorMetaFunction.CALC_CONSTANT ] = IValueMeta.TYPE_STRING;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_COPY_OF_FIELD ] = IValueMeta.TYPE_NONE;
    // A + B
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ADD ] = IValueMeta.TYPE_NUMBER;
    // A - B
    calcDefaultResultType[ CalculatorMetaFunction.CALC_SUBTRACT ] = IValueMeta.TYPE_NUMBER;
    // A * B
    calcDefaultResultType[ CalculatorMetaFunction.CALC_MULTIPLY ] = IValueMeta.TYPE_NUMBER;
    // A / B
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DIVIDE ] = IValueMeta.TYPE_NUMBER;
    // A * A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_SQUARE ] = IValueMeta.TYPE_NUMBER;
    // SQRT( A )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_SQUARE_ROOT ] = IValueMeta.TYPE_NUMBER;
    // 100 * A / B
    calcDefaultResultType[ CalculatorMetaFunction.CALC_PERCENT_1 ] = IValueMeta.TYPE_NUMBER;
    // A - ( A * B / 100 )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_PERCENT_2 ] = IValueMeta.TYPE_NUMBER;
    // A + ( A * B / 100 )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_PERCENT_3 ] = IValueMeta.TYPE_NUMBER;
    // A + B * C
    calcDefaultResultType[ CalculatorMetaFunction.CALC_COMBINATION_1 ] = IValueMeta.TYPE_NUMBER;
    // SQRT( A*A + B*B )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_COMBINATION_2 ] = IValueMeta.TYPE_NUMBER;
    // ROUND( A )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ROUND_1 ] = IValueMeta.TYPE_INTEGER;
    // ROUND( A , B )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ROUND_2 ] = IValueMeta.TYPE_NUMBER;
    // STDROUND( A )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ROUND_STD_1 ] = IValueMeta.TYPE_INTEGER;
    // STDROUND( A , B )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ROUND_STD_2 ] = IValueMeta.TYPE_NUMBER;
    // CEIL( A )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_CEIL ] = IValueMeta.TYPE_INTEGER;
    // FLOOR( A )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_FLOOR ] = IValueMeta.TYPE_INTEGER;
    // Replace null values with another value
    calcDefaultResultType[ CalculatorMetaFunction.CALC_NVL ] = IValueMeta.TYPE_NONE;
    // Add B days to date field A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ADD_DAYS ] = IValueMeta.TYPE_DATE;
    // What is the year (Integer) of a date?
    calcDefaultResultType[ CalculatorMetaFunction.CALC_YEAR_OF_DATE ] = IValueMeta.TYPE_INTEGER;
    // What is the month (Integer) of a date?
    calcDefaultResultType[ CalculatorMetaFunction.CALC_MONTH_OF_DATE ] = IValueMeta.TYPE_INTEGER;
    // What is the day of year (Integer) of a date?
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DAY_OF_YEAR ] = IValueMeta.TYPE_INTEGER;
    // What is the day of month (Integer) of a date?
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DAY_OF_MONTH ] = IValueMeta.TYPE_INTEGER;
    // What is the day of week (Integer) of a date?
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DAY_OF_WEEK ] = IValueMeta.TYPE_INTEGER;
    // What is the week of year (Integer) of a date?
    calcDefaultResultType[ CalculatorMetaFunction.CALC_WEEK_OF_YEAR ] = IValueMeta.TYPE_INTEGER;
    // What is the week of year (Integer) of a date ISO8601 style?
    calcDefaultResultType[ CalculatorMetaFunction.CALC_WEEK_OF_YEAR_ISO8601 ] = IValueMeta.TYPE_INTEGER;
    // What is the year (Integer) of a date ISO8601 style?
    calcDefaultResultType[ CalculatorMetaFunction.CALC_YEAR_OF_DATE_ISO8601 ] = IValueMeta.TYPE_INTEGER;
    // Byte to Hex encode string field A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_BYTE_TO_HEX_ENCODE ] = IValueMeta.TYPE_STRING;
    // Hex to Byte decode string field A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_HEX_TO_BYTE_DECODE ] = IValueMeta.TYPE_STRING;
    // Char to Hex encode string field A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_CHAR_TO_HEX_ENCODE ] = IValueMeta.TYPE_STRING;
    // Hex to Char decode string field A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_HEX_TO_CHAR_DECODE ] = IValueMeta.TYPE_STRING;
    // CRC32 of a file A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_CRC32 ] = IValueMeta.TYPE_INTEGER;
    // ADLER32 of a file A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ADLER32 ] = IValueMeta.TYPE_INTEGER;
    // MD5 of a file A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_MD5 ] = IValueMeta.TYPE_STRING;
    // SHA1 of a file Al
    calcDefaultResultType[ CalculatorMetaFunction.CALC_SHA1 ] = IValueMeta.TYPE_STRING;
    // LEVENSHTEIN_DISTANCE of string A and string B
    calcDefaultResultType[ CalculatorMetaFunction.CALC_LEVENSHTEIN_DISTANCE ] = IValueMeta.TYPE_INTEGER;
    // METAPHONE of string A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_METAPHONE ] = IValueMeta.TYPE_STRING;
    // Double METAPHONE of string A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DOUBLE_METAPHONE ] = IValueMeta.TYPE_STRING;
    // ABS( A )
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ABS ] = IValueMeta.TYPE_INTEGER;
    // Remove time from field A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_REMOVE_TIME_FROM_DATE ] = IValueMeta.TYPE_DATE;
    // DateA - DateB
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DATE_DIFF ] = IValueMeta.TYPE_INTEGER;
    // A + B +C
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ADD3 ] = IValueMeta.TYPE_NUMBER;
    // InitCap(A)
    calcDefaultResultType[ CalculatorMetaFunction.CALC_INITCAP ] = IValueMeta.TYPE_STRING;
    // UpperCase(A)
    calcDefaultResultType[ CalculatorMetaFunction.CALC_UPPER_CASE ] = IValueMeta.TYPE_STRING;
    // LowerCase(A)
    calcDefaultResultType[ CalculatorMetaFunction.CALC_LOWER_CASE ] = IValueMeta.TYPE_STRING;
    // MaskXML(A)
    calcDefaultResultType[ CalculatorMetaFunction.CALC_MASK_XML ] = IValueMeta.TYPE_STRING;
    // CDATA(A)
    calcDefaultResultType[ CalculatorMetaFunction.CALC_USE_CDATA ] = IValueMeta.TYPE_STRING;
    // REMOVE CR FROM string A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_REMOVE_CR ] = IValueMeta.TYPE_STRING;
    // REMOVE LF FROM string A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_REMOVE_LF ] = IValueMeta.TYPE_STRING;
    // REMOVE CRLF FROM string A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_REMOVE_CRLF ] = IValueMeta.TYPE_STRING;
    // REMOVE TAB FROM string A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_REMOVE_TAB ] = IValueMeta.TYPE_STRING;
    // GET ONLY DIGITS FROM string A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_GET_ONLY_DIGITS ] = IValueMeta.TYPE_STRING;
    // REMOVE DIGITS FROM string A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_REMOVE_DIGITS ] = IValueMeta.TYPE_STRING;
    // LENGTH OF string A
    calcDefaultResultType[ CalculatorMetaFunction.CALC_STRING_LEN ] = IValueMeta.TYPE_INTEGER;
    // LOAD FILE CONTENT IN BLOB
    calcDefaultResultType[ CalculatorMetaFunction.CALC_LOAD_FILE_CONTENT_BINARY ] = IValueMeta.TYPE_BINARY;
    // ADD TIME TO A DATE
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ADD_TIME_TO_DATE ] = IValueMeta.TYPE_DATE;
    // What is the quarter (Integer) of a date?
    calcDefaultResultType[ CalculatorMetaFunction.CALC_QUARTER_OF_DATE ] = IValueMeta.TYPE_INTEGER;
    // variable substitution in string
    calcDefaultResultType[ CalculatorMetaFunction.CALC_SUBSTITUTE_VARIABLE ] = IValueMeta.TYPE_STRING;
    // unEscape XML
    calcDefaultResultType[ CalculatorMetaFunction.CALC_UNESCAPE_XML ] = IValueMeta.TYPE_STRING;
    // escape HTML
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ESCAPE_HTML ] = IValueMeta.TYPE_STRING;
    // unEscape HTML
    calcDefaultResultType[ CalculatorMetaFunction.CALC_UNESCAPE_HTML ] = IValueMeta.TYPE_STRING;
    // escape SQL
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ESCAPE_SQL ] = IValueMeta.TYPE_STRING;
    // Date A - Date B
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DATE_WORKING_DIFF ] = IValueMeta.TYPE_INTEGER;
    // Date A - B Months
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ADD_MONTHS ] = IValueMeta.TYPE_DATE;
    // XML file A well formed
    calcDefaultResultType[ CalculatorMetaFunction.CALC_CHECK_XML_FILE_WELL_FORMED ] = IValueMeta.TYPE_BOOLEAN;
    // XML string A well formed
    calcDefaultResultType[ CalculatorMetaFunction.CALC_CHECK_XML_WELL_FORMED ] = IValueMeta.TYPE_BOOLEAN;
    // get file encoding
    calcDefaultResultType[ CalculatorMetaFunction.CALC_GET_FILE_ENCODING ] = IValueMeta.TYPE_STRING;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DAMERAU_LEVENSHTEIN ] = IValueMeta.TYPE_INTEGER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_NEEDLEMAN_WUNSH ] = IValueMeta.TYPE_INTEGER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_JARO ] = IValueMeta.TYPE_NUMBER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_JARO_WINKLER ] = IValueMeta.TYPE_NUMBER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_SOUNDEX ] = IValueMeta.TYPE_STRING;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_REFINED_SOUNDEX ] = IValueMeta.TYPE_STRING;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ADD_HOURS ] = IValueMeta.TYPE_DATE;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ADD_MINUTES ] = IValueMeta.TYPE_DATE;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DATE_DIFF_MSEC ] = IValueMeta.TYPE_INTEGER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DATE_DIFF_SEC ] = IValueMeta.TYPE_INTEGER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DATE_DIFF_MN ] = IValueMeta.TYPE_INTEGER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_DATE_DIFF_HR ] = IValueMeta.TYPE_INTEGER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_HOUR_OF_DAY ] = IValueMeta.TYPE_INTEGER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_MINUTE_OF_HOUR ] = IValueMeta.TYPE_INTEGER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_SECOND_OF_MINUTE ] = IValueMeta.TYPE_INTEGER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ROUND_CUSTOM_1 ] = IValueMeta.TYPE_NUMBER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ROUND_CUSTOM_2 ] = IValueMeta.TYPE_NUMBER;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_ADD_SECONDS ] = IValueMeta.TYPE_DATE;
    calcDefaultResultType[ CalculatorMetaFunction.CALC_REMAINDER ] = IValueMeta.TYPE_NUMBER;
  }

  private String fieldName;
  private int calcType;
  private String fieldA;
  private String fieldB;
  private String fieldC;

  private int valueType;
  private int valueLength;
  private int valuePrecision;

  private String conversionMask;
  private String decimalSymbol;
  private String groupingSymbol;
  private String currencySymbol;

  private boolean removedFromResult;

  /**
   * @param fieldName      out field name
   * @param calcType       calculation type, see CALC_* set of constants defined
   * @param fieldA         name of field "A"
   * @param fieldB         name of field "B"
   * @param fieldC         name of field "C"
   * @param valueType      out value type
   * @param valueLength    out value length
   * @param valuePrecision out value precision
   * @param conversionMask out value conversion mask
   * @param decimalSymbol  out value decimal symbol
   * @param groupingSymbol out value grouping symbol
   * @param currencySymbol out value currency symbol
   */
  public CalculatorMetaFunction( String fieldName, int calcType, String fieldA, String fieldB, String fieldC,
                                 int valueType, int valueLength, int valuePrecision, boolean removedFromResult,
                                 String conversionMask,
                                 String decimalSymbol, String groupingSymbol, String currencySymbol ) {
    this.fieldName = fieldName;
    this.calcType = calcType;
    this.fieldA = fieldA;
    this.fieldB = fieldB;
    this.fieldC = fieldC;
    this.valueType = valueType;
    this.valueLength = valueLength;
    this.valuePrecision = valuePrecision;
    this.removedFromResult = removedFromResult;
    this.conversionMask = conversionMask;
    this.decimalSymbol = decimalSymbol;
    this.groupingSymbol = groupingSymbol;
    this.currencySymbol = currencySymbol;
  }

  public CalculatorMetaFunction() {
    // all null
  }

  @Override
  public boolean equals( Object obj ) {
    if ( obj != null && ( obj.getClass().equals( this.getClass() ) ) ) {
      CalculatorMetaFunction mf = (CalculatorMetaFunction) obj;
      return ( getXml().equals( mf.getXml() ) );
    }

    return false;
  }

  @Override
  public int hashCode() {
    return getXml().hashCode();
  }

  @Override
  public Object clone() {
    try {
      CalculatorMetaFunction retval = (CalculatorMetaFunction) super.clone();
      return retval;
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append( "    " ).append( XmlHandler.openTag( XML_TAG ) ).append( Const.CR );
    xml.append( "      " ).append( XmlHandler.addTagValue( "field_name", fieldName ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "calc_type", getCalcTypeDesc() ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "field_a", fieldA ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "field_b", fieldB ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "field_c", fieldC ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "value_type", ValueMetaFactory.getValueMetaName( valueType ) ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "value_length", valueLength ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "value_precision", valuePrecision ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "remove", removedFromResult ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "conversion_mask", conversionMask ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "decimal_symbol", decimalSymbol ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "grouping_symbol", groupingSymbol ) );
    xml.append( "      " ).append( XmlHandler.addTagValue( "currency_symbol", currencySymbol ) );
    xml.append( "    " ).append( XmlHandler.closeTag( XML_TAG ) ).append( Const.CR );

    return xml.toString();
  }

  public CalculatorMetaFunction( Node calcnode ) {
    fieldName = XmlHandler.getTagValue( calcnode, "field_name" );
    calcType = getCalcFunctionType( XmlHandler.getTagValue( calcnode, "calc_type" ) );
    fieldA = XmlHandler.getTagValue( calcnode, "field_a" );
    fieldB = XmlHandler.getTagValue( calcnode, "field_b" );
    fieldC = XmlHandler.getTagValue( calcnode, "field_c" );
    valueType = ValueMetaFactory.getIdForValueMeta( XmlHandler.getTagValue( calcnode, "value_type" ) );
    valueLength = Const.toInt( XmlHandler.getTagValue( calcnode, "value_length" ), -1 );
    valuePrecision = Const.toInt( XmlHandler.getTagValue( calcnode, "value_precision" ), -1 );
    removedFromResult = "Y".equalsIgnoreCase( XmlHandler.getTagValue( calcnode, "remove" ) );
    conversionMask = XmlHandler.getTagValue( calcnode, "conversion_mask" );
    decimalSymbol = XmlHandler.getTagValue( calcnode, "decimal_symbol" );
    groupingSymbol = XmlHandler.getTagValue( calcnode, "grouping_symbol" );
    currencySymbol = XmlHandler.getTagValue( calcnode, "currency_symbol" );

    // Fix 2.x backward compatibility
    // The conversion mask was added in a certain revision.
    // Anything that we load from before then should get masks set to retain backward compatibility
    //
    if ( XmlHandler.getSubNode( calcnode, "conversion_mask" ) == null ) {
      fixBackwardCompatibility();
    }
  }

  private void fixBackwardCompatibility() {
    if ( valueType == IValueMeta.TYPE_INTEGER ) {
      if ( Utils.isEmpty( conversionMask ) ) {
        conversionMask = "0";
      }
      if ( Utils.isEmpty( decimalSymbol ) ) {
        decimalSymbol = ".";
      }
      if ( Utils.isEmpty( groupingSymbol ) ) {
        groupingSymbol = ",";
      }
    }
    if ( valueType == IValueMeta.TYPE_NUMBER ) {
      if ( Utils.isEmpty( conversionMask ) ) {
        conversionMask = "0.0";
      }
      if ( Utils.isEmpty( decimalSymbol ) ) {
        decimalSymbol = ".";
      }
      if ( Utils.isEmpty( groupingSymbol ) ) {
        groupingSymbol = ",";
      }
    }
  }

  public static int getCalcFunctionType( String desc ) {
    for ( int i = 1; i < calcDesc.length; i++ ) {
      if ( calcDesc[ i ].equalsIgnoreCase( desc ) ) {
        return i;
      }
    }
    for ( int i = 1; i < calcLongDesc.length; i++ ) {
      if ( calcLongDesc[ i ].equalsIgnoreCase( desc ) ) {
        return i;
      }
    }

    return CALC_NONE;
  }

  public static String getCalcFunctionDesc( int type ) {
    if ( type < 0 || type >= calcDesc.length ) {
      return null;
    }
    return calcDesc[ type ];
  }

  public static String getCalcFunctionLongDesc( int type ) {
    if ( type < 0 || type >= calcLongDesc.length ) {
      return null;
    }
    return calcLongDesc[ type ];
  }

  public static int getCalcFunctionDefaultResultType( int type ) {
    if ( type < 0 || type >= calcDefaultResultType.length ) {
      return IValueMeta.TYPE_NONE;
    }
    return calcDefaultResultType[ type ];
  }

  /**
   * @return Returns the calcType.
   */
  public int getCalcType() {
    return calcType;
  }

  /**
   * @param calcType The calcType to set.
   */
  public void setCalcType( int calcType ) {
    this.calcType = calcType;
  }

  public String getCalcTypeDesc() {
    return getCalcFunctionDesc( calcType );
  }

  public String getCalcTypeLongDesc() {
    return getCalcFunctionLongDesc( calcType );
  }

  /**
   * @return Returns the fieldA.
   */
  public String getFieldA() {
    return fieldA;
  }

  /**
   * @param fieldA The fieldA to set.
   */
  public void setFieldA( String fieldA ) {
    this.fieldA = fieldA;
  }

  /**
   * @return Returns the fieldB.
   */
  public String getFieldB() {
    return fieldB;
  }

  /**
   * @param fieldB The fieldB to set.
   */
  public void setFieldB( String fieldB ) {
    this.fieldB = fieldB;
  }

  /**
   * @return Returns the fieldC.
   */
  public String getFieldC() {
    return fieldC;
  }

  /**
   * @param fieldC The fieldC to set.
   */
  public void setFieldC( String fieldC ) {
    this.fieldC = fieldC;
  }

  /**
   * @return Returns the fieldName.
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set.
   */
  public void setFieldName( String fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * @return Returns the valueLength.
   */
  public int getValueLength() {
    return valueLength;
  }

  /**
   * @param valueLength The valueLength to set.
   */
  public void setValueLength( int valueLength ) {
    this.valueLength = valueLength;
  }

  /**
   * @return Returns the valuePrecision.
   */
  public int getValuePrecision() {
    return valuePrecision;
  }

  /**
   * @param valuePrecision The valuePrecision to set.
   */
  public void setValuePrecision( int valuePrecision ) {
    this.valuePrecision = valuePrecision;
  }

  /**
   * @return Returns the valueType.
   */
  public int getValueType() {
    return valueType;
  }

  /**
   * @param valueType The valueType to set.
   */
  public void setValueType( int valueType ) {
    this.valueType = valueType;
  }

  /**
   * @return Returns the removedFromResult.
   */
  public boolean isRemovedFromResult() {
    return removedFromResult;
  }

  /**
   * @param removedFromResult The removedFromResult to set.
   */
  public void setRemovedFromResult( boolean removedFromResult ) {
    this.removedFromResult = removedFromResult;
  }

  /**
   * @return the conversionMask
   */
  public String getConversionMask() {
    return conversionMask;
  }

  /**
   * @param conversionMask the conversionMask to set
   */
  public void setConversionMask( String conversionMask ) {
    this.conversionMask = conversionMask;
  }

  /**
   * @return the decimalSymbol
   */
  public String getDecimalSymbol() {
    return decimalSymbol;
  }

  /**
   * @param decimalSymbol the decimalSymbol to set
   */
  public void setDecimalSymbol( String decimalSymbol ) {
    this.decimalSymbol = decimalSymbol;
  }

  /**
   * @return the groupingSymbol
   */
  public String getGroupingSymbol() {
    return groupingSymbol;
  }

  /**
   * @param groupingSymbol the groupingSymbol to set
   */
  public void setGroupingSymbol( String groupingSymbol ) {
    this.groupingSymbol = groupingSymbol;
  }

  /**
   * @return the currencySymbol
   */
  public String getCurrencySymbol() {
    return currencySymbol;
  }

  /**
   * @param currencySymbol the currencySymbol to set
   */
  public void setCurrencySymbol( String currencySymbol ) {
    this.currencySymbol = currencySymbol;
  }
}
