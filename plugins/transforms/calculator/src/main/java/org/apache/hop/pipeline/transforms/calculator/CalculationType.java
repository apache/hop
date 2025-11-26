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

package org.apache.hop.pipeline.transforms.calculator;

import java.util.stream.Stream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.pipeline.transforms.calculator.calculations.Constant;
import org.apache.hop.pipeline.transforms.calculator.calculations.CopyField;
import org.apache.hop.pipeline.transforms.calculator.calculations.None;
import org.apache.hop.pipeline.transforms.calculator.calculations.Nvl;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.Base64Decode;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.Base64Encode;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.Base64EncodePadded;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.ByteToHexEncode;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.CharToHexEncode;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.EscapeHTML;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.EscapeSQL;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.HexToByteDecode;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.HexToCharDecode;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.MaskXML;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.UnescapeHTML;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.UnescapeXML;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.UrlDecode;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.UrlEncode;
import org.apache.hop.pipeline.transforms.calculator.calculations.conversion.UseCDATA;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.AddDays;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.AddHours;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.AddMinutes;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.AddMonths;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.AddSeconds;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.AddTimeToDate;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.DateDiff;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.DateWorkingDiff;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.DayOfMonth;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.DayOfWeek;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.DayOfYear;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.FirstDayOfMonth;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.HourOfDay;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.LastDayOfMonth;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.MinuteOfHour;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.MonthOfDate;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.QuarterOfDate;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.RemoveTimeFromDate;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.SecondOfMinute;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.WeekOfYear;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.WeekOfYearISO8601;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.YearOfDate;
import org.apache.hop.pipeline.transforms.calculator.calculations.date.YearOfDateISO8601;
import org.apache.hop.pipeline.transforms.calculator.calculations.file.ADLER32;
import org.apache.hop.pipeline.transforms.calculator.calculations.file.CRC32;
import org.apache.hop.pipeline.transforms.calculator.calculations.file.CheckXMLFileWellFormed;
import org.apache.hop.pipeline.transforms.calculator.calculations.file.GetFileEncoding;
import org.apache.hop.pipeline.transforms.calculator.calculations.file.LoadFileContentBinary;
import org.apache.hop.pipeline.transforms.calculator.calculations.file.MessageDigest;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Abs;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Add;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Add3;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Ceil;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Combination1;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Combination2;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Divide;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Floor;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.GetOnlyDigits;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Multiply;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Percent1;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Percent2;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Percent3;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Remainder;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.RemoveDigits;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Round1;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Round2;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.RoundCustom1;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.RoundCustom2;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.RoundStd1;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.RoundStd2;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Square;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.SquareRoot;
import org.apache.hop.pipeline.transforms.calculator.calculations.math.Subtract;
import org.apache.hop.pipeline.transforms.calculator.calculations.statistics.DamerauLevenshtein;
import org.apache.hop.pipeline.transforms.calculator.calculations.statistics.DoubleMetaphone;
import org.apache.hop.pipeline.transforms.calculator.calculations.statistics.Jaro;
import org.apache.hop.pipeline.transforms.calculator.calculations.statistics.JaroWinkler;
import org.apache.hop.pipeline.transforms.calculator.calculations.statistics.LevenshteinDistance;
import org.apache.hop.pipeline.transforms.calculator.calculations.statistics.Metaphone;
import org.apache.hop.pipeline.transforms.calculator.calculations.statistics.NeedlemanWunsch;
import org.apache.hop.pipeline.transforms.calculator.calculations.statistics.RefinedSoundex;
import org.apache.hop.pipeline.transforms.calculator.calculations.statistics.Soundex;
import org.apache.hop.pipeline.transforms.calculator.calculations.text.CheckXmlWellFormed;
import org.apache.hop.pipeline.transforms.calculator.calculations.text.InitCap;
import org.apache.hop.pipeline.transforms.calculator.calculations.text.LowerCase;
import org.apache.hop.pipeline.transforms.calculator.calculations.text.RemoveCR;
import org.apache.hop.pipeline.transforms.calculator.calculations.text.RemoveCRLF;
import org.apache.hop.pipeline.transforms.calculator.calculations.text.RemoveLF;
import org.apache.hop.pipeline.transforms.calculator.calculations.text.RemoveTab;
import org.apache.hop.pipeline.transforms.calculator.calculations.text.StringLen;
import org.apache.hop.pipeline.transforms.calculator.calculations.text.SubstituteVariable;
import org.apache.hop.pipeline.transforms.calculator.calculations.text.UpperCase;

public enum CalculationType implements IEnumHasCodeAndDescription {
  NONE(new None()),
  CONSTANT(new Constant()),
  COPY_OF_FIELD(new CopyField()),
  ADD(new Add()),
  SUBTRACT(new Subtract()),
  MULTIPLY(new Multiply()),
  DIVIDE(new Divide()),
  SQUARE(new Square()),
  SQUARE_ROOT(new SquareRoot()),
  PERCENT_1(new Percent1()),
  PERCENT_2(new Percent2()),
  PERCENT_3(new Percent3()),
  COMBINATION_1(new Combination1()),
  COMBINATION_2(new Combination2()),
  ROUND_1(new Round1()),
  ROUND_2(new Round2()),
  ROUND_STD_1(new RoundStd1()),
  ROUND_STD_2(new RoundStd2()),
  CEIL(new Ceil()),
  FLOOR(new Floor()),
  NVL(new Nvl()),
  ADD_DAYS(new AddDays()),
  YEAR_OF_DATE(new YearOfDate()),
  MONTH_OF_DATE(new MonthOfDate()),
  DAY_OF_YEAR(new DayOfYear()),
  DAY_OF_MONTH(new DayOfMonth()),
  DAY_OF_WEEK(new DayOfWeek()),
  WEEK_OF_YEAR(new WeekOfYear()),
  WEEK_OF_YEAR_ISO8601(new WeekOfYearISO8601()),
  YEAR_OF_DATE_ISO8601(new YearOfDateISO8601()),
  BYTE_TO_HEX_ENCODE(new ByteToHexEncode()),
  HEX_TO_BYTE_DECODE(new HexToByteDecode()),
  CHAR_TO_HEX_ENCODE(new CharToHexEncode()),
  HEX_TO_CHAR_DECODE(new HexToCharDecode()),
  CRC32(new CRC32()),
  ADLER32(new ADLER32()),
  MD5(new MessageDigest(MessageDigest.ALGORITHM.MD5)),
  SHA1(new MessageDigest(MessageDigest.ALGORITHM.SHA1)),
  SHA256(new MessageDigest(MessageDigest.ALGORITHM.SHA256)),
  SHA384(new MessageDigest(MessageDigest.ALGORITHM.SHA384)),
  SHA512(new MessageDigest(MessageDigest.ALGORITHM.SHA512)),
  LEVENSHTEIN_DISTANCE(new LevenshteinDistance()),
  METAPHONE(new Metaphone()),
  DOUBLE_METAPHONE(new DoubleMetaphone()),
  ABS(new Abs()),
  REMOVE_TIME_FROM_DATE(new RemoveTimeFromDate()),
  DATE_DIFF(new DateDiff(DateDiff.INTERVAL.DATE_DIFF)),
  ADD3(new Add3()),
  INIT_CAP(new InitCap()),
  UPPER_CASE(new UpperCase()),
  LOWER_CASE(new LowerCase()),
  MASK_XML(new MaskXML()),
  USE_CDATA(new UseCDATA()),
  REMOVE_CR(new RemoveCR()),
  REMOVE_LF(new RemoveLF()),
  REMOVE_CRLF(new RemoveCRLF()),
  REMOVE_TAB(new RemoveTab()),
  GET_ONLY_DIGITS(new GetOnlyDigits()),
  REMOVE_DIGITS(new RemoveDigits()),
  STRING_LEN(new StringLen()),
  LOAD_FILE_CONTENT_BINARY(new LoadFileContentBinary()),
  ADD_TIME_TO_DATE(new AddTimeToDate()),
  QUARTER_OF_DATE(new QuarterOfDate()),
  SUBSTITUTE_VARIABLE(new SubstituteVariable()),
  UNESCAPE_XML(new UnescapeXML()),
  ESCAPE_HTML(new EscapeHTML()),
  UNESCAPE_HTML(new UnescapeHTML()),
  ESCAPE_SQL(new EscapeSQL()),
  DATE_WORKING_DIFF(new DateWorkingDiff()),
  ADD_MONTHS(new AddMonths()),
  CHECK_XML_FILE_WELL_FORMED(new CheckXMLFileWellFormed()),
  CHECK_XML_WELL_FORMED(new CheckXmlWellFormed()),
  GET_FILE_ENCODING(new GetFileEncoding()),
  DAMERAU_LEVENSHTEIN(new DamerauLevenshtein()),
  NEEDLEMAN_WUNSCH(new NeedlemanWunsch()),
  JARO(new Jaro()),
  JARO_WINKLER(new JaroWinkler()),
  SOUNDEX(new Soundex()),
  REFINED_SOUNDEX(new RefinedSoundex()),
  ADD_HOURS(new AddHours()),
  ADD_MINUTES(new AddMinutes()),
  DATE_DIFF_MSEC(new DateDiff(DateDiff.INTERVAL.DATE_DIFF_MSEC)),
  DATE_DIFF_SEC(new DateDiff(DateDiff.INTERVAL.DATE_DIFF_SEC)),
  DATE_DIFF_MN(new DateDiff(DateDiff.INTERVAL.DATE_DIFF_MN)),
  DATE_DIFF_HR(new DateDiff(DateDiff.INTERVAL.DATE_DIFF_HR)),
  HOUR_OF_DAY(new HourOfDay()),
  MINUTE_OF_HOUR(new MinuteOfHour()),
  SECOND_OF_MINUTE(new SecondOfMinute()),
  ROUND_CUSTOM_1(new RoundCustom1()),
  ROUND_CUSTOM_2(new RoundCustom2()),
  ADD_SECONDS(new AddSeconds()),
  REMAINDER(new Remainder()),
  BASE64_ENCODE(new Base64Encode()),
  BASE64_ENCODE_PADDED(new Base64EncodePadded()),
  BASE64_DECODE(new Base64Decode()),
  FIRST_DAY_OF_MONTH(new FirstDayOfMonth()),
  LAST_DAY_OF_MONTH(new LastDayOfMonth()),
  URL_ENCODE(new UrlEncode()),
  URL_DECODE(new UrlDecode());

  public static final String[] descriptions =
      Stream.of(values()).map(e -> e.calculation.getDescription()).toArray(String[]::new);

  public final ICalculation calculation;

  CalculationType(ICalculation calculation) {
    this.calculation = calculation;
  }

  public static CalculationType findByDescription(String description) {
    int index = ArrayUtils.indexOf(descriptions, description);
    return index == -1 ? NONE : values()[index];
  }

  @Override
  public String getCode() {
    return calculation.getCode();
  }

  @Override
  public String getDescription() {
    return calculation.getDescription();
  }
}
