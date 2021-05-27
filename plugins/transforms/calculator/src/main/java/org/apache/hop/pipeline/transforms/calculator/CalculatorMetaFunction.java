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

import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;

import java.util.Objects;

public class CalculatorMetaFunction implements Cloneable {
  private static final Class<?> PKG = CalculatorMeta.class; // For Translator

  public enum CalculationType implements IEnumHasCode {
    NONE("-", "-", IValueMeta.TYPE_NONE),
    CONSTANT(
        "CONSTANT",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.SetFieldToConstant"),
        IValueMeta.TYPE_STRING),
    COPY_OF_FIELD(
        "COPY_FIELD",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.CreateCopyOfField"),
        IValueMeta.TYPE_NONE),
    ADD("ADD", "A + B", IValueMeta.TYPE_NUMBER),
    SUBTRACT("SUBTRACT", "A - B", IValueMeta.TYPE_NUMBER),
    MULTIPLY("MULTIPLY", "A * B", IValueMeta.TYPE_NUMBER),
    DIVIDE("DIVIDE", "A / B", IValueMeta.TYPE_NUMBER),
    SQUARE("SQUARE", "A * A", IValueMeta.TYPE_NUMBER),
    SQUARE_ROOT(
        "SQUARE_ROOT",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.SQRT"),
        IValueMeta.TYPE_NUMBER),
    PERCENT_1("PERCENT_1", "100 * A / B", IValueMeta.TYPE_NUMBER),
    PERCENT_2("PERCENT_2", "A - ( A * B / 100 )", IValueMeta.TYPE_NUMBER),
    PERCENT_3("PERCENT_3", "A + ( A * B / 100 )", IValueMeta.TYPE_NUMBER),
    COMBINATION_1("COMBINATION_1", "A + B * C", IValueMeta.TYPE_NUMBER),
    COMBINATION_2(
        "COMBINATION_2",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Hypotenuse"),
        IValueMeta.TYPE_NUMBER),
    ROUND_1(
        "ROUND_1",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Round"),
        IValueMeta.TYPE_INTEGER),
    ROUND_2(
        "ROUND_2",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Round2"),
        IValueMeta.TYPE_NUMBER),
    ROUND_STD_1(
        "ROUND_STD_1",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RoundStd"),
        IValueMeta.TYPE_INTEGER),
    ROUND_STD_2(
        "ROUND_STD_2",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RoundStd2"),
        IValueMeta.TYPE_NUMBER),
    CEIL(
        "CEIL",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Ceil"),
        IValueMeta.TYPE_INTEGER),
    FLOOR(
        "FLOOR",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Floor"),
        IValueMeta.TYPE_INTEGER),
    NVL(
        "NVL",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.NVL"),
        IValueMeta.TYPE_NONE),
    ADD_DAYS(
        "ADD_DAYS",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DatePlusDays"),
        IValueMeta.TYPE_DATE),
    YEAR_OF_DATE(
        "YEAR_OF_DATE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.YearOfDate"),
        IValueMeta.TYPE_INTEGER),
    MONTH_OF_DATE(
        "MONTH_OF_DATE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.MonthOfDate"),
        IValueMeta.TYPE_INTEGER),
    DAY_OF_YEAR(
        "DAY_OF_YEAR",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DayOfYear"),
        IValueMeta.TYPE_INTEGER),
    DAY_OF_MONTH(
        "DAY_OF_MONTH",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DayOfMonth"),
        IValueMeta.TYPE_INTEGER),
    DAY_OF_WEEK(
        "DAY_OF_WEEK",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DayOfWeek"),
        IValueMeta.TYPE_INTEGER),
    WEEK_OF_YEAR(
        "WEEK_OF_YEAR",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.WeekOfYear"),
        IValueMeta.TYPE_INTEGER),
    WEEK_OF_YEAR_ISO8601(
        "WEEK_OF_YEAR_ISO8601",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.WeekOfYearISO8601"),
        IValueMeta.TYPE_INTEGER),
    YEAR_OF_DATE_ISO8601(
        "YEAR_OF_DATE_ISO8601",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.YearOfDateISO8601"),
        IValueMeta.TYPE_INTEGER),
    BYTE_TO_HEX_ENCODE(
        "BYTE_TO_HEX_ENCODE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.ByteToHexEncode"),
        IValueMeta.TYPE_STRING),
    HEX_TO_BYTE_DECODE(
        "HEX_TO_BYTE_DECODE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.HexToByteDecode"),
        IValueMeta.TYPE_STRING),
    CHAR_TO_HEX_ENCODE(
        "CHAR_TO_HEX_ENCODE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.CharToHexEncode"),
        IValueMeta.TYPE_STRING),
    HEX_TO_CHAR_DECODE(
        "HEX_TO_CHAR_DECODE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.HexToCharDecode"),
        IValueMeta.TYPE_STRING),
    CRC32(
        "CRC32",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.CRC32"),
        IValueMeta.TYPE_INTEGER),
    ADLER32(
        "ADLER32",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Adler32"),
        IValueMeta.TYPE_INTEGER),
    MD5(
        "MD5",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.MD5"),
        IValueMeta.TYPE_STRING),
    SHA1(
        "SHA1",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.SHA1"),
        IValueMeta.TYPE_STRING),
    LEVENSHTEIN_DISTANCE(
        "LEVENSHTEIN_DISTANCE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.LevenshteinDistance"),
        IValueMeta.TYPE_INTEGER),
    METAPHONE(
        "METAPHONE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Metaphone"),
        IValueMeta.TYPE_STRING),
    DOUBLE_METAPHONE(
        "DOUBLE_METAPHONE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DoubleMetaphone"),
        IValueMeta.TYPE_STRING),
    ABS(
        "ABS",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Abs"),
        IValueMeta.TYPE_INTEGER),
    REMOVE_TIME_FROM_DATE(
        "REMOVE_TIME_FROM_DATE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RemoveTimeFromDate"),
        IValueMeta.TYPE_DATE),
    DATE_DIFF(
        "DATE_DIFF",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DateDiff"),
        IValueMeta.TYPE_INTEGER),
    ADD3("ADD3", "A + B + C", IValueMeta.TYPE_NUMBER),
    INITCAP(
        "INIT_CAP",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.InitCap"),
        IValueMeta.TYPE_STRING),
    UPPER_CASE(
        "UPPER_CASE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.UpperCase"),
        IValueMeta.TYPE_STRING),
    LOWER_CASE(
        "LOWER_CASE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.LowerCase"),
        IValueMeta.TYPE_STRING),
    MASK_XML(
        "MASK_XML",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.MaskXML"),
        IValueMeta.TYPE_STRING),
    USE_CDATA(
        "USE_CDATA",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.UseCDATA"),
        IValueMeta.TYPE_STRING),
    REMOVE_CR(
        "REMOVE_CR",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RemoveCR"),
        IValueMeta.TYPE_STRING),
    REMOVE_LF(
        "REMOVE_LF",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RemoveLF"),
        IValueMeta.TYPE_STRING),
    REMOVE_CRLF(
        "REMOVE_CRLF",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RemoveCRLF"),
        IValueMeta.TYPE_STRING),
    REMOVE_TAB(
        "REMOVE_TAB",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RemoveTAB"),
        IValueMeta.TYPE_STRING),
    GET_ONLY_DIGITS(
        "GET_ONLY_DIGITS",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.GetOnlyDigits"),
        IValueMeta.TYPE_STRING),
    REMOVE_DIGITS(
        "REMOVE_DIGITS",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RemoveDigits"),
        IValueMeta.TYPE_STRING),
    STRING_LEN(
        "STRING_LEN",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.StringLen"),
        IValueMeta.TYPE_INTEGER),
    LOAD_FILE_CONTENT_BINARY(
        "LOAD_FILE_CONTENT_BINARY",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.LoadFileContentInBinary"),
        IValueMeta.TYPE_BINARY),
    ADD_TIME_TO_DATE(
        "ADD_TIME_TO_DATE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.AddTimeToDate"),
        IValueMeta.TYPE_DATE),
    QUARTER_OF_DATE(
        "QUARTER_OF_DATE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.QuarterOfDate"),
        IValueMeta.TYPE_INTEGER),
    SUBSTITUTE_VARIABLE(
        "SUBSTITUTE_VARIABLE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.SubstituteVariable"),
        IValueMeta.TYPE_STRING),
    UNESCAPE_XML(
        "UNESCAPE_XML",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.UnescapeXML"),
        IValueMeta.TYPE_STRING),
    ESCAPE_HTML(
        "ESCAPE_HTML",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.EscapeHTML"),
        IValueMeta.TYPE_STRING),
    UNESCAPE_HTML(
        "UNESCAPE_HTML",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.UnescapeHTML"),
        IValueMeta.TYPE_STRING),
    ESCAPE_SQL(
        "ESCAPE_SQL",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.EscapeSQL"),
        IValueMeta.TYPE_STRING),
    DATE_WORKING_DIFF(
        "DATE_WORKING_DIFF",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DateDiffWorking"),
        IValueMeta.TYPE_INTEGER),
    ADD_MONTHS(
        "ADD_MONTHS",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DatePlusMonths"),
        IValueMeta.TYPE_DATE),
    CHECK_XML_FILE_WELL_FORMED(
        "CHECK_XML_FILE_WELL_FORMED",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.CheckXmlFileWellFormed"),
        IValueMeta.TYPE_BOOLEAN),
    CHECK_XML_WELL_FORMED(
        "CHECK_XML_WELL_FORMED",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.CheckXmlWellFormed"),
        IValueMeta.TYPE_BOOLEAN),
    GET_FILE_ENCODING(
        "GET_FILE_ENCODING",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.GetFileEncoding"),
        IValueMeta.TYPE_STRING),
    DAMERAU_LEVENSHTEIN(
        "DAMERAU_LEVENSHTEIN",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DamerauLevenshtein"),
        IValueMeta.TYPE_INTEGER),
    NEEDLEMAN_WUNSH(
        "NEEDLEMAN_WUNSH",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.NeedlemanWunsch"),
        IValueMeta.TYPE_INTEGER),
    JARO(
        "JARO",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Jaro"),
        IValueMeta.TYPE_NUMBER),
    JARO_WINKLER(
        "JARO_WINKLER",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.JaroWinkler"),
        IValueMeta.TYPE_NUMBER),
    SOUNDEX(
        "SOUNDEX",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.SoundEx"),
        IValueMeta.TYPE_STRING),
    REFINED_SOUNDEX(
        "REFINED_SOUNDEX",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RefinedSoundEx"),
        IValueMeta.TYPE_STRING),
    ADD_HOURS(
        "ADD_HOURS",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DatePlusHours"),
        IValueMeta.TYPE_DATE),
    ADD_MINUTES(
        "ADD_MINUTES",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DatePlusMinutes"),
        IValueMeta.TYPE_DATE),
    DATE_DIFF_MSEC(
        "DATE_DIFF_MSEC",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DateDiffMsec"),
        IValueMeta.TYPE_INTEGER),
    DATE_DIFF_SEC(
        "DATE_DIFF_SEC",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DateDiffSec"),
        IValueMeta.TYPE_INTEGER),
    DATE_DIFF_MN(
        "DATE_DIFF_MN",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DateDiffMn"),
        IValueMeta.TYPE_INTEGER),
    DATE_DIFF_HR(
        "DATE_DIFF_HR",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.DateDiffHr"),
        IValueMeta.TYPE_INTEGER),
    HOUR_OF_DAY(
        "HOUR_OF_DAY",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.HourOfDay"),
        IValueMeta.TYPE_INTEGER),
    MINUTE_OF_HOUR(
        "MINUTE_OF_HOUR",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.MinuteOfHour"),
        IValueMeta.TYPE_INTEGER),
    SECOND_OF_MINUTE(
        "SECOND_OF_MINUTE",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.SecondOfMinute"),
        IValueMeta.TYPE_INTEGER),
    ROUND_CUSTOM_1(
        "ROUND_CUSTOM_1",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RoundCustom"),
        IValueMeta.TYPE_NUMBER),
    ROUND_CUSTOM_2(
        "ROUND_CUSTOM_2",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.RoundCustom2"),
        IValueMeta.TYPE_NUMBER),
    ADD_SECONDS(
        "ADD_SECONDS",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.AddSeconds"),
        IValueMeta.TYPE_DATE),
    REMAINDER(
        "REMAINDER",
        BaseMessages.getString(PKG, "CalculatorMetaFunction.CalcFunctions.Remainder"),
        IValueMeta.TYPE_NUMBER),
    ;

    private String code;
    private String description;
    private int defaultResultType;

    CalculationType(String code, String description, int defaultResultType) {
      this.code = code;
      this.description = description;
      this.defaultResultType = defaultResultType;
    }

    public static String[] getDescriptions() {
      String[] descriptions = new String[values().length];
      for (int i = 0; i < descriptions.length; i++) {
        descriptions[i] = values()[i].getDescription();
      }
      return descriptions;
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }

    /**
     * Gets defaultResultType
     *
     * @return value of defaultResultType
     */
    public int getDefaultResultType() {
      return defaultResultType;
    }

    public static CalculationType getTypeWithCode(String code) {
      for (CalculationType value : values()) {
        if (value.getCode().equals(code)) {
          return value;
        }
      }
      return NONE;
    }

    public static CalculationType getTypeWithDescription(String description) {
      for (CalculationType value : values()) {
        if (value.getDescription().equals(description)) {
          return value;
        }
      }
      return NONE;
    }
  }

  @HopMetadataProperty(
      key = "field_name",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.FieldName")
  private String fieldName;

  @HopMetadataProperty(
      key = "calc_type",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.CalculationType",
      storeWithCode = true)
  private CalculationType calcType;

  @HopMetadataProperty(
      key = "field_a",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.FieldA")
  private String fieldA;

  @HopMetadataProperty(
      key = "field_b",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.FieldB")
  private String fieldB;

  @HopMetadataProperty(
      key = "field_c",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.FieldC")
  private String fieldC;

  @HopMetadataProperty(
      key = "value_type",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueType")
  private String valueType;

  @HopMetadataProperty(
      key = "value_length",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueLength")
  private int valueLength;

  @HopMetadataProperty(
      key = "value_precision",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValuePrecision")
  private int valuePrecision;

  @HopMetadataProperty(
      key = "conversion_mask",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueFormat")
  private String conversionMask;

  @HopMetadataProperty(
      key = "decimal_symbol",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueDecimal")
  private String decimalSymbol;

  @HopMetadataProperty(
      key = "grouping_symbol",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueGroup")
  private String groupingSymbol;

  @HopMetadataProperty(
      key = "currency_symbol",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.ValueCurrency")
  private String currencySymbol;

  @HopMetadataProperty(
      key = "remove",
      injectionKeyDescription = "CalculatorMeta.Injection.Calculation.Remove")
  private boolean removedFromResult;

  public CalculatorMetaFunction() {
    this.calcType = CalculationType.NONE;
  }

  /**
   * @param fieldName out field name
   * @param calcType calculation type, see CALC_* set of constants defined
   * @param fieldA name of field "A"
   * @param fieldB name of field "B"
   * @param fieldC name of field "C"
   * @param valueType out value type
   * @param valueLength out value length
   * @param valuePrecision out value precision
   * @param conversionMask out value conversion mask
   * @param decimalSymbol out value decimal symbol
   * @param groupingSymbol out value grouping symbol
   * @param currencySymbol out value currency symbol
   * @param removedFromResult If this result needs to be removed from the output
   */
  public CalculatorMetaFunction(
      String fieldName,
      CalculationType calcType,
      String fieldA,
      String fieldB,
      String fieldC,
      String valueType,
      int valueLength,
      int valuePrecision,
      String conversionMask,
      String decimalSymbol,
      String groupingSymbol,
      String currencySymbol,
      boolean removedFromResult) {
    this.fieldName = fieldName;
    this.calcType = calcType;
    this.fieldA = fieldA;
    this.fieldB = fieldB;
    this.fieldC = fieldC;
    this.valueType = valueType;
    this.valueLength = valueLength;
    this.valuePrecision = valuePrecision;
    this.conversionMask = conversionMask;
    this.decimalSymbol = decimalSymbol;
    this.groupingSymbol = groupingSymbol;
    this.currencySymbol = currencySymbol;
    this.removedFromResult = removedFromResult;
  }

  public CalculatorMetaFunction(CalculatorMetaFunction f) {
    this.fieldName = f.fieldName;
    this.calcType = f.calcType;
    this.fieldA = f.fieldA;
    this.fieldB = f.fieldB;
    this.fieldC = f.fieldC;
    this.valueType = f.valueType;
    this.valueLength = f.valueLength;
    this.valuePrecision = f.valuePrecision;
    this.conversionMask = f.conversionMask;
    this.decimalSymbol = f.decimalSymbol;
    this.groupingSymbol = f.groupingSymbol;
    this.currencySymbol = f.currencySymbol;
    this.removedFromResult = f.removedFromResult;
  }

  @Override
  public CalculatorMetaFunction clone() {
    return new CalculatorMetaFunction(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CalculatorMetaFunction that = (CalculatorMetaFunction) o;
    return valueLength == that.valueLength
        && valuePrecision == that.valuePrecision
        && removedFromResult == that.removedFromResult
        && Objects.equals(fieldName, that.fieldName)
        && calcType == that.calcType
        && Objects.equals(fieldA, that.fieldA)
        && Objects.equals(fieldB, that.fieldB)
        && Objects.equals(fieldC, that.fieldC)
        && Objects.equals(valueType, that.valueType)
        && Objects.equals(conversionMask, that.conversionMask)
        && Objects.equals(decimalSymbol, that.decimalSymbol)
        && Objects.equals(groupingSymbol, that.groupingSymbol)
        && Objects.equals(currencySymbol, that.currencySymbol);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fieldName,
        calcType,
        fieldA,
        fieldB,
        fieldC,
        valueType,
        valueLength,
        valuePrecision,
        conversionMask,
        decimalSymbol,
        groupingSymbol,
        currencySymbol,
        removedFromResult);
  }

  /**
   * Gets fieldName
   *
   * @return value of fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

  /** @param fieldName The fieldName to set */
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * Gets calcType
   *
   * @return value of calcType
   */
  public CalculationType getCalcType() {
    return calcType;
  }

  /** @param calcType The calcType to set */
  public void setCalcType(CalculationType calcType) {
    this.calcType = calcType;
  }

  /**
   * Gets fieldA
   *
   * @return value of fieldA
   */
  public String getFieldA() {
    return fieldA;
  }

  /** @param fieldA The fieldA to set */
  public void setFieldA(String fieldA) {
    this.fieldA = fieldA;
  }

  /**
   * Gets fieldB
   *
   * @return value of fieldB
   */
  public String getFieldB() {
    return fieldB;
  }

  /** @param fieldB The fieldB to set */
  public void setFieldB(String fieldB) {
    this.fieldB = fieldB;
  }

  /**
   * Gets fieldC
   *
   * @return value of fieldC
   */
  public String getFieldC() {
    return fieldC;
  }

  /** @param fieldC The fieldC to set */
  public void setFieldC(String fieldC) {
    this.fieldC = fieldC;
  }

  /**
   * Gets valueType
   *
   * @return value of valueType
   */
  public String getValueType() {
    return valueType;
  }

  /** @param valueType The valueType to set */
  public void setValueType(String valueType) {
    this.valueType = valueType;
  }

  /**
   * Gets valueLength
   *
   * @return value of valueLength
   */
  public int getValueLength() {
    return valueLength;
  }

  /** @param valueLength The valueLength to set */
  public void setValueLength(int valueLength) {
    this.valueLength = valueLength;
  }

  /**
   * Gets valuePrecision
   *
   * @return value of valuePrecision
   */
  public int getValuePrecision() {
    return valuePrecision;
  }

  /** @param valuePrecision The valuePrecision to set */
  public void setValuePrecision(int valuePrecision) {
    this.valuePrecision = valuePrecision;
  }

  /**
   * Gets conversionMask
   *
   * @return value of conversionMask
   */
  public String getConversionMask() {
    return conversionMask;
  }

  /** @param conversionMask The conversionMask to set */
  public void setConversionMask(String conversionMask) {
    this.conversionMask = conversionMask;
  }

  /**
   * Gets decimalSymbol
   *
   * @return value of decimalSymbol
   */
  public String getDecimalSymbol() {
    return decimalSymbol;
  }

  /** @param decimalSymbol The decimalSymbol to set */
  public void setDecimalSymbol(String decimalSymbol) {
    this.decimalSymbol = decimalSymbol;
  }

  /**
   * Gets groupingSymbol
   *
   * @return value of groupingSymbol
   */
  public String getGroupingSymbol() {
    return groupingSymbol;
  }

  /** @param groupingSymbol The groupingSymbol to set */
  public void setGroupingSymbol(String groupingSymbol) {
    this.groupingSymbol = groupingSymbol;
  }

  /**
   * Gets currencySymbol
   *
   * @return value of currencySymbol
   */
  public String getCurrencySymbol() {
    return currencySymbol;
  }

  /** @param currencySymbol The currencySymbol to set */
  public void setCurrencySymbol(String currencySymbol) {
    this.currencySymbol = currencySymbol;
  }

  /**
   * Gets removedFromResult
   *
   * @return value of removedFromResult
   */
  public boolean isRemovedFromResult() {
    return removedFromResult;
  }

  /** @param removedFromResult The removedFromResult to set */
  public void setRemovedFromResult(boolean removedFromResult) {
    this.removedFromResult = removedFromResult;
  }
}
