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

package org.apache.hop.core.file;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.gui.ITextFileInputField;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** Describes a single field in a text file */
@Getter
@Setter
public class TextFileInputField implements ITextFileInputField {
  @HopMetadataProperty(key = "name", injectionKey = "FIELD_NAME")
  private String name;

  @HopMetadataProperty(key = "position", injectionKey = "FIELD_POSITION")
  private int position;

  @HopMetadataProperty(key = "length", injectionKey = "FIELD_LENGTH")
  private int length;

  @HopMetadataProperty(
      key = "type",
      injectionKey = "FIELD_TYPE",
      intCodeConverter = ValueMetaBase.ValueTypeCodeConverter.class)
  private int type;

  @HopMetadataProperty(key = "ignore", injectionKey = "FIELD_IGNORE")
  private boolean ignore;

  @HopMetadataProperty(key = "format", injectionKey = "FIELD_FORMAT")
  private String format;

  @HopMetadataProperty(
      key = "trim_type",
      injectionKey = "FIELD_TRIM_TYPE",
      intCodeConverter = ValueMetaBase.TrimTypeCodeConverter.class)
  private int trimType;

  @HopMetadataProperty(key = "precision", injectionKey = "FIELD_PRECISION")
  private int precision;

  @HopMetadataProperty(key = "currency", injectionKey = "FIELD_CURRENCY")
  private String currencySymbol;

  @HopMetadataProperty(key = "decimal", injectionKey = "FIELD_DECIMAL")
  private String decimalSymbol;

  @HopMetadataProperty(key = "group", injectionKey = "FIELD_GROUP")
  private String groupSymbol;

  @HopMetadataProperty(key = "repeat", injectionKey = "FIELD_REPEAT")
  private boolean repeated;

  @HopMetadataProperty(key = "null_string", injectionKey = "FIELD_NULL_STRING")
  private String nullString;

  @HopMetadataProperty(key = "if_null_value", injectionKey = "FIELD_IF_NULL")
  private String ifNullValue;

  private String[] samples;

  private static final String[] dateFormats =
      new String[] {
        "yyyy/MM/dd HH:mm:ss.SSS", "yyyy/MM/dd HH:mm:ss", "dd/MM/yyyy", "dd-MM-yyyy", "yyyy/MM/dd",
            "yyyy-MM-dd",
        "yyyyMMdd", "ddMMyyyy", "d-M-yyyy", "d/M/yyyy", "d-M-yy", "d/M/yy",
      };

  private static final String[] numberFormats =
      new String[] {
        "",
        "#",
        Const.DEFAULT_NUMBER_FORMAT,
        "0.00",
        "0000000000000",
        "###,###,###.#######",
        "###############.###############",
        "#####.###############%",
      };

  public TextFileInputField() {
    this(null, -9, -9);
  }

  public TextFileInputField(String fieldname, int position, int length) {
    this.name = fieldname;
    this.position = position;
    this.length = length;
    this.type = IValueMeta.TYPE_STRING;
    this.ignore = false;
    this.format = "";
    this.trimType = IValueMeta.TRIM_TYPE_NONE;
    this.groupSymbol = "";
    this.decimalSymbol = "";
    this.currencySymbol = "";
    this.precision = -1;
    this.repeated = false;
    this.nullString = "";
    this.ifNullValue = "";
  }

  public TextFileInputField(TextFileInputField f) {
    this();
    this.name = f.name;
    this.type = f.type;
    this.length = f.length;
    this.precision = f.precision;
    this.format = f.format;
    this.decimalSymbol = f.decimalSymbol;
    this.groupSymbol = f.groupSymbol;
    this.currencySymbol = f.currencySymbol;
    this.trimType = f.trimType;
    this.ifNullValue = f.ifNullValue;
    this.nullString = f.nullString;
    this.repeated = f.repeated;
    this.ignore = f.ignore;
    this.position = f.position;
    this.samples = f.samples;
  }

  public TextFileInputField(String fieldName) {
    this(fieldName, -1, -1);
  }

  public int compare(Object obj) {
    TextFileInputField field = (TextFileInputField) obj;

    return position - field.getPosition();
  }

  @Override
  public int compareTo(ITextFileInputField field) {
    return position - field.getPosition();
  }

  public boolean equals(Object obj) {
    TextFileInputField field = (TextFileInputField) obj;

    return (position == field.getPosition());
  }

  @Override
  public TextFileInputField clone() {
    return new TextFileInputField(this);
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName(type);
  }

  public void setTypeWithString(String value) {
    this.type = ValueMetaFactory.getIdForValueMeta(value);
  }

  public boolean isIgnored() {
    return ignore;
  }

  public void setIgnored(boolean ignore) {
    this.ignore = ignore;
  }

  public void flipIgnored() {
    ignore = !ignore;
  }

  public String getTrimTypeCode() {
    return ValueMetaBase.getTrimTypeCode(trimType);
  }

  public String getTrimTypeDesc() {
    return ValueMetaBase.getTrimTypeDesc(trimType);
  }

  @Injection(name = "FIELD_TRIM_TYPE", group = "INPUT_FIELDS")
  public void setTrimTypeWithString(String value) {
    this.trimType = ValueMetaBase.getTrimTypeByCode(value);
  }

  public void flipRepeated() {
    repeated = !repeated;
  }

  @Override
  public String toString() {
    return name + "@" + position + ":" + length;
  }

  public void guess() {
    guessTrimType();
    guessType();
    guessIgnore();
  }

  public void guessTrimType() {
    boolean spacesBefore = false;
    boolean spacesAfter = false;

    for (int i = 0; i < samples.length; i++) {
      spacesBefore |= Const.nrSpacesBefore(samples[i]) > 0;
      spacesAfter |= Const.nrSpacesAfter(samples[i]) > 0;
      samples[i] = Const.trim(samples[i]);
    }

    trimType = IValueMeta.TRIM_TYPE_NONE;

    if (spacesBefore) {
      trimType |= IValueMeta.TRIM_TYPE_LEFT;
    }
    if (spacesAfter) {
      trimType |= IValueMeta.TRIM_TYPE_RIGHT;
    }
  }

  public void guessType() {
    // Guess fields...
    NumberFormat nf = NumberFormat.getInstance();
    DecimalFormat df = (DecimalFormat) nf;
    DecimalFormatSymbols dfs = new DecimalFormatSymbols();
    SimpleDateFormat daf = new SimpleDateFormat();

    daf.setLenient(false);

    // Start with a string...
    type = IValueMeta.TYPE_STRING;

    // If we have no samples, we assume a String...
    if (samples == null) {
      return;
    }

    // ////////////////////////////
    // DATES
    // ////////////////////////////

    // See if all samples can be transformed into a date...
    int datefmtCnt = dateFormats.length;
    boolean[] datefmt = new boolean[dateFormats.length];
    for (int i = 0; i < dateFormats.length; i++) {
      datefmt[i] = true;
    }
    int datenul = 0;

    for (String sample : samples) {
      if (!sample.isEmpty() && sample.equalsIgnoreCase(nullString)) {
        datenul++;
      } else {
        for (int x = 0; x < dateFormats.length; x++) {
          if (sample == null || Const.onlySpaces(sample) || sample.isEmpty()) {
            datefmt[x] = false;
            datefmtCnt--;
          }

          if (datefmt[x]) {
            try {
              daf.applyPattern(dateFormats[x]);
              Date date = daf.parse(sample);

              Calendar cal = Calendar.getInstance();
              cal.setTime(date);
              int year = cal.get(Calendar.YEAR);

              if (year < 1800 || year > 2200) {
                datefmt[x] = false; // Don't try it again in the future.
                datefmtCnt--; // One less that works..
              }
            } catch (Exception e) {
              datefmt[x] = false; // Don't try it again in the future.
              datefmtCnt--; // One less that works..
            }
          }
        }
      }
    }

    // If it is a date, copy info over to the format etc. Then return with the info.
    // If all samples where NULL values, we can't really decide what the type is.
    // So we're certainly not going to take a date, just take a string in that case.
    if (datefmtCnt > 0 && datenul != samples.length) {
      int first = -1;
      for (int i = 0; i < dateFormats.length && first < 0; i++) {
        if (datefmt[i]) {
          first = i;
        }
      }

      type = IValueMeta.TYPE_DATE;
      format = dateFormats[first];

      return;
    }

    // ////////////////////////////
    // NUMBERS
    // ////////////////////////////

    boolean isnumber = true;

    // Set decimal symbols to default
    decimalSymbol = "" + dfs.getDecimalSeparator();
    groupSymbol = "" + dfs.getGroupingSeparator();

    boolean[] numfmt = new boolean[numberFormats.length];
    int[] maxprecision = new int[numberFormats.length];
    for (int i = 0; i < numfmt.length; i++) {
      numfmt[i] = true;
      maxprecision[i] = -1;
    }
    int numfmtCnt = numberFormats.length;
    int numnul = 0;

    for (int i = 0; i < samples.length && isnumber; i++) {
      boolean containsDot = false;
      boolean containsComma = false;

      String field = samples[i];

      if (!field.isEmpty() && field.equalsIgnoreCase(nullString)) {
        numnul++;
      } else {
        for (int x = 0; x < field.length() && isnumber; x++) {
          char ch = field.charAt(x);
          if (!Character.isDigit(ch)
              && ch != '.'
              && ch != ','
              && (ch != '-' || x > 0)
              && ch != 'E'
              && ch != 'e' // exponential
          ) {
            isnumber = false;
            numfmtCnt = 0;
          } else {
            if (ch == '.') {
              containsDot = true;
            }
            if (ch == ',') {
              containsComma = true;
            }
          }
        }
        // If it's still a number, try to parse it as a double
        if (isnumber) {
          if (containsDot && !containsComma) { // American style 174.5

            dfs.setDecimalSeparator('.');
            decimalSymbol = ".";
            dfs.setGroupingSeparator(',');
            groupSymbol = ",";
          } else if (!containsDot && containsComma) { // European style 174,5

            dfs.setDecimalSeparator(',');
            decimalSymbol = ",";
            dfs.setGroupingSeparator('.');
            groupSymbol = ".";
          } else if (containsDot && containsComma) { // Both appear!

            // What's the last occurance: decimal point!
            int idxDot = field.indexOf('.');
            int idxCom = field.indexOf(',');
            if (idxDot > idxCom) {
              dfs.setDecimalSeparator('.');
              decimalSymbol = ".";
              dfs.setGroupingSeparator(',');
              groupSymbol = ",";
            } else {
              dfs.setDecimalSeparator(',');
              decimalSymbol = ",";
              dfs.setGroupingSeparator('.');
              groupSymbol = ".";
            }
          }

          // Try the remaining possible number formats!
          for (int x = 0; x < numberFormats.length; x++) {
            if (numfmt[x]) {
              boolean islong = true;

              try {
                int prec = -1;
                // Try long integers first....
                if (!containsDot && !containsComma) {
                  try {
                    Long.parseLong(field);
                    prec = 0;
                  } catch (Exception e) {
                    islong = false;
                  }
                }

                if (!islong) { // Try the double

                  df.setDecimalFormatSymbols(dfs);
                  df.applyPattern(numberFormats[x]);

                  double d = df.parse(field).doubleValue();
                  prec = guessPrecision(d);
                }
                if (prec > maxprecision[x]) {
                  maxprecision[x] = prec;
                }
              } catch (Exception e) {
                numfmt[x] = false; // Don't try it again in the future.
                numfmtCnt--; // One less that works..
              }
            }
          }
        }
      }
    }

    // Still a number? Grab the result and return.
    // If all sample strings are empty or represent NULL values we can't take a number as type.
    if (numfmtCnt > 0 && numnul != samples.length) {
      int first = -1;
      for (int i = 0; i < numberFormats.length && first < 0; i++) {
        if (numfmt[i]) {
          first = i;
        }
      }

      type = IValueMeta.TYPE_NUMBER;
      format = numberFormats[first];
      precision = maxprecision[first];

      return;
    }

    //
    // Assume it's a string...
    //
    type = IValueMeta.TYPE_STRING;
    format = "";
    precision = -1;
    decimalSymbol = "";
    groupSymbol = "";
    currencySymbol = "";
  }

  public static final int guessPrecision(double d) {
    int maxprec = 4;
    double maxdiff = 0.00005;

    // Make sure that 7.99995 == 8.00000
    // This is usually a rounding error!
    double diff = Math.abs(Math.floor(d) - d);
    if (diff < maxdiff) {
      return 0; // nothing behind decimal point...
    }

    // remainder: 12.345678 --> 0.345678
    for (int i = 1; i < maxprec; i++) { // cap off precision at a reasonable maximum
      double factor = Math.pow(10.0, i);
      diff = Math.abs(Math.floor(d * factor) - (d * factor));
      if (diff < maxdiff) {
        return i;
      }

      factor *= 10;
    }

    // Unknown length!
    return -1;
  }

  // Should a field be ignored?
  public void guessIgnore() {
    // If the string contains only spaces?
    boolean stop = false;
    for (int i = 0; i < samples.length && !stop; i++) {
      if (!Const.onlySpaces(samples[i])) {
        stop = true;
      }
    }
    if (!stop) {
      ignore = true;
      return;
    }

    // If all the strings are empty
    stop = false;
    for (int i = 0; i < samples.length && !stop; i++) {
      if (!samples[i].isEmpty()) {
        stop = true;
      }
    }
    if (!stop) {
      ignore = true;
      return;
    }

    // If all the strings are equivalent to NULL
    stop = false;
    for (int i = 0; i < samples.length && !stop; i++) {
      if (!samples[i].equalsIgnoreCase(nullString)) {
        stop = true;
      }
    }
    if (!stop) {
      ignore = true;
      return;
    }
  }

  @Override
  public ITextFileInputField createNewInstance(String newFieldname, int x, int newlength) {
    return new TextFileInputField(newFieldname, x, newlength);
  }

  @Override
  public IValueMeta toValueMeta(String fieldOriginTransformName, IVariables variables)
      throws HopPluginException {
    int hopType = getType();
    if (hopType == IValueMeta.TYPE_NONE) {
      hopType = IValueMeta.TYPE_STRING;
    }
    IValueMeta v =
        ValueMetaFactory.createValueMeta(
            variables != null ? variables.resolve(getName()) : getName(), hopType);
    v.setLength(getLength());
    v.setPrecision(getPrecision());
    v.setOrigin(fieldOriginTransformName);
    v.setConversionMask(getFormat());
    v.setDecimalSymbol(getDecimalSymbol());
    v.setGroupingSymbol(getGroupSymbol());
    v.setCurrencySymbol(getCurrencySymbol());
    v.setTrimType(getTrimType());
    return v;
  }
}
