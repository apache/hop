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

package org.apache.hop.pipeline.transforms.jsoninput.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A JSON number is a number: it goes into a numeric field as the value it is, whatever the format
 * and the symbols of the field are. Those are for showing the field and for parsing a JSON string.
 * The value has to be what parsing the number's text with a dot as decimal symbol gives, and the
 * debug message of the failed cast tells whether a value took the old route.
 */
class RowOutputConverterNumberTest {

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  private static IValueMeta field(int type, String mask, String decimalSymbol) throws HopException {
    IValueMeta meta = ValueMetaFactory.createValueMeta("field", type);
    meta.setConversionMask(mask);
    meta.setDecimalSymbol(decimalSymbol);
    meta.setGroupingSymbol(".".equals(decimalSymbol) ? "," : ".");
    return meta;
  }

  /** Every way a numeric field of a type can be set up: none of it may change the value. */
  private static List<IValueMeta> fields(int type) throws HopException {
    return List.of(
        field(type, null, "."),
        field(type, null, ","),
        field(type, "#,##0.00", "."),
        field(type, "#,##0.00", ","),
        field(type, "0.00E0", "."),
        field(type, "#%", "."),
        ValueMetaFactory.createValueMeta("field", type));
  }

  /** What parsing the text of the number gives in a field that reads a dot as decimal symbol. */
  private static Object fromTextWithADot(int type, Object jsonValue) throws HopException {
    IValueMeta plain = field(type, null, ".");
    return plain.convertDataFromString(
        RowOutputConverter.getStringValue(jsonValue),
        ValueMetaFactory.createParseMeta(plain),
        null,
        null,
        plain.getTrimType());
  }

  private static Object convert(IValueMeta target, Object jsonValue, ILogChannel log)
      throws HopException {
    JsonInputData data = new JsonInputData();
    data.outputRowMeta = new RowMeta();
    data.outputRowMeta.addValueMeta(target);
    data.convertRowMeta = ValueMetaFactory.createParseRowMeta(data.outputRowMeta);
    data.totalpreviousfields = 0;
    data.repeatedFields = new BitSet(1);
    return new RowOutputConverter(log).getRow(new Object[1], new Object[] {jsonValue}, data)[0];
  }

  private static ILogChannel debugLog() {
    ILogChannel log = mock(ILogChannel.class);
    when(log.isDebug()).thenReturn(true);
    return log;
  }

  /** The value goes in directly, for every set-up of the field, and is the dot-decimal value. */
  private static void assertTakenAsTheNumberItIs(int type, Object jsonValue) throws HopException {
    Object expected = fromTextWithADot(type, jsonValue);
    for (IValueMeta target : fields(type)) {
      ILogChannel log = debugLog();
      Object actual = convert(target, jsonValue, log);
      String what =
          jsonValue.getClass().getSimpleName()
              + " "
              + jsonValue
              + " into "
              + target.getTypeDesc()
              + " mask="
              + target.getConversionMask()
              + " decimal="
              + target.getDecimalSymbol();
      if (expected instanceof BigDecimal big) {
        assertEquals(0, big.compareTo((BigDecimal) actual), what);
      } else {
        assertEquals(expected, actual, what);
      }
      // the failed cast leaves a debug message; a value that is taken directly leaves none
      verify(log, never()).logDebug(anyString(), any(Throwable.class));
    }
  }

  private static final int[] NUMERIC_TYPES = {
    IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER
  };

  @Test
  void wholeNumbers() throws HopException {
    Object[] values = {
      12,
      12L,
      (short) 12,
      -7,
      0,
      Long.MAX_VALUE,
      IntNode.valueOf(12),
      LongNode.valueOf(-7L),
      BigIntegerNode.valueOf(BigInteger.valueOf(123456789012L))
    };
    for (int type : NUMERIC_TYPES) {
      for (Object value : values) {
        assertTakenAsTheNumberItIs(type, value);
      }
    }
  }

  @Test
  void decimals() throws HopException {
    double[] decimals = {199.5, 12.4, -12.5, 0.0, 0.1 + 0.2, 9999999.75, 1.0E10, 1.0E-5, 42.0};
    for (int type : NUMERIC_TYPES) {
      for (double decimal : decimals) {
        // the default reader hands over a Double, a JSON value in a field a DoubleNode
        assertTakenAsTheNumberItIs(type, decimal);
        assertTakenAsTheNumberItIs(type, DoubleNode.valueOf(decimal));
      }
      assertTakenAsTheNumberItIs(type, DecimalNode.valueOf(new BigDecimal("12345.6789")));
      assertTakenAsTheNumberItIs(type, FloatNode.valueOf(0.1f));
    }
  }

  @Test
  void aDecimalIntoAnIntegerFieldLosesItsFraction() throws HopException {
    assertEquals(12L, convert(field(IValueMeta.TYPE_INTEGER, null, ","), 12.9d, debugLog()));
    assertEquals(
        -12L,
        convert(field(IValueMeta.TYPE_INTEGER, null, ","), DoubleNode.valueOf(-12.9), debugLog()));
  }

  /** The case that gave 1995 for 199.5 on a machine with the comma as decimal symbol. */
  @Test
  void theDecimalSymbolOfTheFieldDoesNotChangeAJsonNumber() throws HopException {
    IValueMeta comma = field(IValueMeta.TYPE_NUMBER, null, ",");

    assertEquals(199.5d, convert(comma, DoubleNode.valueOf(199.5), debugLog()));
    assertEquals(199.5d, convert(comma, 199.5d, debugLog()));
    assertEquals(
        0,
        new BigDecimal("199.5")
            .compareTo(
                (BigDecimal)
                    convert(
                        field(IValueMeta.TYPE_BIGNUMBER, null, ","),
                        DoubleNode.valueOf(199.5),
                        debugLog())));
  }

  /** A JSON string is text: it is still read with the format and the symbols of the field. */
  @Test
  void aJsonStringIsStillParsedWithTheFormatOfTheField() throws HopException {
    // the mask is always written with a dot and a comma; the symbols say what stands for them
    IValueMeta comma = field(IValueMeta.TYPE_NUMBER, "#,##0.00", ",");
    assertEquals(1199.5d, convert(comma, TextNode.valueOf("1.199,50"), debugLog()));
    assertEquals(1199.5d, convert(comma, "1.199,50", debugLog()));

    IValueMeta dot = field(IValueMeta.TYPE_NUMBER, "#,##0.00", ".");
    assertEquals(1199.5d, convert(dot, TextNode.valueOf("1,199.50"), debugLog()));

    // text the format cannot read is an error, as it was
    assertThrows(
        HopException.class,
        () -> convert(field(IValueMeta.TYPE_INTEGER, "$#", "."), "twelve", debugLog()));
  }

  /** A whole number too large for a long takes the route it always took into an Integer field. */
  @Test
  void aWholeNumberBeyondALongIsLeftToTheOldRouteForAnIntegerField() throws HopException {
    BigIntegerNode huge = BigIntegerNode.valueOf(new BigInteger("123456789012345678901234567890"));
    IValueMeta target = field(IValueMeta.TYPE_INTEGER, null, ".");

    assertEquals(
        fromTextWithADot(IValueMeta.TYPE_INTEGER, huge), convert(target, huge, debugLog()));
    // Number and BigNumber fields can hold it
    assertTakenAsTheNumberItIs(IValueMeta.TYPE_NUMBER, huge);
    assertTakenAsTheNumberItIs(IValueMeta.TYPE_BIGNUMBER, huge);
  }
}
