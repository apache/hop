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

package org.apache.hop.pipeline.transforms.concatfields.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.concatfields.ConcatField;
import org.apache.hop.pipeline.transforms.concatfields.ConcatFieldsData;
import org.apache.hop.pipeline.transforms.concatfields.ConcatFieldsMeta;
import org.apache.hop.pipeline.transforms.concatfields.ExtraFields;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Unit test for {@link ConcatFieldHelper} */
class ConcatFieldHelperTests {

  @Test
  void testConcatFieldsBasic() throws Exception {
    // rowMeta
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("f1"));
    rowMeta.addValueMeta(new ValueMetaString("f2"));
    // input row
    Object[] inputRow = new Object[] {"hello", "world"};

    // data
    ConcatFieldsData data = new ConcatFieldsData();
    data.inputFieldIndexes = List.of(0, 1);
    data.outputFieldIndexes = List.of(0, 1);
    data.stringSeparator = ",";
    data.targetFieldLength = 50;
    data.trimType =
        new String[] {
          ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE),
          ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)
        };

    RowMeta outputRowMeta = new RowMeta();
    outputRowMeta.addValueMeta(new ValueMetaString("f1"));
    outputRowMeta.addValueMeta(new ValueMetaString("f2"));
    outputRowMeta.addValueMeta(new ValueMetaString("concat"));

    data.outputRowMeta = outputRowMeta;

    // meta
    ConcatFieldsMeta meta = mock(ConcatFieldsMeta.class);
    when(meta.isForceEnclosure()).thenReturn(false);
    when(meta.getOutputFields()).thenReturn(List.of());
    when(meta.getExtraFields()).thenReturn(new ExtraFields());

    // result
    Object[] result = ConcatFieldHelper.concat(inputRow, data, meta, rowMeta);

    assertEquals("hello,world", result[2]);
  }

  @Test
  void testConcatFieldsWithNullAndTrim() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("f1"));
    rowMeta.addValueMeta(new ValueMetaString("f2"));

    Object[] inputRow = new Object[] {" hello ", null};

    ConcatFieldsData data = new ConcatFieldsData();
    data.inputFieldIndexes = List.of(0, 1);
    data.outputFieldIndexes = List.of(0, 1);
    data.stringSeparator = "|";
    data.targetFieldLength = 50;
    data.trimType =
        new String[] {
          ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_BOTH),
          ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)
        };

    RowMeta outputRowMeta = new RowMeta();
    outputRowMeta.addValueMeta(new ValueMetaString("f1"));
    outputRowMeta.addValueMeta(new ValueMetaString("f2"));
    outputRowMeta.addValueMeta(new ValueMetaString("concat"));
    data.outputRowMeta = outputRowMeta;

    ConcatField field1 = new ConcatField();
    field1.setNullString("");

    ConcatField field2 = new ConcatField();
    field2.setNullString("NULL");

    ConcatFieldsMeta meta = mock(ConcatFieldsMeta.class);
    when(meta.isForceEnclosure()).thenReturn(false);
    when(meta.getOutputFields()).thenReturn(List.of(field1, field2));
    when(meta.getExtraFields()).thenReturn(new ExtraFields());

    Object[] result = ConcatFieldHelper.concat(inputRow, data, meta, rowMeta);

    assertEquals("hello|NULL", result[2]);
  }

  @Test
  void testConcatFieldsWithEnclosure() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("f1"));

    Object[] inputRow = new Object[] {"abc"};

    ConcatFieldsData data = new ConcatFieldsData();
    data.inputFieldIndexes = List.of(0);
    data.outputFieldIndexes = List.of(0, 1);
    data.stringSeparator = "";
    data.targetFieldLength = 10;
    data.trimType = new String[] {ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)};

    RowMeta outputRowMeta = new RowMeta();
    outputRowMeta.addValueMeta(new ValueMetaString("f1"));
    outputRowMeta.addValueMeta(new ValueMetaString("concat"));
    data.outputRowMeta = outputRowMeta;

    ConcatFieldsMeta meta = mock(ConcatFieldsMeta.class);
    when(meta.isForceEnclosure()).thenReturn(true);
    when(meta.getEnclosure()).thenReturn("\"");
    when(meta.getOutputFields()).thenReturn(List.of());
    when(meta.getExtraFields()).thenReturn(new ExtraFields());

    Object[] result = ConcatFieldHelper.concat(inputRow, data, meta, rowMeta);

    assertEquals("\"abc\"", result[1]);
  }

  @ParameterizedTest
  @MethodSource("inputRowProvider")
  void testConcatFieldsBasic(
      Object[] inputRow, boolean isForceEnclosure, boolean skipEmpty, String expected)
      throws Exception {
    // rowMeta
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("col1"));
    rowMeta.addValueMeta(new ValueMetaString("col2"));
    rowMeta.addValueMeta(new ValueMetaString("col3"));

    // data
    ConcatFieldsData data = new ConcatFieldsData();
    data.inputFieldIndexes = List.of(0, 1, 2);
    data.outputFieldIndexes = List.of(0, 1, 2);
    data.stringSeparator = ";";
    data.targetFieldLength = 50;
    data.trimType =
        new String[] {
          ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE),
          ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE),
          ValueMetaBase.getTrimTypeCode(IValueMeta.TRIM_TYPE_NONE)
        };

    RowMeta outputRowMeta = new RowMeta();
    outputRowMeta.addValueMeta(new ValueMetaString("col1"));
    outputRowMeta.addValueMeta(new ValueMetaString("col2"));
    outputRowMeta.addValueMeta(new ValueMetaString("col3"));
    outputRowMeta.addValueMeta(new ValueMetaString("concat"));

    data.outputRowMeta = outputRowMeta;

    // meta
    ConcatFieldsMeta meta = mock(ConcatFieldsMeta.class);
    when(meta.isForceEnclosure()).thenReturn(isForceEnclosure);
    when(meta.getEnclosure()).thenReturn("\"");
    when(meta.getOutputFields()).thenReturn(List.of());
    when(meta.getExtraFields()).thenReturn(new ExtraFields());
    when(meta.isSkipValueEmpty()).thenReturn(skipEmpty);

    // result
    Object[] result = ConcatFieldHelper.concat(inputRow, data, meta, rowMeta);
    assertEquals(expected, result[3]);
  }

  static Stream<Arguments> inputRowProvider() {
    return Stream.of(
        Arguments.of(new Object[] {"a", "b", "c"}, false, false, "a;b;c"),
        Arguments.of(new Object[] {"a", "b", null}, false, false, "a;b;"),
        Arguments.of(new Object[] {"a", null, null}, false, false, "a;;"),
        Arguments.of(new Object[] {null, "b", "c"}, false, false, ";b;c"),
        Arguments.of(new Object[] {null, "b", null}, false, false, ";b;"),
        Arguments.of(new Object[] {null, null, "c"}, false, false, ";;c"),
        Arguments.of(new Object[] {null, null, null}, false, false, ";;"),
        Arguments.of(new Object[] {"", "", ""}, false, false, ";;"),

        // enable force enclosure.
        Arguments.of(new Object[] {"a", "b", "c"}, true, false, "\"a\";\"b\";\"c\""),
        Arguments.of(new Object[] {"a", "b", null}, true, false, "\"a\";\"b\";\"\""),
        Arguments.of(new Object[] {"a", null, null}, true, false, "\"a\";\"\";\"\""),
        Arguments.of(new Object[] {null, "b", "c"}, true, false, "\"\";\"b\";\"c\""),
        Arguments.of(new Object[] {null, "b", null}, true, false, "\"\";\"b\";\"\""),
        Arguments.of(new Object[] {null, null, "c"}, true, false, "\"\";\"\";\"c\""),
        Arguments.of(new Object[] {null, null, null}, true, false, "\"\";\"\";\"\""),
        Arguments.of(new Object[] {"", "", ""}, true, false, "\"\";\"\";\"\""),

        // skip empty(null/"")
        Arguments.of(new Object[] {"a", "b", "c"}, false, true, "a;b;c"),
        Arguments.of(new Object[] {"a", "b", null}, false, true, "a;b"),
        Arguments.of(new Object[] {"a", null, null}, false, true, "a"),
        Arguments.of(new Object[] {null, "b", "c"}, false, true, "b;c"),
        Arguments.of(new Object[] {null, "b", null}, false, true, "b"),
        Arguments.of(new Object[] {null, null, "c"}, false, true, "c"),
        Arguments.of(new Object[] {null, null, null}, false, true, ""),
        Arguments.of(new Object[] {"", "", ""}, false, true, ""),

        // enable force enclosure, skip empty(null/"")
        Arguments.of(new Object[] {"a", "b", "c"}, true, true, "\"a\";\"b\";\"c\""),
        Arguments.of(new Object[] {"a", "b", null}, true, true, "\"a\";\"b\""),
        Arguments.of(new Object[] {"a", null, null}, true, true, "\"a\""),
        Arguments.of(new Object[] {null, "b", "c"}, true, true, "\"b\";\"c\""),
        Arguments.of(new Object[] {null, "b", null}, true, true, "\"b\""),
        Arguments.of(new Object[] {null, null, "c"}, true, true, "\"c\""),
        Arguments.of(new Object[] {null, null, null}, true, true, ""),
        Arguments.of(new Object[] {"", "", ""}, true, true, ""));
  }
}
