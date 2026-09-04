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

package org.apache.hop.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DataSetRowMetaTest {

  @BeforeAll
  static void initHop() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void createFieldsFromRowMetaCopiesNameTypeLengthPrecisionFormat() {
    IRowMeta rowMeta = new RowMeta();
    ValueMetaString name = new ValueMetaString("name", 50, -1);
    name.setComments("customer name");
    name.setConversionMask(null);
    rowMeta.addValueMeta(name);
    ValueMetaInteger id = new ValueMetaInteger("id", 9, 0);
    id.setConversionMask("0");
    rowMeta.addValueMeta(id);

    List<DataSetField> fields = DataSet.createFieldsFromRowMeta(rowMeta);

    assertEquals(2, fields.size());
    assertEquals("name", fields.get(0).getFieldName());
    assertEquals(name.getType(), fields.get(0).getType());
    assertEquals(50, fields.get(0).getLength());
    assertEquals("customer name", fields.get(0).getComment());
    assertEquals("id", fields.get(1).getFieldName());
    assertEquals(id.getType(), fields.get(1).getType());
    assertEquals("0", fields.get(1).getFormat());
  }

  @Test
  void validateRowMetaAcceptsMatchingLayout() throws HopException {
    DataSet dataSet = sampleDataSet();
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("id"));
    input.addValueMeta(new ValueMetaString("name"));
    dataSet.validateRowMeta(input);
  }

  @Test
  void validateRowMetaAcceptsCaseInsensitiveNames() throws HopException {
    DataSet dataSet = sampleDataSet();
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("ID"));
    input.addValueMeta(new ValueMetaString("NAME"));
    dataSet.validateRowMeta(input);
  }

  @Test
  void validateRowMetaRejectsTypeMismatch() {
    DataSet dataSet = sampleDataSet();
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("id"));
    input.addValueMeta(new ValueMetaString("name"));
    HopException exception = assertThrows(HopException.class, () -> dataSet.validateRowMeta(input));
    assertTrue(exception.getMessage().contains("type"));
  }

  @Test
  void validateRowMetaRejectsNameAndCountMismatch() {
    DataSet dataSet = sampleDataSet();
    IRowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("id"));
    HopException exception = assertThrows(HopException.class, () -> dataSet.validateRowMeta(input));
    assertTrue(exception.getMessage().contains("field count"));
  }

  private DataSet sampleDataSet() {
    DataSet dataSet = new DataSet();
    dataSet.setName("customers");
    dataSet.setFields(
        List.of(
            new DataSetField("id", IValueMeta.TYPE_INTEGER, 9, 0, "", "0"),
            new DataSetField("name", IValueMeta.TYPE_STRING, 50, -1, "", "")));
    return dataSet;
  }
}
