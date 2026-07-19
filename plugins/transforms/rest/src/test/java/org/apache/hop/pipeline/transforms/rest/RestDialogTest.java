/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.rest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.hop.ui.core.widget.ColumnInfo;
import org.junit.jupiter.api.Test;

class RestDialogTest {

  @Test
  void populatesQueryAndMatrixParameterFieldLists() {
    ColumnInfo[] queryColumns = parameterColumns();
    ColumnInfo[] matrixColumns = parameterColumns();
    String[] fieldNames = {"id", "search_value"};

    RestDialog.setParameterComboValues(queryColumns, matrixColumns, fieldNames);

    assertArrayEquals(fieldNames, queryColumns[0].getComboValues());
    assertArrayEquals(fieldNames, matrixColumns[0].getComboValues());
  }

  private static ColumnInfo[] parameterColumns() {
    return new ColumnInfo[] {
      new ColumnInfo("Parameter field", ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {""}, false)
    };
  }
}
