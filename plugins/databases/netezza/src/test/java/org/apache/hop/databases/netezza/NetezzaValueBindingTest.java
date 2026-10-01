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

package org.apache.hop.databases.netezza;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Types;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The Netezza driver does not return a usable value from getDate on a TIME column. This used to be
 * an isNetezzaVariant() check inside ValueMetaBase.
 */
class NetezzaValueBindingTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private Object read(int columnType) throws Exception {
    ResultSetMetaData rm = mock(ResultSetMetaData.class);
    when(rm.getColumnType(1)).thenReturn(columnType);
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getMetaData()).thenReturn(rm);
    when(resultSet.getTime(1)).thenReturn(new Time(0L));
    when(resultSet.getDate(1)).thenReturn(new java.sql.Date(0L));

    ValueMetaDate valueMeta = new ValueMetaDate("d");
    valueMeta.setPrecision(1);
    // Through the dialect, which is where the engine asks for a value.
    return new NetezzaDatabaseMeta().getValueFromResultSet(resultSet, valueMeta, 0);
  }

  @Test
  void aTimeColumnIsReadAsATime() throws Exception {
    assertEquals(Time.class, read(Types.TIME).getClass());
  }

  @Test
  void anyOtherColumnIsReadAsADate() throws Exception {
    assertEquals(java.sql.Date.class, read(Types.DATE).getClass());
  }
}
