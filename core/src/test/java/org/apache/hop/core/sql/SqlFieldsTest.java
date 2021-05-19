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

package org.apache.hop.core.sql;

import org.apache.hop.core.row.IRowMeta;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SqlFieldsTest {

  @Test
  public void testSqlFromFields01() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldsClause = "A, B";

    SqlFields fromFields = new SqlFields("Service", rowMeta, fieldsClause);
    assertFalse(fromFields.isDistinct());

    assertEquals(2, fromFields.getFields().size());

    SqlField field = fromFields.getFields().get(0);
    assertEquals("A", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("A", field.getValueMeta().getName().toUpperCase());

    field = fromFields.getFields().get(1);
    assertEquals("B", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("B", field.getValueMeta().getName().toUpperCase());
  }

  @Test
  public void testSqlFromFields02() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldsClause = "A as foo, B";

    SqlFields fromFields = new SqlFields("Service", rowMeta, fieldsClause);
    assertFalse(fromFields.isDistinct());

    assertEquals(2, fromFields.getFields().size());

    SqlField field = fromFields.getFields().get(0);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("A", field.getValueMeta().getName().toUpperCase());

    field = fromFields.getFields().get(1);
    assertEquals("B", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("B", field.getValueMeta().getName().toUpperCase());
  }

  @Test
  public void testSqlFromFields03() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldsClause = "A, sum( B ) as \"Total sales\"";

    SqlFields fromFields = new SqlFields("Service", rowMeta, fieldsClause);
    assertFalse(fromFields.isDistinct());

    assertEquals(2, fromFields.getFields().size());

    SqlField field = fromFields.getFields().get(0);
    assertEquals("A", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("A", field.getValueMeta().getName().toUpperCase());

    field = fromFields.getFields().get(1);
    assertEquals("B", field.getName());
    assertEquals("Total sales", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("B", field.getValueMeta().getName().toUpperCase());
  }

  @Test
  public void testSqlFromFields04() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldsClause = "DISTINCT A as foo, B as bar";

    SqlFields fromFields = new SqlFields("Service", rowMeta, fieldsClause);
    assertTrue(fromFields.isDistinct());

    assertEquals(2, fromFields.getFields().size());

    SqlField field = fromFields.getFields().get(0);
    assertEquals("A", field.getName());
    assertEquals("foo", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("A", field.getValueMeta().getName().toUpperCase());

    field = fromFields.getFields().get(1);
    assertEquals("B", field.getName());
    assertEquals("bar", field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("B", field.getValueMeta().getName().toUpperCase());
  }

  @Test
  public void testSqlFromFields05() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldsClause = "*";

    SqlFields fromFields = new SqlFields("Service", rowMeta, fieldsClause);
    assertFalse(fromFields.isDistinct());

    assertEquals(2, fromFields.getFields().size());

    SqlField field = fromFields.getFields().get(0);
    assertEquals("A", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("A", field.getValueMeta().getName().toUpperCase());

    field = fromFields.getFields().get(1);
    assertEquals("B", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("B", field.getValueMeta().getName().toUpperCase());
  }

  @Test
  public void testSqlFromFields05Alias() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldsClause = "Service.*";

    SqlFields fromFields = new SqlFields("Service", rowMeta, fieldsClause);
    assertFalse(fromFields.isDistinct());

    assertEquals(2, fromFields.getFields().size());

    SqlField field = fromFields.getFields().get(0);
    assertEquals("A", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("A", field.getValueMeta().getName().toUpperCase());

    field = fromFields.getFields().get(1);
    assertEquals("B", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("B", field.getValueMeta().getName().toUpperCase());
  }

  @Test
  public void testSqlFromFields06() throws Exception {
    IRowMeta rowMeta = SqlTest.generateTest2RowMeta();

    String fieldsClause = "*, *";

    SqlFields fromFields = new SqlFields("Service", rowMeta, fieldsClause);
    assertFalse(fromFields.isDistinct());

    assertEquals(4, fromFields.getFields().size());

    SqlField field = fromFields.getFields().get(0);
    assertEquals("A", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("A", field.getValueMeta().getName().toUpperCase());

    field = fromFields.getFields().get(1);
    assertEquals("B", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("B", field.getValueMeta().getName().toUpperCase());

    field = fromFields.getFields().get(2);
    assertEquals("A", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("A", field.getValueMeta().getName().toUpperCase());

    field = fromFields.getFields().get(3);
    assertEquals("B", field.getName());
    assertNull(field.getAlias());
    assertNotNull("The service data type was not discovered", field.getValueMeta());
    assertEquals("B", field.getValueMeta().getName().toUpperCase());
  }
}
