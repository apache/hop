/*!
 * Copyright 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.pentaho.cassandra.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SelectorTest {

  private static final String ARG_FUNCTION = "WRITETIME";

  private static final String ARG_ALIAS = "alias";

  private static final String ARG_NAME = "name";

  private static final String ARG_FUNCTION_NAME = "writetime(Argument)";

  private Selector actualSelector;

  @Test
  public void testSelector_ByNameAliasFunction() {
    actualSelector = new Selector( ARG_FUNCTION_NAME, ARG_ALIAS, ARG_FUNCTION );
    assertNotNull( actualSelector );
    assertEquals( ARG_FUNCTION_NAME, actualSelector.getColumnName() );
    assertEquals( ARG_ALIAS, actualSelector.getAlias() );
    assertEquals( ARG_FUNCTION, actualSelector.getFunction().name() );
    assertTrue( actualSelector.isFunction() );
  }

  @Test
  public void testSelector_ByNameFunction() {
    actualSelector = new Selector( ARG_FUNCTION_NAME, null, ARG_FUNCTION );
    assertNotNull( actualSelector );
    assertEquals( ARG_FUNCTION_NAME, actualSelector.getColumnName() );
    assertNull( actualSelector.getAlias() );
    assertEquals( ARG_FUNCTION, actualSelector.getFunction().name() );
    assertTrue( actualSelector.isFunction() );
  }

  @Test
  public void testSelector_ByName() {
    actualSelector = new Selector( ARG_NAME );
    assertNotNull( actualSelector );
    assertEquals( ARG_NAME, actualSelector.getColumnName() );
    assertNull( actualSelector.getAlias() );
    assertNull( actualSelector.getFunction() );
    assertFalse( actualSelector.isFunction() );
  }

  @Test
  public void testSelector_ByNameAlias() {
    actualSelector = new Selector( ARG_NAME, ARG_ALIAS );
    assertNotNull( actualSelector );
    assertEquals( ARG_NAME, actualSelector.getColumnName() );
    assertEquals( ARG_ALIAS, actualSelector.getAlias() );
    assertNull( actualSelector.getFunction() );
    assertFalse( actualSelector.isFunction() );
  }

  @Test
  public void testCaseInsensetiveFunction_NameLowerCase() {
    Selector selector = new Selector( "Token(Test)", null, "TOKEN" );
    String columnName = selector.getColumnName();
    assertEquals( "token(Test)", columnName );
  }

  @Test
  public void testCaseSensetiveFunction_NameInOriginCase() {
    Selector selector = new Selector( "DaTeOF(Test)", null, "DATEOF" );
    String columnName = selector.getColumnName();
    assertEquals( "DaTeOF(Test)", columnName );
  }

  @Test
  public void testCaseInSensetiveFunctionCount_SpecificCase() {
    Selector selector = new Selector( "COUNT", null, "COUNT" );
    assertNotNull( selector );
    assertEquals( "count", selector.getColumnName() );
    assertNull( selector.getAlias() );
    assertEquals( "COUNT", selector.getFunction().name() );
    assertTrue( selector.isFunction() );
  }

  @Test
  public void testSelector_ByNullName() {
    Selector selector = new Selector( null );
    String columnName = selector.getColumnName();
    assertNotNull( selector );
    assertNull( columnName );
  }

}
