/*!
 * Copyright 2014 - 2018 Hitachi Vantara.  All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class CQLUtilsTest {

  /**
   * 
   */
  private static final String ALIAS_IS_INCORRECT = "Alias is incorrect:";
  /**
   * 
   */
  private static final String COLUMN_NAME_IS_INCORRECT = "Column Name is incorrect:";
  private String cqlExpression;
  private String expected;
  private String selectorExpression;
  private boolean isCql3;
  private Selector selector;
  private Selector expectedSelector;
  private String result;
  private Selector[] selectors;
  private Selector[] expectedSelectors;

  @Test
  public void testCleanQoutesMixed() {
    cqlExpression = "one_more as \"\"ALIAS\", field(a, b) as '\"aaaaa'";
    expected = "one_more as ALIAS, field(a, b) as aaaaa";
    result = CQLUtils.cleanQuotes( cqlExpression );
    assertEquals( expected, result );
  }

  @Test
  public void testCleanDoubleQoutes_IfAtTheBeginAndEnd() {
    cqlExpression = "\"Test\"";
    expected = "Test";
    result = CQLUtils.cleanQuotes( cqlExpression );
    assertEquals( expected, result );
  }

  @Test
  public void testCleanQoutes_IfAtTheBeginAndEnd() {
    cqlExpression = "'Test'";
    expected = "Test";
    result = CQLUtils.cleanQuotes( cqlExpression );
    assertEquals( expected, result );
  }

  @Test
  public void testCleanQoutes_IfInputNull() {
    cqlExpression = null;
    result = CQLUtils.cleanQuotes( cqlExpression );
    assertNull( result );
  }

  @Test
  public void testCleanMultipleWhitespaces() {
    cqlExpression = "     Test Test  Test   Test      Test    ";
    expected = "Test Test Test Test Test";
    result = CQLUtils.clean( cqlExpression );
    assertEquals( expected, result );
  }

  @Test
  public void testCleanUnnecessaryWhitespacesInFunctions() {
    cqlExpression = "function      (     arg1     ,      arg2     ,    arg3    )   as alias";
    expected = "function(arg1, arg2, arg3) as alias";
    result = CQLUtils.clean( cqlExpression );
    assertEquals( expected, result );
  }

  @Test
  public void testAddWhitespaceAfterComma() {
    cqlExpression = "selector1,selector2,selector3,function(a,b)";
    expected = "selector1, selector2, selector3, function(a, b)";
    result = CQLUtils.clean( cqlExpression );
    assertEquals( expected, result );
  }

  @Test
  public void testClean_IfInputNull() {
    cqlExpression = null;
    result = CQLUtils.clean( cqlExpression );
    assertNull( result );
  }

  @Test
  public void testGetSelectExpression_SelectClauseMixedWithUpperCase() {
    cqlExpression =
        "SELECT FIRST 25 DISTINCT selector1, selector2, selector3, f(a,b), selector4 as alias4 from cf where";
    expected = "selector1, selector2, selector3, f(a, b), selector4 as alias4";
    result = CQLUtils.getSelectExpression( cqlExpression );
    assertEquals( expected, result );
  }

  @Test
  public void testGetSelectExpression_SelectClauseWithFirstAndCorrectNumber() {
    cqlExpression =
        "select first    5054678   selector1, selector2, selector3, f(a,b), selector4 as alias4 from cf where";
    expected = "selector1, selector2, selector3, f(a, b), selector4 as alias4";
    result = CQLUtils.getSelectExpression( cqlExpression );
    assertEquals( expected, result );
  }

  @Test
  public void testGetSelectExpression_SelectClauseWithFirstAndWithoutNumber() {
    cqlExpression = "select first selector1, selector2, selector3, f(a,b), selector4 as alias4 from cf where";
    expected = "selector1, selector2, selector3, f(a, b), selector4 as alias4";
    result = CQLUtils.getSelectExpression( cqlExpression );
    assertEquals( expected, result );
  }

  @Test
  public void testGetSelectExpression_SelectClauseWithDistinct() {
    cqlExpression = "select distinct selector1, selector2, selector3, f(a,b), selector4 as alias4 from cf where";
    expected = "selector1, selector2, selector3, f(a, b), selector4 as alias4";
    result = CQLUtils.getSelectExpression( cqlExpression );
    assertEquals( expected, result );
  }

  @Test
  public void testGetSelectExpression_IfInputNull() {
    cqlExpression = null;
    result = CQLUtils.getSelectExpression( cqlExpression );
    assertNull( result );
  }

  @Test
  public void testGetSelectExpression_IfInputIsNotSelectClause() {
    cqlExpression = "test input, not select clause";
    result = CQLUtils.getSelectExpression( cqlExpression );
    assertNull( result );
  }

  @Test
  public void testSelectorForCQL3ColumnWithouAlias_NameShouldBeInLowerCase() {
    selectorExpression = "User";
    expectedSelector = new Selector( "user" );
    isCql3 = true;
    selector = CQLUtils.buildSelector( selectorExpression, isCql3 );
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selector.getColumnName() );
    assertNull( selector.getAlias() );
    assertNull( selector.getFunction() );
    assertFalse( selector.isFunction() );
  }

  @Test
  public void testSelectorForCQL3ColumnWithouAlias_NameShouldBeInOriginCase() {
    selectorExpression = "\"User\"";
    expectedSelector = new Selector( "User" );
    isCql3 = true;
    selector = CQLUtils.buildSelector( selectorExpression, isCql3 );
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selector.getColumnName() );
    assertNull( selector.getAlias() );
    assertNull( selector.getFunction() );
    assertFalse( selector.isFunction() );
  }

  @Test
  public void testSelectorForCQLColumnWithouAlias_NameShouldNotBeInLowerCase() {
    selectorExpression = "User";
    expectedSelector = new Selector( "User" );
    isCql3 = false;
    selector = CQLUtils.buildSelector( selectorExpression, isCql3 );
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selector.getColumnName() );
    assertNull( selector.getAlias() );
    assertNull( selector.getFunction() );
    assertFalse( selector.isFunction() );
  }

  @Test
  public void testSelectorForCQL3ColumnWithAlias_AliasShouldBeInLowerCase() {
    selectorExpression = "User as Alias";
    expectedSelector = new Selector( "user", "alias" );
    isCql3 = true;
    selector = CQLUtils.buildSelector( selectorExpression, isCql3 );
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selector.getColumnName() );
    assertEquals( expectedSelector.getAlias(), selector.getAlias() );
    assertNull( selector.getFunction() );
    assertFalse( selector.isFunction() );
  }

  @Test
  public void testSelectorForCQL3ColumnWithAlias_AliasShouldBeInOriginCase() {
    selectorExpression = "User as \"Alias\"";
    expectedSelector = new Selector( "user", "Alias" );
    isCql3 = true;
    selector = CQLUtils.buildSelector( selectorExpression, isCql3 );
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selector.getColumnName() );
    assertEquals( expectedSelector.getAlias(), selector.getAlias() );
    assertNull( selector.getFunction() );
    assertFalse( selector.isFunction() );
  }

  @Test
  public void testSelectorForCQLColumnWithAlias_AliasShouldNotBeInLowerCase() {
    selectorExpression = "User as Alias";
    expectedSelector = new Selector( "User", "Alias" );
    isCql3 = false;
    selector = CQLUtils.buildSelector( selectorExpression, isCql3 );
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selector.getColumnName() );
    assertEquals( expectedSelector.getAlias(), selector.getAlias() );
    assertNull( selector.getFunction() );
    assertFalse( selector.isFunction() );
  }

  @Test
  public void testSelectorForCQL3FunctionWithAlias() {
    selectorExpression = "token(a, b) as \"Alias\"";
    expectedSelector = new Selector( "token(a, b)", "Alias", "TOKEN" );
    isCql3 = true;
    selector = CQLUtils.buildSelector( selectorExpression, isCql3 );
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selector.getColumnName() );
    assertEquals( expectedSelector.getAlias(), selector.getAlias() );
    assertEquals( expectedSelector.getFunction(), selector.getFunction() );
    assertEquals( expectedSelector.isFunction(), selector.isFunction() );
  }

  @Test
  public void testSelectorForCQL3FunctionWithoutAlias() {
    selectorExpression = "token(a, b)";
    expectedSelector = new Selector( "token(a, b)", null, "TOKEN" );
    isCql3 = true;
    selector = CQLUtils.buildSelector( selectorExpression, isCql3 );
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selector.getColumnName() );
    assertNull( selector.getAlias() );
    assertEquals( expectedSelector.getFunction(), selector.getFunction() );
    assertEquals( expectedSelector.isFunction(), selector.isFunction() );
  }

  @Test
  public void testSelectorForCQL3FunctionName_ShouldBeInOriginCase() {
    selectorExpression = "TOKEN(a, b)";
    expectedSelector = new Selector( "TOKEN(a, b)", null, "TOKEN" );
    isCql3 = false;
    selector = CQLUtils.buildSelector( selectorExpression, isCql3 );
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selector.getColumnName() );
    assertNull( selector.getAlias() );
    assertEquals( expectedSelector.getFunction(), selector.getFunction() );
    assertEquals( expectedSelector.isFunction(), selector.isFunction() );
  }

  @Test
  public void testOneFunctionWithoutAliasInSelectorList_CQL3TurnedOn() {
    cqlExpression = "token(a,b)";
    expectedSelector = new Selector( "token(a, b)", null, "TOKEN" );
    isCql3 = true;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );

    assertNotNull( selectors );
    assertEquals( 1, selectors.length );
    // check selector
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selectors[0].getColumnName() );
    assertNull( selectors[0].getAlias() );
    assertEquals( expectedSelector.getFunction(), selectors[0].getFunction() );
    assertEquals( expectedSelector.isFunction(), selectors[0].isFunction() );
  }

  @Test
  public void testOneFunctionWithoutAliasInSelectorList_CQL3TurnedOFF() {
    cqlExpression = "token(a,b)";
    expectedSelector = new Selector( "token(a, b)", null, "TOKEN" );
    isCql3 = false;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );

    assertNotNull( selectors );
    assertEquals( 1, selectors.length );
    // check selector
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selectors[0].getColumnName() );
    assertNull( selectors[0].getAlias() );
    assertEquals( expectedSelector.getFunction(), selectors[0].getFunction() );
    assertEquals( expectedSelector.isFunction(), selectors[0].isFunction() );
  }

  @Test
  public void testOneColumnWithoutAliasInSelectorList_CQL3TurnedOn() {
    cqlExpression = "Column";
    expectedSelector = new Selector( "column" );
    isCql3 = true;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );

    assertNotNull( selectors );
    assertEquals( 1, selectors.length );
    // check selector
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selectors[0].getColumnName() );
    assertNull( selectors[0].getAlias() );
    assertNull( selectors[0].getFunction() );
    assertFalse( selectors[0].isFunction() );
  }

  @Test
  public void testOneColumnWithoutAliasInSelectorList_CQL3TurnedOFF() {
    cqlExpression = "Column";
    expectedSelector = new Selector( "Column" );
    isCql3 = false;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );

    assertNotNull( selectors );
    assertEquals( 1, selectors.length );
    // check selector
    assertEquals( COLUMN_NAME_IS_INCORRECT, expectedSelector.getColumnName(), selectors[0].getColumnName() );
    assertNull( selectors[0].getAlias() );
    assertNull( selectors[0].getFunction() );
    assertFalse( selectors[0].isFunction() );
  }

  @Test
  public void testMixedColumnsAndFunctionsWithoutAliasInSelectorList_CQL3TurnedOn() {
    cqlExpression = "\"Column\", token(id,cust_id), id, cust_id, dateOf(time), User_name";
    expectedSelectors =
        new Selector[] { new Selector( "Column" ), new Selector( "token(id, cust_id)", null, "TOKEN" ),
          new Selector( "id" ), new Selector( "cust_id" ), new Selector( "dateOf(time)", null, "DATEOF" ),
          new Selector( "user_name" ) };
    isCql3 = true;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );

    verifySelectors( expectedSelectors, selectors );
  }

  @Test
  public void testMixedColumnsAndFunctionsWithoutAliasInSelectorList_CQL3TurnedOFF() {
    cqlExpression = "'Column', token(id,cust_id), id, cust_id, dateOf(time), User_name";
    expectedSelectors =
        new Selector[] { new Selector( "Column" ), new Selector( "token(id, cust_id)", null, "TOKEN" ),
          new Selector( "id" ), new Selector( "cust_id" ), new Selector( "dateOf(time)", null, "DATEOF" ),
          new Selector( "User_name" ) };
    isCql3 = false;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );

    verifySelectors( expectedSelectors, selectors );
  }

  @Test
  public void testMixedColumnsAndFunctionsWithAliasInSelectorList_CQL3TurnedOn() {
    cqlExpression =
        "\"Column\" as alias, token(id,cust_id) as \"Alias For Token\", id, cust_id as Customer, dateOf(time), User_name";
    expectedSelectors =
        new Selector[] { new Selector( "Column", "alias" ),
          new Selector( "token(id, cust_id)", "Alias For Token", "TOKEN" ), new Selector( "id" ),
          new Selector( "cust_id", "customer" ), new Selector( "dateOf(time)", null, "DATEOF" ),
          new Selector( "user_name" ) };
    isCql3 = true;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );

    verifySelectors( expectedSelectors, selectors );
  }

  @Test
  public void testSpecificNameForCountFunctionWithArgumentAsterisk() {
    cqlExpression = "count(*)";
    expectedSelectors = new Selector[] { new Selector( "COUNT", null, "COUNT" ) };
    isCql3 = true;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );

    verifySelectors( expectedSelectors, selectors );
  }

  @Test
  public void testSpecificNameForCountFunctionWithArgumentOne() {
    cqlExpression = "count(1)";
    expectedSelectors = new Selector[] { new Selector( "COUNT", null, "COUNT" ) };
    isCql3 = true;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );

    verifySelectors( expectedSelectors, selectors );
  }

  @Test
  public void testColumnsInSelect_IfSelectExpressionNull() {
    cqlExpression = null;
    isCql3 = true;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );
    assertNull( selectors );
  }

  @Test
  public void testColumnsInSelect_IfSelectExpressionEmpty() {
    cqlExpression = "";
    isCql3 = true;

    selectors = CQLUtils.getColumnsInSelect( cqlExpression, isCql3 );
    assertNull( selectors );
  }

  private void verifySelectors( Selector[] expected, Selector[] actual ) {
    assertNotNull( actual );
    assertEquals( expected.length, actual.length );
    // check selectors
    for ( int i = 0; i < expected.length; i++ ) {
      assertEquals( COLUMN_NAME_IS_INCORRECT, expected[i].getColumnName(), actual[i].getColumnName() );
      if ( expected[i].getAlias() != null ) {
        assertEquals( ALIAS_IS_INCORRECT, expected[i].getAlias(), actual[i].getAlias() );
      } else {
        assertNull( actual[i].getAlias() );
      }
      assertEquals( expected[i].isFunction(), actual[i].isFunction() );
      if ( expected[i].isFunction() ) {
        assertEquals( expected[i].getFunction(), actual[i].getFunction() );
      } else {
        assertNull( actual[i].getFunction() );
      }
    }
  }

}
