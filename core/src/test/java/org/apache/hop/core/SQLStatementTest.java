/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.apache.hop.core;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SQLStatementTest {
  @Test
  public void testClass() throws HopException {
    final String name = "stepName";
    final DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    final String sql = "sql string";
    final String error = "error";

    SQLStatement statement = new SQLStatement( name, dbMeta, sql );
    assertSame( name, statement.getStepname() );
    assertSame( dbMeta, statement.getDatabase() );
    assertTrue( statement.hasSQL() );
    assertSame( sql, statement.getSQL() );
    statement.setStepname( null );
    assertNull( statement.getStepname() );
    statement.setDatabase( null );
    assertNull( statement.getDatabase() );
    statement.setSQL( null );
    assertNull( statement.getSQL() );
    assertFalse( statement.hasSQL() );
    assertFalse( statement.hasError() );
    statement.setError( error );
    assertTrue( statement.hasError() );
    assertSame( error, statement.getError() );
  }
}
