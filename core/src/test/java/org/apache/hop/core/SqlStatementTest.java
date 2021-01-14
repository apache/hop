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
package org.apache.hop.core;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SqlStatementTest {
  @Test
  public void testClass() throws HopException {
    final String name = "transformName";
    final DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    final String sql = "sql string";
    final String error = "error";

    SqlStatement statement = new SqlStatement( name, dbMeta, sql );
    assertSame( name, statement.getTransformName() );
    assertSame( dbMeta, statement.getDatabase() );
    assertTrue( statement.hasSql() );
    assertSame( sql, statement.getSql() );
    statement.setTransformName( null );
    assertNull( statement.getTransformName() );
    statement.setDatabase( null );
    assertNull( statement.getDatabase() );
    statement.setSql( null );
    assertNull( statement.getSql() );
    assertFalse( statement.hasSql() );
    assertFalse( statement.hasError() );
    statement.setError( error );
    assertTrue( statement.hasError() );
    assertSame( error, statement.getError() );
  }
}
