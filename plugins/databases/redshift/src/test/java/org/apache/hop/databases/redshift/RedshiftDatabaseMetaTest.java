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

package org.apache.hop.databases.redshift;

import org.apache.hop.core.database.DatabaseMeta;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for RedshiftDatabaseMeta
 */
public class RedshiftDatabaseMetaTest {

  private RedshiftDatabaseMeta dbMeta;

  @Before
  public void setUp() throws Exception {
    dbMeta = new RedshiftDatabaseMeta();
    dbMeta.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
  }

  @Test
  public void testExtraOption() {
    Map<String, String> opts = dbMeta.getExtraOptions();
    assertNotNull( opts );
    assertEquals( "true", opts.get( "REDSHIFT.tcpKeepAlive" ) );
  }

  @Test
  public void testGetDefaultDatabasePort() throws Exception {
    assertEquals( 5439, dbMeta.getDefaultDatabasePort() );
  }

  @Test
  public void testGetDriverClass() throws Exception {
    assertEquals( "com.amazon.redshift.jdbc4.Driver", dbMeta.getDriverClass() );
  }

  @Test
  public void testGetURL() throws Exception {
    assertEquals( "jdbc:redshift://:/", dbMeta.getURL( "", "", "" ) );
    assertEquals( "jdbc:redshift://rs.project-hop.org:4444/myDB",
      dbMeta.getURL( "rs.project-hop.org", "4444", "myDB" ) );
  }

  @Test
  public void testGetExtraOptionsHelpText() throws Exception {
    assertEquals( "http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html",
      dbMeta.getExtraOptionsHelpText() );
  }

  @Test
  public void testIsFetchSizeSupported() throws Exception {
    assertFalse( dbMeta.isFetchSizeSupported() );
  }

  @Test
  public void testSupportsSetMaxRows() throws Exception {
    assertFalse( dbMeta.supportsSetMaxRows() );
  }

}
