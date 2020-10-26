/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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
package org.apache.hop.databases.infobright;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InfobrightDatabaseMetaTest {

  @Test
  public void mysqlTestOverrides() throws Exception {
    InfobrightDatabaseMeta idm = new InfobrightDatabaseMeta();
    idm.setAccessType( DatabaseMeta.TYPE_ACCESS_NATIVE );
    assertEquals( 5029, idm.getDefaultDatabasePort() );
  }

  @Ignore
  @Test
  public void testAddOptionsInfobright() {
    DatabaseMeta databaseMeta = new DatabaseMeta( "", "Infobright", "JDBC", null, "stub:stub", null, null, null );
    Map<String, String> options = databaseMeta.getExtraOptions();
    if ( !options.keySet().contains( "INFOBRIGHT.characterEncoding" ) ) {
      fail();
    }
  }

  @Ignore
  @Test
  public void testAttributesVariable() throws HopDatabaseException {
    DatabaseMeta dbmeta = new DatabaseMeta( "", "Infobright", "JDBC", null, "stub:stub", null, null, null );
    dbmeta.setVariable( "someVar", "someValue" );
    dbmeta.setAttributes( new HashMap<>() );
    Map<String,String> props = dbmeta.getAttributes();
    props.put( "EXTRA_OPTION_Infobright.additional_param", "${someVar}" );
    dbmeta.getURL();
    assertTrue( dbmeta.getURL().contains( "someValue" ) );
  }

  @Ignore
  @Test
  public void testfindDatabase() throws HopDatabaseException {
    List<DatabaseMeta> databases = new ArrayList<DatabaseMeta>();
    databases.add( new DatabaseMeta( "  1", "Infobright", "JDBC", null, "stub:stub", null, null, null ) );
    databases.add( new DatabaseMeta( "  1  ", "Infobright", "JDBC", null, "stub:stub", null, null, null ) );
    databases.add( new DatabaseMeta( "1  ", "Infobright", "JDBC", null, "stub:stub", null, null, null ) );
    Assert.assertNotNull( DatabaseMeta.findDatabase( databases, "1" ) );
    Assert.assertNotNull( DatabaseMeta.findDatabase( databases, "1 " ) );
    Assert.assertNotNull( DatabaseMeta.findDatabase( databases, " 1" ) );
    Assert.assertNotNull( DatabaseMeta.findDatabase( databases, " 1 " ) );
  }
}
