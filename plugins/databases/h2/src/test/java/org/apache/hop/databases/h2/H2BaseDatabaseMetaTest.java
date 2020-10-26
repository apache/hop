/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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
package org.apache.hop.databases.h2;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.SqlScriptStatement;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class H2BaseDatabaseMetaTest {
  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();
  BaseDatabaseMeta nativeMeta;

  @Before
  public void setupOnce() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  public void testShowIsTreatedAsAResultsQuery() throws Exception {
    List<SqlScriptStatement> sqlScriptStatements =
      new H2DatabaseMeta().getSqlScriptStatements( "show annotations from service" );
    assertTrue( sqlScriptStatements.get( 0 ).isQuery() );
  }

}
