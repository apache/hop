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

package org.apache.hop.core.database;

import static org.junit.Assert.*;

import org.junit.Test;

public class ReleaseSavePointTest {

  DatabaseInterface[] noSupport = new DatabaseInterface[] {
    new OracleDatabaseMeta(),
    new MySQLDatabaseMeta()
  };

  DatabaseInterface[] support = new DatabaseInterface[] {
    new GenericDatabaseMeta()
  };

  @Test
  public void testReleaseSavePointBooleans() {
    try {
      for ( DatabaseInterface db : support ) {
        assertTrue( db.releaseSavepoint() );
      }

      for ( DatabaseInterface db : noSupport ) {
        assertFalse( db.releaseSavepoint() );
      }
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }
}
