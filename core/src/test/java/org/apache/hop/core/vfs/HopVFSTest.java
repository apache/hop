/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core.vfs;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


import org.junit.Test;
import org.apache.hop.core.exception.HopFileException;


import java.io.IOException;


public class HopVFSTest {

  /**
   * Test to validate that startsWitScheme() returns true if the fileName starts with
   * known protocol like zip: jar: then it returns true else returns false
   * @param fileName
   */
  @Test
  public void testStartsWithScheme() {
    String fileName = "zip:file:///SavedLinkedres.zip!Calculate median and percentiles using the group by steps.ktr";
    assertTrue( HopVFS.startsWithScheme( fileName ) );

    fileName = "SavedLinkedres.zip!Calculate median and percentiles using the group by steps.ktr";
    assertFalse( HopVFS.startsWithScheme( fileName ) );
  }


  @Test
  public void testCheckForSchemeSuccess() throws HopFileException, IOException {
    String[] schemes = {"hdfs"};
    String vfsFilename = "hdfs://hsbcmaster.labs.eag.hitachivantara.com:8020/tmp/acltest/";

    boolean test = HopVFS.checkForScheme(schemes, true, vfsFilename, null, null);
    assertFalse( test );

  }

  @Test
  public void testCheckForSchemeFail() throws HopFileException, IOException {
    String[] schemes = {"file"};
    String vfsFilename = "hdfs://hsbcmaster.labs.eag.hitachivantara.com:8020/tmp/acltest/";

    boolean test = HopVFS.checkForScheme(schemes, true, vfsFilename, null, null);
    assertTrue( test );

  }

}
