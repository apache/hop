/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.metastore;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metastore.stores.xml.XmlUtil;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MetaStoreConstTest {

  @Test
  public void testOpenLocalHopMetaStore() throws Exception {
    IVariables variables = Variables.getADefaultVariableSpace();
    MetaStoreConst.disableMetaStore = false;
    File tempDir = Files.createTempDir();
    String tempPath = tempDir.getAbsolutePath();
    variables.setVariable( Const.HOP_METASTORE_FOLDER, tempPath );
    String metaFolder = tempPath + File.separator + XmlUtil.META_FOLDER_NAME;

    // Create a metastore
    assertNotNull( MetaStoreConst.openLocalHopMetaStore(variables) );
    assertTrue( ( new File( metaFolder ) ).exists() );

    // Check existing while disabling the metastore ( used for tests )
    MetaStoreConst.disableMetaStore = true;
    assertNull( MetaStoreConst.openLocalHopMetaStore(variables) );

    // Check existing metastore
    MetaStoreConst.disableMetaStore = false;
    assertNotNull( MetaStoreConst.openLocalHopMetaStore( variables, false ) );

    // Try to read a metastore that does not exist with allowCreate = false
    FileUtils.deleteDirectory( new File( metaFolder ) );
    assertNull( MetaStoreConst.openLocalHopMetaStore( variables, false ) );
    assertFalse( ( new File( metaFolder ) ).exists() );
  }

}
