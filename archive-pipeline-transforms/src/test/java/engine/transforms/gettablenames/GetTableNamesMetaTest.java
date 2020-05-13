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

package org.apache.hop.pipeline.transforms.gettablenames;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GetTableNamesMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  @Test
  public void testLoadSave() throws HopException {
    List<String> attributes = Arrays.asList( "Database", "SchemaName", "TablenameFieldName", "ObjectTypeFieldName",
      "SystemObjectFieldName", "SQLCreationFieldName", "includeCatalog", "includeSchema", "includeTable",
      "includeView", "includeProcedure", "includeSynonym", "AddSchemaInOut", "DynamicSchema", "SchemaFieldName" );

    LoadSaveTester<GetTableNamesMeta> loadSaveTester =
      new LoadSaveTester<GetTableNamesMeta>( GetTableNamesMeta.class, attributes );

    loadSaveTester.testSerialization();
  }
}
