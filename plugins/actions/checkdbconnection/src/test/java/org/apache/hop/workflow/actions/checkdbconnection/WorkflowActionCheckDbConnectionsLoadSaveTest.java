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

package org.apache.hop.workflow.actions.checkdbconnection;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.workflow.action.loadsave.WorkflowActionLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.DatabaseMetaLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.PrimitiveIntArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class WorkflowActionCheckDbConnectionsLoadSaveTest extends WorkflowActionLoadSaveTestSupport<ActionCheckDbConnections> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<ActionCheckDbConnections> getActionClass() {
    return ActionCheckDbConnections.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "connections", "waitfors", "waittimes" } );
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<String, IFieldLoadSaveValidator<?>>();
    int entries = new Random().nextInt( 10 ) + 1;
    validators.put( "connections", new ArrayLoadSaveValidator<DatabaseMeta>(
      new DatabaseMetaLoadSaveValidator(), entries ) );
    validators.put( "waitfors", new ArrayLoadSaveValidator<String>(
      new StringLoadSaveValidator(), entries ) );
    validators.put( "waittimes", new PrimitiveIntArrayLoadSaveValidator(
      new IntLoadSaveValidator( ActionCheckDbConnections.unitTimeCode.length ), entries ) );
    return validators;
  }
}
