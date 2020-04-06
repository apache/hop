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
package org.apache.hop.pipeline.transforms.mail;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.transforms.loadsave.initializer.InitializerInterface;
import org.apache.hop.pipeline.transforms.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MailMetaTest implements InitializerInterface<ITransform> {
  LoadSaveTester loadSaveTester;
  Class<MailMeta> testMetaClass = MailMeta.class;
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init( false );
    List<String> attributes =
      Arrays.asList( "server", "destination", "destinationCc", "destinationBCc", "replyAddress", "replyName", "subject", "includeDate",
        "includeSubFolders", "zipFilenameDynamic", "isFilenameDynamic", "dynamicFieldname", "dynamicWildcard", "dynamicZipFilenameField",
        "sourceFileFoldername", "sourceWildcard", "contactPerson", "contactPhone", "comment", "includingFiles", "zipFiles", "zipFilename",
        "zipLimitSize", "usingAuthentication", "authenticationUser", "authenticationPassword", "onlySendComment", "useHTML",
        "usingSecureAuthentication", "usePriority", "port", "priority", "importance", "sensitivity", "secureConnectionType", "encoding",
        "replyToAddresses", "attachContentFromField", "attachContentField", "attachContentFileNameField", "embeddedImages", "contentIds" );

    Map<String, String> getterMap = new HashMap<String, String>() {
      {
        put( "isFilenameDynamic", "isDynamicFilename" );
      }
    };
    Map<String, String> setterMap = new HashMap<String, String>() {
      {
        put( "isFilenameDynamic", "setisDynamicFilename" );
      }
    };
    FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
      new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), 5 );

    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    attrValidatorMap.put( "embeddedImages", stringArrayLoadSaveValidator );
    attrValidatorMap.put( "contentIds", stringArrayLoadSaveValidator );

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    loadSaveTester =
      new LoadSaveTester( testMetaClass, attributes, new ArrayList<>(),
        getterMap, setterMap, attrValidatorMap, typeValidatorMap, this );
  }

  // Call the allocate method on the LoadSaveTester meta class
  @Override
  public void modify( ITransform someMeta ) {
    if ( someMeta instanceof MailMeta ) {
      ( (MailMeta) someMeta ).allocate( 5 );
    }
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }
}
