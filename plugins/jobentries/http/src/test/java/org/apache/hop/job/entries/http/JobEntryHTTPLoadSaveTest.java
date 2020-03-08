/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.job.entries.http;

import org.apache.hop.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class JobEntryHTTPLoadSaveTest extends JobEntryLoadSaveTestSupport<JobEntryHTTP> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<JobEntryHTTP> getJobEntryClass() {
    return JobEntryHTTP.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "url", "targetFilename", "fileAppended", "dateTimeAdded",
      "targetFilenameExtension", "uploadFilename", "runForEveryRow", "urlFieldname", "uploadFieldname",
      "destinationFieldname", "username", "password", "proxyHostname", "proxyPort", "nonProxyHosts",
      "addFilenameToResult", "headerName", "headerValue" } );
  }

  @Override
  protected Map<String, FieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, FieldLoadSaveValidator<?>> validators = new HashMap<String, FieldLoadSaveValidator<?>>();
    int entries = new Random().nextInt( 20 ) + 1;
    validators.put( "headerName", new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), entries ) );
    validators.put( "headerValue", new ArrayLoadSaveValidator<String>( new StringLoadSaveValidator(), entries ) );
    return validators;
  }

}
