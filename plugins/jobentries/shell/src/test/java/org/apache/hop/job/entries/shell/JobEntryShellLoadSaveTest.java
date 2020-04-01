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

package org.apache.hop.job.entries.shell;

import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.steps.loadsave.validator.EnumLoadSaveValidator;
import org.apache.hop.pipeline.steps.loadsave.validator.FieldLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobEntryShellLoadSaveTest extends JobEntryLoadSaveTestSupport<JobEntryShell> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<JobEntryShell> getJobEntryClass() {
    return JobEntryShell.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "filename",
      "workDirectory",
      "argFromPrevious",
      "execPerRow",
      "setLogfile",
      "logfile",
      "setAppendLogfile",
      "logext",
      "addDate",
      "addTime",
      "insertScript",
      "script",
      "logFileLevel",
      "arguments" } );
  }

  @Override
  protected Map<String, FieldLoadSaveValidator<?>> createTypeValidatorsMap() {
    Map<String, FieldLoadSaveValidator<?>> validators = new HashMap<String, FieldLoadSaveValidator<?>>();
    validators.put( LogLevel.class.getName(), new EnumLoadSaveValidator<LogLevel>( LogLevel.class ) );
    return validators;
  }

}
