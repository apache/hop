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

package org.apache.hop.job.entries.eval;

import org.apache.hop.core.Const;
import org.apache.hop.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.transforms.loadsave.validator.IFieldLoadSaveValidator;
import org.apache.hop.pipeline.transforms.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class JobEntryEvalLoadSaveTest extends JobEntryLoadSaveTestSupport<JobEntryEval> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<JobEntryEval> getJobEntryClass() {
    return JobEntryEval.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "script" } );
  }

  @Override
  protected Map<String, IFieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, IFieldLoadSaveValidator<?>> validators = new HashMap<String, IFieldLoadSaveValidator<?>>();
    validators.put( "script", new MultiLineStringFieldLoadSaveValidator() );
    return validators;
  }

  public static class MultiLineStringFieldLoadSaveValidator extends StringLoadSaveValidator {

    @Override
    public String getTestObject() {
      String lineTerminator = Const.isWindows() ? "\n" : Const.CR;
      StringBuilder text = new StringBuilder();
      int lines = new Random().nextInt( 10 ) + 1;
      for ( int i = 0; i < lines; i++ ) {
        text.append( super.getTestObject() );
        if ( i + 1 < lines ) {
          text.append( lineTerminator );
        }
      }
      return text.toString();
    }

  }
}
