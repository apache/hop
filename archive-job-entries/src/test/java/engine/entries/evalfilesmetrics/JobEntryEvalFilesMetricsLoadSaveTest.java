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

package org.apache.hop.job.entries.evalfilesmetrics;

import org.apache.hop.job.entries.simpleeval.JobEntrySimpleEval;
import org.apache.hop.job.entry.loadSave.JobEntryLoadSaveTestSupport;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.trans.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.IntLoadSaveValidator;
import org.apache.hop.trans.steps.loadsave.validator.StringLoadSaveValidator;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class JobEntryEvalFilesMetricsLoadSaveTest extends JobEntryLoadSaveTestSupport<JobEntryEvalFilesMetrics> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Override
  protected Class<JobEntryEvalFilesMetrics> getJobEntryClass() {
    return JobEntryEvalFilesMetrics.class;
  }

  @Override
  protected List<String> listCommonAttributes() {
    return Arrays.asList( new String[] { "resultFilenamesWildcard", "resultFieldFile", "resultFieldWildcard",
      "resultFieldIncludeSubfolders", "sourceFileFolder", "sourceWildcard", "sourceIncludeSubfolders",
      "compareValue", "minValue", "maxValue", "successConditionType", "sourceFiles", "evaluationType", "scale" } );
  }

  @Override
  protected Map<String, FieldLoadSaveValidator<?>> createAttributeValidatorsMap() {
    Map<String, FieldLoadSaveValidator<?>> validators = new HashMap<String, FieldLoadSaveValidator<?>>();
    int sourceFileCount = new Random().nextInt( 50 ) + 1;
    validators.put( "sourceFileFolder", new ArrayLoadSaveValidator<String>(
      new StringLoadSaveValidator(), sourceFileCount ) );
    validators.put( "sourceWildcard", new ArrayLoadSaveValidator<String>(
      new StringLoadSaveValidator(), sourceFileCount ) );
    validators.put( "sourceIncludeSubfolders", new ArrayLoadSaveValidator<String>(
      new StringLoadSaveValidator(), sourceFileCount ) );
    validators.put( "successConditionType",
      new IntLoadSaveValidator( JobEntrySimpleEval.successNumberConditionCode.length ) );
    validators.put( "sourceFiles",
      new IntLoadSaveValidator( JobEntryEvalFilesMetrics.SourceFilesCodes.length ) );
    validators.put( "evaluationType",
      new IntLoadSaveValidator( JobEntryEvalFilesMetrics.EvaluationTypeCodes.length ) );
    validators.put( "scale",
      new IntLoadSaveValidator( JobEntryEvalFilesMetrics.scaleCodes.length ) );

    return validators;
  }
}
