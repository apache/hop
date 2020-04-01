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

package org.apache.hop.pipeline.steps.jobexecutor;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.steps.loadsave.LoadSaveTester;
import org.apache.hop.pipeline.steps.loadsave.validator.FieldLoadSaveValidator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNull;

/**
 * <p>
 * PDI-11979 - Fieldnames in the "Execution results" tab of the Job executor step saved incorrectly in repository.
 * </p>
 */
public class JobExecutorMetaTest {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  LoadSaveTester loadSaveTester;

  /**
   * Check all simple string fields.
   *
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {

    List<String> attributes =
      Arrays.asList( "fileName", "groupSize", "groupField", "groupTime",
        "executionTimeField", "executionFilesRetrievedField", "executionLogTextField",
        "executionLogChannelIdField", "executionResultField", "executionNrErrorsField", "executionLinesReadField",
        "executionLinesWrittenField", "executionLinesInputField", "executionLinesOutputField",
        "executionLinesRejectedField", "executionLinesUpdatedField", "executionLinesDeletedField",
        "executionExitStatusField" );

    // executionResultTargetStepMeta -? (see for switch case meta)
    Map<String, String> getterMap = new HashMap<>();
    Map<String, String> setterMap = new HashMap<>();
    Map<String, FieldLoadSaveValidator<?>> attrValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();

    Map<String, FieldLoadSaveValidator<?>> typeValidatorMap = new HashMap<String, FieldLoadSaveValidator<?>>();
    loadSaveTester =
      new LoadSaveTester( JobExecutorMeta.class, attributes, getterMap, setterMap, attrValidatorMap, typeValidatorMap );
  }

  @Test
  public void testSerialization() throws HopException {
    loadSaveTester.testSerialization();
  }

  @Test
  public void testRemoveHopFrom() throws Exception {
    JobExecutorMeta jobExecutorMeta = new JobExecutorMeta();
    jobExecutorMeta.setExecutionResultTargetStepMeta( new StepMeta() );
    jobExecutorMeta.setResultRowsTargetStepMeta( new StepMeta() );
    jobExecutorMeta.setResultFilesTargetStepMeta( new StepMeta() );

    jobExecutorMeta.cleanAfterHopFromRemove();

    assertNull( jobExecutorMeta.getExecutionResultTargetStepMeta() );
    assertNull( jobExecutorMeta.getResultRowsTargetStepMeta() );
    assertNull( jobExecutorMeta.getResultFilesTargetStepMeta() );
  }
}
