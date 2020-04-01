/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.steps.missing;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.steps.dummy.Dummy;

public class MissingStep extends Dummy {
  private static Class<?> PKG = MissingStep.class; // for i18n purposes, needed by Translator!!

  public MissingStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( stepMeta, stepDataInterface, copyNr, pipelineMeta, pipeline );
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    if ( super.init( smi, sdi ) ) {
      logError( BaseMessages.getString( PKG, "MissingPipelineStep.Log.CannotRunPipeline" ) );
    }
    return false;
  }

}
