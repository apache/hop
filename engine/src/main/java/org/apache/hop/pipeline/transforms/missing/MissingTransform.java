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

package org.apache.hop.pipeline.transforms.missing;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.dummy.Dummy;

public class MissingTransform extends Dummy {
  private static Class<?> PKG = MissingTransform.class; // for i18n purposes, needed by Translator!!

  public MissingTransform( TransformMeta transformMeta, ITransformData iTransformData, int copyNr, PipelineMeta pipelineMeta,
                           Pipeline pipeline ) {
    super( transformMeta, iTransformData, copyNr, pipelineMeta, pipeline );
  }

  public boolean init( ITransformMeta smi, ITransformData sdi ) {
    if ( super.init( smi, sdi ) ) {
      logError( BaseMessages.getString( PKG, "MissingPipelineTransform.Log.CannotRunPipeline" ) );
    }
    return false;
  }

}
