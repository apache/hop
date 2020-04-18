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

package org.apache.hop.pipeline.transforms.mappingoutput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */

public class MappingOutputMeta extends BaseTransformMeta implements ITransformMeta<MappingOutput, MappingOutputData> {

  private static Class<?> PKG = MappingOutputMeta.class; // for i18n purposes, needed by Translator!!

  public MappingOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  public Object clone() {
    MappingOutputMeta retval = (MappingOutputMeta) super.clone();

    return retval;
  }

  public void allocate( int nrFields ) {
  }

  public void setDefault() {
    int nrFields = 0;

    allocate( nrFields );
  }

  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "MappingOutputMeta.CheckResult.NotReceivingFields" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingOutputMeta.CheckResult.TransformReceivingDatasOK", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MappingOutputMeta.CheckResult.TransformReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MappingOutputMeta.CheckResult.NoInputReceived" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform createTransform( TransformMeta transformMeta, MappingOutputData data, int cnr, PipelineMeta tr,
                                     Pipeline pipeline ) {
    return new MappingOutput( transformMeta, this, data, cnr, tr, pipeline );
  }

  public MappingOutputData getTransformData() {
    return new MappingOutputData();
  }
}
