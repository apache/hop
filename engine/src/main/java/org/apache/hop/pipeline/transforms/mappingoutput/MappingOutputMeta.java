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

package org.apache.hop.pipeline.transforms.mappingoutput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;

import java.util.ArrayList;
import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */

public class MappingOutputMeta extends BaseTransformMeta implements ITransformMeta<MappingOutput, MappingOutputData> {

  private static Class<?> PKG = MappingOutputMeta.class; // for i18n purposes, needed by Translator!!

  private volatile List<MappingValueRename> inputValueRenames;
  private volatile List<MappingValueRename> outputValueRenames;

  public MappingOutputMeta() {
    super(); // allocate BaseTransformMeta
    inputValueRenames = new ArrayList<MappingValueRename>();
    inputValueRenames = new ArrayList<MappingValueRename>();
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

  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // It's best that this method doesn't change anything by itself.
    // Eventually it's the Mapping transform that's going to tell this transform how to behave meta-data wise.
    // It is the mapping transform that tells the mapping output transform what fields to rename.
    //
    if ( inputValueRenames != null ) {
      for ( MappingValueRename valueRename : inputValueRenames ) {
        IValueMeta valueMeta = r.searchValueMeta( valueRename.getTargetValueName() );
        if ( valueMeta != null ) {
          valueMeta.setName( valueRename.getSourceValueName() );
        }
      }
    }

    // This is the optionally entered stuff in the output tab of the mapping dialog.
    //
    if ( outputValueRenames != null ) {
      for ( MappingValueRename valueRename : outputValueRenames ) {
        int valueMetaRenameIndex = r.indexOfValue( valueRename.getSourceValueName() );
        if ( valueMetaRenameIndex >= 0 ) {
          IValueMeta valueMetaRename = r.getValueMeta( valueMetaRenameIndex ).clone();
          valueMetaRename.setName( valueRename.getTargetValueName() );
          // must maintain the same columns order. Noticed when implementing the Mapping transform in AEL (BACKLOG-23372)
          r.removeValueMeta( valueMetaRenameIndex );
          r.addValueMeta( valueMetaRenameIndex, valueMetaRename );
        }
      }
    }
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

  /**
   * @return the inputValueRenames
   */
  public List<MappingValueRename> getInputValueRenames() {
    return inputValueRenames;
  }

  /**
   * @param inputValueRenames the inputValueRenames to set
   */
  public void setInputValueRenames( List<MappingValueRename> inputValueRenames ) {
    this.inputValueRenames = inputValueRenames;
  }

  /**
   * @return the outputValueRenames
   */
  public List<MappingValueRename> getOutputValueRenames() {
    return outputValueRenames;
  }

  /**
   * @param outputValueRenames the outputValueRenames to set
   */
  public void setOutputValueRenames( List<MappingValueRename> outputValueRenames ) {
    this.outputValueRenames = outputValueRenames;
  }
}
