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

package org.apache.hop.pipeline.steps.mappingoutput;

import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStep;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.steps.mapping.MappingValueRename;

import java.util.List;

/**
 * Do nothing. Pass all input data to the next steps.
 *
 * @author Matt
 * @since 2-jun-2003
 */
public class MappingOutput extends BaseStep implements StepInterface {
  private static Class<?> PKG = MappingOutputMeta.class; // for i18n purposes, needed by Translator!!

  private MappingOutputMeta meta;
  private MappingOutputData data;

  public MappingOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( stepMeta, stepDataInterface, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {
    meta = (MappingOutputMeta) smi;
    data = (MappingOutputData) sdi;

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) {
      // No more input to be expected... Tell the next steps.
      //
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.setOutputValueRenames( data.outputValueRenames );
      meta.setInputValueRenames( data.inputValueRenames );
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, metaStore );

      //
      // Wait until the parent pipeline has started completely.
      // However, don't wait forever, if we don't have a connection after 60 seconds: bail out!
      //
      int totalsleep = 0;
      if ( getPipeline().getParentPipeline() != null ) {
        while ( !isStopped() && !getPipeline().getParentPipeline().isRunning() ) {
          try {
            totalsleep += 10;
            Thread.sleep( 10 );
          } catch ( InterruptedException e ) {
            stopAll();
          }
          if ( totalsleep > 60000 ) {
            throw new HopException( BaseMessages.getString(
              PKG, "MappingOutput.Exception.UnableToConnectWithParentMapping", "" + ( totalsleep / 1000 ) ) );
          }
        }
      }
      // Now see if there is a target step to send data to.
      // If not, simply eat the data...
      //
      if ( data.targetSteps == null ) {
        logDetailed( BaseMessages.getString( PKG, "MappingOutput.NoTargetStepSpecified", getStepname() ) );
      }
    }

    // Copy row to possible alternate rowset(s).
    // Rowsets where added for all the possible targets in the setter for data.targetSteps...
    //
    putRow( data.outputRowMeta, r );

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "MappingOutput.Log.LineNumber" ) + getLinesRead() );
    }

    return true;
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (MappingOutputMeta) smi;
    data = (MappingOutputData) sdi;

    if ( super.init( smi, sdi ) ) {
      // Add init code here.
      return true;
    }
    return false;
  }

  public void setConnectorSteps( StepInterface[] targetSteps, List<MappingValueRename> inputValueRenames,
                                 List<MappingValueRename> outputValueRenames ) {
    for ( int i = 0; i < targetSteps.length; i++ ) {

      // OK, before we leave, make sure there is a rowset that covers the path to this target step.
      // We need to create a new RowSet and add it to the Input RowSets of the target step
      //
      BlockingRowSet rowSet = new BlockingRowSet( getPipeline().getRowSetSize() );

      // This is always a single copy, but for source and target...
      //
      rowSet.setThreadNameFromToCopy( getStepname(), 0, targetSteps[ i ].getStepname(), 0 );

      // Make sure to connect it to both sides...
      //
      addRowSetToOutputRowSets( rowSet );

      // Add the row set to the target step as input.
      // This will appropriately drain the buffer as data comes in.
      // However, as an exception, we can't attach it to another mapping step.
      // We need to attach it to the appropriate mapping input step.
      // The main problem is that we can't do it here since we don't know that the other step has initialized properly
      // yet.
      // This method is called during init() and we can't tell for sure it's done already.
      // As such, we'll simply grab the remaining row sets at the Mapping#processRow() level and assign them to a
      // Mapping Input step.
      //
      targetSteps[ i ].addRowSetToInputRowSets( rowSet );
    }

    data.inputValueRenames = inputValueRenames;
    data.outputValueRenames = outputValueRenames;
    data.targetSteps = targetSteps;
  }

}
