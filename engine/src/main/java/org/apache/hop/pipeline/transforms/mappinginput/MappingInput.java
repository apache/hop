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

package org.apache.hop.pipeline.transforms.mappinginput;

import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;

import java.util.List;

/**
 * Do nothing. Pass all input data to the next transforms.
 *
 * @author Matt
 * @since 2-jun-2003
 */
public class MappingInput
  extends BaseTransform<MappingInputMeta, MappingInputData>
  implements ITransform<MappingInputMeta, MappingInputData> {

  private static Class<?> PKG = MappingInputMeta.class; // for i18n purposes, needed by Translator!!
  private int timeOut = 60000;

  public MappingInput( TransformMeta transformMeta, MappingInputMeta meta, MappingInputData data, int copyNr, PipelineMeta pipelineMeta,
                       Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public void setTimeOut( int timeOut ) {
    this.timeOut = timeOut;
  }

  // ProcessRow is not doing anything
  // It's a place holder for accepting rows from the parent pipeline...
  // So, basically, this is a glorified Dummy with a little bit of meta-data
  //
  @Override
  public boolean processRow() throws HopException {

    if ( !data.linked ) {
      //
      // Wait until we know were to read from the parent pipeline...
      // However, don't wait forever, if we don't have a connection after 60 seconds: bail out!
      //
      int totalsleep = 0;
      while ( !isStopped() && data.sourceTransforms == null ) {
        try {
          totalsleep += 10;
          Thread.sleep( 10 );
        } catch ( InterruptedException e ) {
          stopAll();
        }
        if ( totalsleep > timeOut ) {
          throw new HopException( BaseMessages.getString( PKG,
            "MappingInput.Exception.UnableToConnectWithParentMapping", "" + ( totalsleep / 1000 ) ) );
        }
      }

      // OK, now we're ready to read from the parent source transforms.
      data.linked = true;
    }

    Object[] row = getRow();
    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      // The Input RowMetadata is not the same as the output row meta-data.
      // The difference is described in the data interface
      //
      // String[] data.sourceFieldname
      // String[] data.targetFieldname
      //
      // --> getInputRowMeta() is not corresponding to what we're outputting.
      // In essence, we need to rename a couple of fields...
      //
      data.outputRowMeta = getInputRowMeta().clone();

      // Now change the field names according to the mapping specification...
      // That means that all fields go through unchanged, unless specified.
      //
      for ( MappingValueRename valueRename : data.valueRenames ) {
        IValueMeta valueMeta = data.outputRowMeta.searchValueMeta( valueRename.getSourceValueName() );
        if ( valueMeta == null ) {
          throw new HopTransformException( BaseMessages.getString( PKG, "MappingInput.Exception.UnableToFindMappedValue",
            valueRename.getSourceValueName() ) );
        }
        valueMeta.setName( valueRename.getTargetValueName() );

        valueMeta = getInputRowMeta().searchValueMeta( valueRename.getSourceValueName() );
        if ( valueMeta == null ) {
          throw new HopTransformException( BaseMessages.getString( PKG, "MappingInput.Exception.UnableToFindMappedValue",
            valueRename.getSourceValueName() ) );
        }
        valueMeta.setName( valueRename.getTargetValueName() );
      }

      // This is typical side effect of ESR-4178
      data.outputRowMeta.setValueMetaList( data.outputRowMeta.getValueMetaList() );
      this.getInputRowMeta().setValueMetaList( this.getInputRowMeta().getValueMetaList() );

      // The input row meta has been manipulated correctly for the call to meta.getFields(), so create a blank
      // outputRowMeta
      meta.setInputRowMeta( getInputRowMeta() );
      if ( meta.isSelectingAndSortingUnspecifiedFields() ) {
        data.outputRowMeta = new RowMeta();
      } else {
        meta.setInputRowMeta( new RowMeta() );
      }

      // Fill the output row meta with the processed fields
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );

      if ( meta.isSelectingAndSortingUnspecifiedFields() ) {
        //
        // Create a list of the indexes to get the right order or fields on the output.
        //
        data.fieldNrs = new int[ data.outputRowMeta.size() ];
        for ( int i = 0; i < data.outputRowMeta.size(); i++ ) {
          data.fieldNrs[ i ] = getInputRowMeta().indexOfValue( data.outputRowMeta.getValueMeta( i ).getName() );
        }
      }
    }

    // Fill and send the output row
    if ( meta.isSelectingAndSortingUnspecifiedFields() ) {
      Object[] outputRowData = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
      for ( int i = 0; i < data.fieldNrs.length; i++ ) {
        outputRowData[ i ] = row[ data.fieldNrs[ i ] ];
      }
      putRow( data.outputRowMeta, outputRowData );
    } else {
      putRow( data.outputRowMeta, row );
    }

    return true;
  }

  public boolean init() {
    return super.init( );
  }

  public void setConnectorTransforms( ITransform[] sourceTransforms, List<MappingValueRename> valueRenames,
                                      String mappingTransformName ) {

    if ( sourceTransforms == null ) {
      throw new IllegalArgumentException( BaseMessages
        .getString( PKG, "MappingInput.Exception.IllegalArgumentSourceTransform" ) );
    }

    if ( valueRenames == null ) {
      throw new IllegalArgumentException( BaseMessages
        .getString( PKG, "MappingInput.Exception.IllegalArgumentValueRename" ) );
    }

    if ( sourceTransforms.length != 0 ) {
      if ( mappingTransformName == null ) {
        throw new IllegalArgumentException( BaseMessages
          .getString( PKG, "MappingInput.Exception.IllegalArgumentTransformName" ) );
      }
    }

    for ( ITransform sourceTransform : sourceTransforms ) {

      // We don't want to add the mapping-to-mapping rowset
      //
      if ( !sourceTransform.isMapping() ) {
        // OK, before we leave, make sure there is a rowset that covers the path to this target transform.
        // We need to create a new IRowSet and add it to the Input RowSets of the target transform
        //
        BlockingRowSet rowSet = new BlockingRowSet( getPipeline().getRowSetSize() );

        // This is always a single copy, both for source and target...
        //
        rowSet.setThreadNameFromToCopy( sourceTransform.getTransformName(), 0, mappingTransformName, 0 );

        // Make sure to connect it to both sides...
        //
        sourceTransform.addRowSetToOutputRowSets( rowSet );
        sourceTransform.identifyErrorOutput();
        addRowSetToInputRowSets( rowSet );
      }
    }
    data.valueRenames = valueRenames;

    data.sourceTransforms = sourceTransforms;
  }
}
