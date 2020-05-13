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

package org.apache.hop.pipeline.transforms.sasinput;

import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.eobjects.sassy.SasColumnType;
import org.eobjects.sassy.SasReader;
import org.eobjects.sassy.SasReaderCallback;

import java.io.File;
import java.util.ArrayList;

/**
 * Reads data from a SAS file in SAS7BAT format.
 *
 * @author Matt
 * @version 4.3
 * @since 9-OCT-2011
 */
public class SasInput extends BaseTransform implements ITransform {
  private static Class<?> PKG = SasInputMeta.class; // for i18n purposes, needed
  // by Translator!!

  private SasInputMeta meta;
  private SasInputData data;

  public SasInput( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                   Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {
    meta = (SasInputMeta) smi;
    data = (SasInputData) sdi;

    final Object[] fileRowData = getRow();
    if ( fileRowData == null ) {
      // No more work to do...
      //
      setOutputDone();
      return false;
    }

    // First we see if we need to get a list of files from input...
    //
    if ( first ) {

      // The output row meta data, what does it look like?
      //
      data.outputRowMeta = new RowMeta();

      // See if the input row contains the filename field...
      //
      int idx = getInputRowMeta().indexOfValue( meta.getAcceptingField() );
      if ( idx < 0 ) {
        throw new HopException( BaseMessages.getString(
          PKG, "SASInput.Log.Error.UnableToFindFilenameField", meta.getAcceptingField() ) );
      }

      // Determine the output row layout
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );
    }

    String rawFilename = getInputRowMeta().getString( fileRowData, meta.getAcceptingField(), null );
    final String filename = HopVFS.getFilename( HopVFS.getFileObject( rawFilename ) );

    data.helper = new SasInputHelper( filename );
    logBasic( BaseMessages.getString( PKG, "SASInput.Log.OpenedSASFile" ) + " : [" + data.helper + "]" );

    // verify the row layout...
    //
    if ( data.fileLayout == null ) {
      data.fileLayout = data.helper.getRowMeta();
    } else {
      // Verify that all files are of the same file format, this is a requirement...
      //
      if ( data.fileLayout.size() != data.helper.getRowMeta().size() ) {
        throw new HopException( "All input files need to have the same number of fields. File '"
          + filename + "' has " + data.helper.getRowMeta().size() + " fields while the first file only had "
          + data.fileLayout.size() );
      }
      for ( int i = 0; i < data.fileLayout.size(); i++ ) {
        IValueMeta first = data.fileLayout.getValueMeta( i );
        IValueMeta second = data.helper.getRowMeta().getValueMeta( i );
        if ( !first.getName().equalsIgnoreCase( second.getName() ) ) {
          throw new HopException( "Field nr "
            + i + " in file '" + filename + "' is called '" + second.getName() + "' while it was called '"
            + first.getName() + "' in the first file" );
        }
        if ( first.getType() != second.getType() ) {
          throw new HopException( "Field nr "
            + i + " in file '" + filename + "' is of data type '" + second.getTypeDesc() + "' while it was '"
            + first.getTypeDesc() + "' in the first file" );
        }
      }
    }

    // Also make sure that we only read the specified fields, not any other...
    //
    if ( first ) {
      first = false;

      data.fieldIndexes = new ArrayList<Integer>();
      for ( SasInputField field : meta.getOutputFields() ) {
        int fieldIndex = data.fileLayout.indexOfValue( field.getName() );
        if ( fieldIndex < 0 ) {
          throw new HopException( "Selected field '"
            + field.getName() + "' couldn't be found in file '" + filename + "'" );
        }
        data.fieldIndexes.add( fieldIndex );
      }
    }

    // Add this to the result file names...
    //
    ResultFile resultFile =
      new ResultFile( ResultFile.FILE_TYPE_GENERAL, HopVFS.getFileObject( filename ), getPipelineMeta()
        .getName(), getTransformName() );
    resultFile.setComment( BaseMessages.getString( PKG, "SASInput.ResultFile.Comment" ) );
    addResultFile( resultFile );

    SasReader sasReader = new SasReader( new File( filename ) );
    sasReader.read( new SasReaderCallback() {
      private boolean firstRead = true;

      @Override
      public void column( int index, String name, String label, SasColumnType type, int length ) {
      }

      @Override
      public boolean readData() {
        return true;
      }

      @Override
      public boolean row( int rowNumber, Object[] rowData ) {
        try {
          // Let's copy the data for safety
          //
          if ( firstRead ) {
            firstRead = false;
          } else {
            if ( rowNumber == 1 ) {
              return false;
            }
          }
          Object[] row = RowDataUtil.createResizedCopy( fileRowData, data.outputRowMeta.size() );

          // Only pick those fields that we're interested in.
          //
          int outputIndex = getInputRowMeta().size();
          for ( int i = 0; i < data.fieldIndexes.size(); i++ ) {
            int fieldIndex = data.fieldIndexes.get( i );
            int type = data.fileLayout.getValueMeta( fieldIndex ).getType();
            switch ( type ) {
              case IValueMeta.TYPE_STRING:
                row[ outputIndex++ ] = rowData[ fieldIndex ];
                break;
              case IValueMeta.TYPE_NUMBER:
                Double value = (Double) rowData[ fieldIndex ];
                if ( value.equals( Double.NaN ) ) {
                  value = null;
                }
                row[ outputIndex++ ] = value;
                break;
              default:
                throw new RuntimeException( "Unhandled data type '" + ValueMetaFactory.getValueMetaName( type ) );
            }
          }

          // Convert the data type of the new data to the requested data types
          //
          convertData( data.fileLayout, row, data.outputRowMeta );

          // Pass along the row to further transforms...
          //
          putRow( data.outputRowMeta, row );

          return !isStopped();
        } catch ( Exception e ) {
          throw new RuntimeException( "There was an error reading from SAS7BAT file '" + filename + "'", e );
        }
      }
    } );

    return true;
  }

  protected void convertData( IRowMeta source, Object[] sourceData, IRowMeta target ) throws HopException {
    int targetIndex = getInputRowMeta().size();
    for ( int i = 0; i < data.fieldIndexes.size(); i++ ) {
      int fieldIndex = data.fieldIndexes.get( i );
      IValueMeta sourceValueMeta = source.getValueMeta( fieldIndex );
      IValueMeta targetValueMeta = target.getValueMeta( targetIndex );
      sourceData[ targetIndex ] = targetValueMeta.convertData( sourceValueMeta, sourceData[ targetIndex ] );

      targetIndex++;
    }
  }

  @Override
  public void stopRunning( ITransform transformMetaInterface, ITransformData iTransformData ) throws HopException {
  }

}
