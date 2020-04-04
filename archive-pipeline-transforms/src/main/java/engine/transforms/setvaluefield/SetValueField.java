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

package org.apache.hop.pipeline.transforms.setvaluefield;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

/**
 * Set value field with another value field.
 *
 * @author Samatar
 * @since 10-11-2008
 */
public class SetValueField extends BaseTransform implements ITransform {
  private static Class<?> PKG = SetValueFieldMeta.class; // for i18n purposes, needed by Translator!!

  private SetValueFieldMeta meta;
  private SetValueFieldData data;

  public SetValueField( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {
    meta = (SetValueFieldMeta) smi;
    data = (SetValueFieldData) sdi;

    // Get one row from one of the rowsets...
    Object[] r = getRow();

    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      // What's the format of the output row?
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metaStore );

      data.indexOfField = new int[ meta.getFieldName().length ];
      data.indexOfReplaceByValue = new int[ meta.getFieldName().length ];
      for ( int i = 0; i < meta.getFieldName().length; i++ ) {
        // Check if this field was specified only one time
        for ( int j = 0; j < meta.getFieldName().length; j++ ) {
          if ( meta.getFieldName()[ j ].equals( meta.getFieldName()[ i ] ) ) {
            if ( j != i ) {
              throw new HopException( BaseMessages.getString(
                PKG, "SetValueField.Log.FieldSpecifiedMoreThatOne", meta.getFieldName()[ i ], "" + i, "" + j ) );
            }
          }
        }

        data.indexOfField[ i ] = data.outputRowMeta.indexOfValue( environmentSubstitute( meta.getFieldName()[ i ] ) );
        if ( data.indexOfField[ i ] < 0 ) {
          throw new HopTransformException( BaseMessages.getString(
            PKG, "SetValueField.Log.CouldNotFindFieldInRow", meta.getFieldName()[ i ] ) );
        }
        String sourceField = environmentSubstitute(
          meta.getReplaceByFieldValue() != null && meta.getReplaceByFieldValue().length > 0
            ? meta.getReplaceByFieldValue()[ i ] : null
        );
        if ( Utils.isEmpty( sourceField ) ) {
          throw new HopTransformException( BaseMessages.getString(
            PKG, "SetValueField.Log.ReplaceByValueFieldMissing", "" + i ) );
        }
        data.indexOfReplaceByValue[ i ] = data.outputRowMeta.indexOfValue( sourceField );
        if ( data.indexOfReplaceByValue[ i ] < 0 ) {
          throw new HopTransformException( BaseMessages.getString(
            PKG, "SetValueField.Log.CouldNotFindFieldInRow", sourceField ) );
        }
        // Compare fields type
        IValueMeta SourceValue = getInputRowMeta().getValueMeta( data.indexOfField[ i ] );
        IValueMeta ReplaceByValue = getInputRowMeta().getValueMeta( data.indexOfReplaceByValue[ i ] );

        if ( SourceValue.getType() != ReplaceByValue.getType() ) {
          String err =
            BaseMessages.getString( PKG, "SetValueField.Log.FieldsTypeDifferent", SourceValue.getName()
              + " (" + SourceValue.getTypeDesc() + ")", ReplaceByValue.getName()
              + " (" + ReplaceByValue.getTypeDesc() + ")" );
          throw new HopTransformException( err );
        }
      }
    }
    try {
      for ( int i = 0; i < data.indexOfField.length; i++ ) {
        r[ data.indexOfField[ i ] ] = r[ data.indexOfReplaceByValue[ i ] ];
      }
      putRow( data.outputRowMeta, r ); // copy row to output rowset(s);
    } catch ( HopException e ) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if ( getTransformMeta().isDoingErrorHandling() ) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError( BaseMessages.getString( PKG, "SetValueField.Log.ErrorInTransform", e.getMessage() ) );
        setErrors( 1 );
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if ( sendToErrorRow ) {
        // Simply add this row to the error row
        putError( data.outputRowMeta, r, 1, errorMessage, null, "SetValueField001" );
      }
    }
    return true;
  }

  public void.dispose() {
    meta = (SetValueFieldMeta) smi;
    data = (SetValueFieldData) sdi;

    super.dispose();
  }

  public boolean init() {
    meta = (SetValueFieldMeta) smi;
    data = (SetValueFieldData) sdi;

    if ( super.init() ) {
      // Add init code here.
      return true;
    }
    return false;
  }

}
