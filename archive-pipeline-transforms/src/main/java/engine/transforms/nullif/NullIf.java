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

package org.apache.hop.pipeline.transforms.nullif;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

/**
 * NullIf transform, put null as value when the original field matches a specific value.
 *
 * @author Matt
 * @since 4-aug-2003
 */
public class NullIf extends BaseTransform implements ITransform {
  private static Class<?> PKG = NullIfMeta.class; // for i18n purposes, needed by Translator!!

  private NullIfMeta meta;
  private NullIfData data;

  public NullIf( TransformMeta transformMeta, ITransformData iTransformData, int copyNr, PipelineMeta pipelineMeta,
                 Pipeline pipeline ) {
    super( transformMeta, iTransformData, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow( TransformMetaInterface smi, ITransformData sdi ) throws HopException {
    meta = (NullIfMeta) smi;
    data = (NullIfData) sdi;

    // Get one row from one of the rowsets...
    Object[] r = getRow();

    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      int fieldsLength = meta.getFields().length;
      data.keynr = new int[ fieldsLength ];
      data.nullValue = new Object[ fieldsLength ];
      data.nullValueMeta = new IValueMeta[ fieldsLength ];
      for ( int i = 0; i < fieldsLength; i++ ) {
        data.keynr[ i ] = data.outputRowMeta.indexOfValue( meta.getFields()[ i ].getFieldName() );
        if ( data.keynr[ i ] < 0 ) {
          logError( BaseMessages.getString( PKG, "NullIf.Log.CouldNotFindFieldInRow", meta.getFields()[ i ].getFieldName() ) );
          setErrors( 1 );
          stopAll();
          return false;
        }
        data.nullValueMeta[ i ] = data.outputRowMeta.getValueMeta( data.keynr[ i ] );
        // convert from input string entered by the user
        ValueMetaString vms = new ValueMetaString();
        vms.setConversionMask( data.nullValueMeta[ i ].getConversionMask() );
        data.nullValue[ i ] =
          data.nullValueMeta[ i ].convertData( vms, meta.getFields()[ i ].getFieldValue() );
      }
    }

    if ( log.isRowLevel() ) {
      logRowlevel( BaseMessages.getString( PKG, "NullIf.Log.ConvertFieldValuesToNullForRow" )
        + data.outputRowMeta.getString( r ) );
    }

    for ( int i = 0; i < meta.getFields().length; i++ ) {
      Object field = r[ data.keynr[ i ] ];
      if ( field != null && data.nullValueMeta[ i ].compare( field, data.nullValue[ i ] ) == 0 ) {
        // OK, this value needs to be set to NULL
        r[ data.keynr[ i ] ] = null;
      }
    }

    putRow( data.outputRowMeta, r ); // Just one row!

    return true;
  }

  @Override
  public void dispose( TransformMetaInterface smi, ITransformData sdi ) {
    meta = (NullIfMeta) smi;
    data = (NullIfData) sdi;

    super.dispose( smi, sdi );
  }

  @Override
  public boolean init( TransformMetaInterface smi, ITransformData sdi ) {
    meta = (NullIfMeta) smi;
    data = (NullIfData) sdi;

    if ( super.init( smi, sdi ) ) {
      // Add init code here.
      return true;
    }
    return false;
  }

}
