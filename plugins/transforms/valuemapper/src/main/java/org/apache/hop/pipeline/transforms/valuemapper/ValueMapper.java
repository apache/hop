/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.valuemapper;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;

import javax.xml.crypto.Data;
import java.util.Hashtable;

/**
 * Convert Values in a certain fields to other values
 *
 * @author Matt
 * @since 3-apr-2006
 */
public class ValueMapper extends BaseTransform<ValueMapperMeta, ValueMapperData> implements ITransform<ValueMapperMeta, ValueMapperData> {
  private static final Class<?> PKG = ValueMapperMeta.class; // For Translator

  private boolean nonMatchActivated = false;

  public ValueMapper(TransformMeta transformMeta, ValueMapperMeta meta, ValueMapperData data, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow() throws HopException {

    // Get one row from one of the rowsets...
    //
    Object[] r = getRow();
    if ( r == null ) { // means: no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.previousMeta = getInputRowMeta().clone();
      data.outputMeta = data.previousMeta.clone();
      meta.getFields( data.outputMeta, getTransformName(), null, null, this, metadataProvider );

      data.keynr = data.previousMeta.indexOfValue( meta.getFieldToUse() );
      if ( data.keynr < 0 ) {
        String message =
          BaseMessages.getString( PKG, "ValueMapper.RuntimeError.FieldToUseNotFound.VALUEMAPPER0001", meta
            .getFieldToUse(), Const.CR, getInputRowMeta().getString( r ) );
        logError( message );
        setErrors( 1 );
        stopAll();
        return false;
      }

      // If there is an empty entry: we map null or "" to the target at the index
      // 0 or 1 empty mapping is allowed, not 2 or more.
      //
      for ( int i = 0; i < meta.getSourceValue().length; i++ ) {
        if ( Utils.isEmpty( meta.getSourceValue()[ i ] ) ) {
          if ( data.emptyFieldIndex < 0 ) {
            data.emptyFieldIndex = i;
          } else {
            throw new HopException( BaseMessages.getString(
              PKG, "ValueMapper.RuntimeError.OnlyOneEmptyMappingAllowed.VALUEMAPPER0004" ) );
          }
        }
      }

      data.sourceValueMeta = getInputRowMeta().getValueMeta( data.keynr );

      if ( Utils.isEmpty( meta.getTargetField() ) ) {
        data.outputValueMeta = data.outputMeta.getValueMeta( data.keynr ); // Same field

      } else {
        data.outputValueMeta = data.outputMeta.searchValueMeta( meta.getTargetField() ); // new field
      }
    }

    Object sourceData = r[ data.keynr ];
    String source = data.sourceValueMeta.getCompatibleString( sourceData );
    String target = null;

    // Null/Empty mapping to value...
    //
    if ( data.emptyFieldIndex >= 0 && ( r[ data.keynr ] == null || Utils.isEmpty( source ) ) ) {
      target = meta.getTargetValue()[ data.emptyFieldIndex ]; // that's all there is to it.
    } else {
      if ( !Utils.isEmpty( source ) ) {
        target = data.hashtable.get( source );
        if ( nonMatchActivated && target == null ) {
          // If we do non matching and we don't have a match
          target = meta.getNonMatchDefault();
        }
      }
    }

    if ( !Utils.isEmpty( meta.getTargetField() ) ) {
      // room for the target
      r = RowDataUtil.resizeArray( r, data.outputMeta.size() );
      // Did we find anything to map to?
      if ( !Utils.isEmpty( target ) ) {
        r[ data.outputMeta.size() - 1 ] = target;
      } else {
        r[ data.outputMeta.size() - 1 ] = null;
      }
    } else {
      // Don't set the original value to null if we don't have a target.
      if ( target != null ) {
        if ( target.length() > 0 ) {
          // See if the expected type is a String...
          //
          if ( data.sourceValueMeta.isString() ) {
            r[ data.keynr ] = target;
          } else {
            // Do implicit conversion of the String to the target type...
            //
            r[ data.keynr ] = data.outputValueMeta.convertData( data.stringMeta, target );
          }
        } else {
          // allow target to be set to null since 3.0
          r[ data.keynr ] = null;
        }
      } else {
        // Convert to normal storage type.
        // Otherwise we're going to be mixing storage types.
        //
        if ( data.sourceValueMeta.isStorageBinaryString() ) {
          Object normal = data.sourceValueMeta.convertToNormalStorageType( r[ data.keynr ] );
          r[ data.keynr ] = normal;
        }
      }
    }
    putRow( data.outputMeta, r );

    return true;
  }

  public void dispose() {
    super.dispose();
  }

  public boolean init() {

    if ( super.init() ) {
      data.hashtable = new Hashtable<>();
      data.emptyFieldIndex = -1;

      if ( !Utils.isEmpty( meta.getNonMatchDefault() ) ) {
        nonMatchActivated = true;
      }

      // Add all source to target mappings in here...
      for ( int i = 0; i < meta.getSourceValue().length; i++ ) {
        String src = meta.getSourceValue()[ i ];
        String tgt = meta.getTargetValue()[ i ];

        if ( !Utils.isEmpty( src ) && !Utils.isEmpty( tgt ) ) {
          data.hashtable.put( src, tgt );
        } else {
          if ( Utils.isEmpty( tgt ) ) {
            // allow target to be set to null since 3.0
            data.hashtable.put( src, "" );
          }
        }
      }
      return true;
    }
    return false;
  }

}
