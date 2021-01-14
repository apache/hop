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

package org.apache.hop.pipeline.transforms.closure;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;

import java.util.HashMap;

/**
 * Reads information from a database table by using freehand SQL
 *
 * @author Matt
 * @since 8-apr-2003
 */
public class ClosureGenerator extends BaseTransform<ClosureGeneratorMeta, ClosureGeneratorData> implements ITransform<ClosureGeneratorMeta, ClosureGeneratorData> {

  private static final Class<?> PKG = ClosureGeneratorMeta.class; // For Translator

  public ClosureGenerator( TransformMeta transformMeta, ClosureGeneratorMeta meta, ClosureGeneratorData data, int copyNr,
                           PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {
    if ( data.reading ) {
      Object[] rowData = getRow();

      if ( rowData == null ) {
        data.reading = false;
      } else {
        if ( first ) {
          first = false;

          // Create the output row metadata
          //
          data.outputRowMeta = getInputRowMeta().clone();
          meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

          // Get indexes of parent and child field
          //
          data.parentIndex = getInputRowMeta().indexOfValue( meta.getParentIdFieldName() );
          if ( data.parentIndex < 0 ) {
            throw new HopException( BaseMessages.getString(
              PKG, "ClosureGenerator.Exception.ParentFieldNotFound" ) );
          }
          data.childIndex = getInputRowMeta().indexOfValue( meta.getChildIdFieldName() );
          if ( data.childIndex < 0 ) {
            throw new HopException( BaseMessages.getString(
              PKG, "ClosureGenerator.Exception.ChildFieldNotFound" ) );
          }

          data.parentValueMeta = getInputRowMeta().getValueMeta( data.parentIndex );
          data.childValueMeta = getInputRowMeta().getValueMeta( data.childIndex );
        }

        // add values to the buffer...
        //
        Object parentId = rowData[ data.parentIndex ];
        Object childId = rowData[ data.childIndex ];
        data.map.put( childId, parentId );
      }
    } else {
      // Writing the rows back...
      //
      for ( Object current : data.map.keySet() ) {
        data.parents = new HashMap<>();

        // add self as distance 0
        //
        data.parents.put( current, 0L );

        recurseParents( current, 1 );
        for ( Object parent : data.parents.keySet() ) {
          Object[] outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
          outputRow[ 0 ] = parent;
          outputRow[ 1 ] = current;
          outputRow[ 2 ] = data.parents.get( parent );
          putRow( data.outputRowMeta, outputRow );
        }
      }

      setOutputDone();
      return false;
    }

    return true;
  }

  private void recurseParents( Object key, long distance ) {
    // catch infinite loop - change at will
    if ( distance > 50 ) {
      throw new RuntimeException( "infinite loop detected:" + key );
    }
    Object parent = data.map.get( key );

    if ( parent == null || parent == data.topLevel || parent.equals( data.topLevel ) ) {
      return;
    } else {
      data.parents.put( parent, distance );
      recurseParents( parent, distance + 1 );
      return;
    }
  }

  @Override
  public boolean init(){

    if ( super.init() ) {
      data.reading = true;
      data.map = new HashMap<>();

      data.topLevel = null;
      if ( meta.isRootIdZero() ) {
        data.topLevel = new Long( 0 );
      }

      return true;
    }

    return false;
  }

}
