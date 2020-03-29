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

package org.apache.hop.trans.steps.closure;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStep;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;

import java.util.HashMap;

/**
 * Reads information from a database table by using freehand SQL
 *
 * @author Matt
 * @since 8-apr-2003
 */
public class ClosureGenerator extends BaseStep implements StepInterface {
  private static final Class<?> PKG = ClosureGeneratorMeta.class; // for i18n purposes, needed by Translator2!!

  private ClosureGeneratorMeta meta;
  private ClosureGeneratorData data;

  public ClosureGenerator( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                           TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {
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
          meta.getFields( data.outputRowMeta, getStepname(), null, null, this, metaStore );

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
        data.parents = new HashMap<Object, Long>();

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
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ClosureGeneratorMeta) smi;
    data = (ClosureGeneratorData) sdi;

    if ( super.init( smi, sdi ) ) {
      data.reading = true;
      data.map = new HashMap<Object, Object>();

      data.topLevel = null;
      if ( meta.isRootIdZero() ) {
        data.topLevel = new Long( 0 );
      }

      return true;
    }

    return false;
  }

}
