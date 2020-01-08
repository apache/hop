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

package org.apache.hop.trans.steps.janino;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStep;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.codehaus.janino.ExpressionEvaluator;

import java.util.ArrayList;
import java.util.List;

/**
 * Calculate new field values using pre-defined functions.
 *
 * @author Matt
 * @since 8-sep-2005
 */
public class Janino extends BaseStep implements StepInterface {
  private static Class<?> PKG = JaninoMeta.class;
  private JaninoMeta meta;
  private JaninoData data;

  public Janino( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                 Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {
    meta = (JaninoMeta) smi;
    data = (JaninoData) sdi;

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, metaStore );

      // Calculate replace indexes...
      //
      data.replaceIndex = new int[ meta.getFormula().length ];
      data.returnType = new ValueMetaInterface[ meta.getFormula().length ];
      for ( int i = 0; i < meta.getFormula().length; i++ ) {
        JaninoMetaFunction fn = meta.getFormula()[ i ];
        data.returnType[ i ] = ValueMetaFactory.createValueMeta( fn.getValueType() );
        if ( !Utils.isEmpty( fn.getReplaceField() ) ) {
          data.replaceIndex[ i ] = getInputRowMeta().indexOfValue( fn.getReplaceField() );
          if ( data.replaceIndex[ i ] < 0 ) {
            throw new HopException( "Unknown field specified to replace with a formula result: ["
              + fn.getReplaceField() + "]" );
          }
        } else {
          data.replaceIndex[ i ] = -1;
        }
      }
    }

    if ( log.isRowLevel() ) {
      logRowlevel( "Read row #" + getLinesRead() + " : " + getInputRowMeta().getString( r ) );
    }

    try {
      Object[] outputRowData = calcFields( getInputRowMeta(), r );
      putRow( data.outputRowMeta, outputRowData ); // copy row to possible alternate rowset(s).
      if ( log.isRowLevel() ) {
        logRowlevel( "Wrote row #" + getLinesWritten() + " : " + data.outputRowMeta.getString( outputRowData ) );
      }
    } catch ( Exception e ) {
      if ( getStepMeta().isDoingErrorHandling() ) {
        putError( getInputRowMeta(), r, 1L, e.toString(), null, "UJE001" );
      } else {
        throw new HopException( e );
      }
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( "Linenr " + getLinesRead() );
    }

    return true;
  }

  private Object[] calcFields( RowMetaInterface rowMeta, Object[] r ) throws HopValueException {
    try {
      Object[] outputRowData = RowDataUtil.createResizedCopy( r, data.outputRowMeta.size() );
      int tempIndex = rowMeta.size();

      // Initialize evaluators etc. Only do it once.
      //
      if ( data.expressionEvaluators == null ) {
        data.expressionEvaluators = new ExpressionEvaluator[ meta.getFormula().length ];
        data.argumentIndexes = new ArrayList<List<Integer>>();

        for ( int i = 0; i < meta.getFormula().length; i++ ) {
          List<Integer> argIndexes = new ArrayList<Integer>();
          data.argumentIndexes.add( argIndexes );
        }

        for ( int m = 0; m < meta.getFormula().length; m++ ) {
          List<Integer> argIndexes = data.argumentIndexes.get( m );
          List<String> parameterNames = new ArrayList<String>();
          List<Class<?>> parameterTypes = new ArrayList<Class<?>>();

          for ( int i = 0; i < data.outputRowMeta.size(); i++ ) {

            ValueMetaInterface valueMeta = data.outputRowMeta.getValueMeta( i );

            // See if the value is being used in a formula...
            //
            if ( meta.getFormula()[ m ].getFormula().contains( valueMeta.getName() ) ) {
              // If so, add it to the indexes...
              argIndexes.add( i );

              parameterTypes.add( valueMeta.getNativeDataTypeClass() );
              parameterNames.add( valueMeta.getName() );
            }
          }

          JaninoMetaFunction fn = meta.getFormula()[ m ];
          if ( !Utils.isEmpty( fn.getFieldName() ) ) {

            // Create the expression evaluator: is relatively slow so we do it only for the first row...
            //
            data.expressionEvaluators[ m ] = new ExpressionEvaluator();
            data.expressionEvaluators[ m ].setParameters(
              parameterNames.toArray( new String[ parameterNames.size() ] ), parameterTypes
                .toArray( new Class<?>[ parameterTypes.size() ] ) );
            data.expressionEvaluators[ m ].setReturnType( Object.class );
            data.expressionEvaluators[ m ].setThrownExceptions( new Class<?>[] { Exception.class } );
            data.expressionEvaluators[ m ].cook( fn.getFormula() );
          } else {
            throw new HopException( "Unable to find field name for formula ["
              + Const.NVL( fn.getFormula(), "" ) + "]" );
          }
        }
      }

      for ( int i = 0; i < meta.getFormula().length; i++ ) {
        List<Integer> argumentIndexes = data.argumentIndexes.get( i );

        // This method can only accept the specified number of values...
        //
        Object[] argumentData = new Object[ argumentIndexes.size() ];
        for ( int x = 0; x < argumentIndexes.size(); x++ ) {
          int index = argumentIndexes.get( x );
          ValueMetaInterface outputValueMeta = data.outputRowMeta.getValueMeta( index );
          argumentData[ x ] = outputValueMeta.convertToNormalStorageType( outputRowData[ index ] );
        }

        Object formulaResult = data.expressionEvaluators[ i ].evaluate( argumentData );

        Object value = null;
        if ( formulaResult == null ) {
          value = null;
        } else {
          ValueMetaInterface valueMeta = data.returnType[ i ];
          if ( valueMeta.getNativeDataTypeClass().isAssignableFrom( formulaResult.getClass() ) ) {
            value = formulaResult;
          } else if ( formulaResult instanceof Integer && valueMeta.getType() == ValueMetaInterface.TYPE_INTEGER ) {
            value = ( (Integer) formulaResult ).longValue();
          } else {
            throw new HopValueException(
              BaseMessages.getString( PKG, "Janino.Error.ValueTypeMismatch", valueMeta.getTypeDesc(),
                meta.getFormula()[ i ].getFieldName(), formulaResult.getClass(), meta.getFormula()[ i ].getFormula() ) );
          }
        }

        // We're done, store it in the row with all the data, including the temporary data...
        //
        if ( data.replaceIndex[ i ] < 0 ) {
          outputRowData[ tempIndex++ ] = value;
        } else {
          outputRowData[ data.replaceIndex[ i ] ] = value;
        }
      }

      return outputRowData;
    } catch ( Exception e ) {
      throw new HopValueException( e );
    }
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (JaninoMeta) smi;
    data = (JaninoData) sdi;

    return super.init( smi, sdi );
  }

}
