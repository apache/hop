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

package org.apache.hop.pipeline.transforms.janino;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.codehaus.janino.ExpressionEvaluator;

import java.util.ArrayList;
import java.util.List;

/**
 * Calculate new field values using pre-defined functions.
 *
 * @author Matt
 * @since 8-sep-2005
 */
public class Janino extends BaseTransform<JaninoMeta, JaninoData> implements ITransform<JaninoMeta, JaninoData> {
  private static final Class<?> PKG = JaninoMeta.class; // For Translator

  public Janino(TransformMeta transformMeta, JaninoMeta meta, JaninoData data, int copyNr, PipelineMeta pipelineMeta,
                Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getTransformName(), null, null, this, metadataProvider );

      // Calculate replace indexes...
      //
      data.replaceIndex = new int[ meta.getFormula().length ];
      data.returnType = new IValueMeta[ meta.getFormula().length ];
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
      if ( getTransformMeta().isDoingErrorHandling() ) {
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

  private Object[] calcFields( IRowMeta rowMeta, Object[] r ) throws HopValueException {
    try {
      Object[] outputRowData = RowDataUtil.createResizedCopy( r, data.outputRowMeta.size() );
      int tempIndex = rowMeta.size();

      // Initialize evaluators etc. Only do it once.
      //
      if ( data.expressionEvaluators == null ) {
        data.expressionEvaluators = new ExpressionEvaluator[ meta.getFormula().length ];
        data.argumentIndexes = new ArrayList<>();

        for ( int i = 0; i < meta.getFormula().length; i++ ) {
          List<Integer> argIndexes = new ArrayList<>();
          data.argumentIndexes.add( argIndexes );
        }

        for ( int m = 0; m < meta.getFormula().length; m++ ) {
          List<Integer> argIndexes = data.argumentIndexes.get( m );
          List<String> parameterNames = new ArrayList<>();
          List<Class<?>> parameterTypes = new ArrayList<>();

          for ( int i = 0; i < data.outputRowMeta.size(); i++ ) {

            IValueMeta valueMeta = data.outputRowMeta.getValueMeta( i );

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
              + (!StringUtil.isEmpty(fn.getFormula()) ? fn.getFormula() : "") + "]" );
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
          IValueMeta outputValueMeta = data.outputRowMeta.getValueMeta( index );
          argumentData[ x ] = outputValueMeta.convertToNormalStorageType( outputRowData[ index ] );
        }

        Object formulaResult = data.expressionEvaluators[ i ].evaluate( argumentData );

        Object value = null;
        if ( formulaResult == null ) {
          value = null;
        } else {
          IValueMeta valueMeta = data.returnType[ i ];
          if ( valueMeta.getNativeDataTypeClass().isAssignableFrom( formulaResult.getClass() ) ) {
            value = formulaResult;
          } else if ( formulaResult instanceof Integer && valueMeta.getType() == IValueMeta.TYPE_INTEGER ) {
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
  public boolean init() {

    return super.init();
  }

}
