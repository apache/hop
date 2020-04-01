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

package org.apache.hop.pipeline.steps.javafilter;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStep;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.step.errorhandling.StreamInterface;
import org.codehaus.janino.ExpressionEvaluator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Calculate new field values using pre-defined functions.
 *
 * @author Matt
 * @since 8-sep-2005
 */
public class JavaFilter extends BaseStep implements StepInterface {
  private static Class<?> PKG = JavaFilterMeta.class; // for i18n purposes, needed by Translator!!

  private JavaFilterMeta meta;
  private JavaFilterData data;

  public JavaFilter( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, PipelineMeta pipelineMeta,
                     Pipeline pipeline ) {
    super( stepMeta, stepDataInterface, copyNr, pipelineMeta, pipeline );
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws HopException {
    meta = (JavaFilterMeta) smi;
    data = (JavaFilterData) sdi;

    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) { // no more input to be expected...

      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, metaStore );

      // Cache the position of the RowSet for the output.
      //
      if ( data.chosesTargetSteps ) {
        List<StreamInterface> targetStreams = meta.getStepIOMeta().getTargetStreams();
        data.trueRowSet = findOutputRowSet( getStepname(), getCopy(), targetStreams.get( 0 ).getStepname(), 0 );
        if ( data.trueRowSet == null ) {
          throw new HopException( BaseMessages.getString(
            PKG, "JavaFilter.Log.TargetStepInvalid", targetStreams.get( 0 ).getStepname() ) );
        }

        data.falseRowSet = findOutputRowSet( getStepname(), getCopy(), targetStreams.get( 1 ).getStepname(), 0 );
        if ( data.falseRowSet == null ) {
          throw new HopException( BaseMessages.getString(
            PKG, "JavaFilter.Log.TargetStepInvalid", targetStreams.get( 1 ).getStepname() ) );
        }
      }

    }

    if ( log.isRowLevel() ) {
      logRowlevel( "Read row #" + getLinesRead() + " : " + getInputRowMeta().getString( r ) );
    }

    boolean keep = calcFields( getInputRowMeta(), r );

    if ( !data.chosesTargetSteps ) {
      if ( keep ) {
        putRow( data.outputRowMeta, r ); // copy row to output rowset(s);
      }
    } else {
      if ( keep ) {
        if ( log.isRowLevel() ) {
          logRowlevel( "Sending row to true  :" + data.trueStepname + " : " + getInputRowMeta().getString( r ) );
        }
        putRowTo( data.outputRowMeta, r, data.trueRowSet );
      } else {
        if ( log.isRowLevel() ) {
          logRowlevel( "Sending row to false :" + data.falseStepname + " : " + getInputRowMeta().getString( r ) );
        }
        putRowTo( data.outputRowMeta, r, data.falseRowSet );
      }
    }

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "JavaFilter.Log.LineNumber" ) + getLinesRead() );
      }
    }

    return true;
  }

  private boolean calcFields( RowMetaInterface rowMeta, Object[] r ) throws HopValueException {
    try {
      // Initialize evaluators etc. Only do it once.
      //
      if ( data.expressionEvaluator == null ) {
        String realCondition = environmentSubstitute( meta.getCondition() );
        data.argumentIndexes = new ArrayList<Integer>();

        List<String> parameterNames = new ArrayList<>();
        List<Class<?>> parameterTypes = new ArrayList<Class<?>>();

        for ( int i = 0; i < data.outputRowMeta.size(); i++ ) {

          ValueMetaInterface valueMeta = data.outputRowMeta.getValueMeta( i );

          // See if the value is being used in a formula...
          //
          if ( realCondition.contains( valueMeta.getName() ) ) {
            // If so, add it to the indexes...
            data.argumentIndexes.add( i );

            Class<?> parameterType;
            switch ( valueMeta.getType() ) {
              case ValueMetaInterface.TYPE_STRING:
                parameterType = String.class;
                break;
              case ValueMetaInterface.TYPE_NUMBER:
                parameterType = Double.class;
                break;
              case ValueMetaInterface.TYPE_INTEGER:
                parameterType = Long.class;
                break;
              case ValueMetaInterface.TYPE_DATE:
                parameterType = Date.class;
                break;
              case ValueMetaInterface.TYPE_BIGNUMBER:
                parameterType = BigDecimal.class;
                break;
              case ValueMetaInterface.TYPE_BOOLEAN:
                parameterType = Boolean.class;
                break;
              case ValueMetaInterface.TYPE_BINARY:
                parameterType = byte[].class;
                break;
              default:
                parameterType = String.class;
                break;
            }
            parameterTypes.add( parameterType );
            parameterNames.add( valueMeta.getName() );
          }
        }

        // Create the expression evaluator: is relatively slow so we do it only for the first row...
        //
        data.expressionEvaluator = new ExpressionEvaluator();
        data.expressionEvaluator.setParameters(
          parameterNames.toArray( new String[ parameterNames.size() ] ), parameterTypes
            .toArray( new Class<?>[ parameterTypes.size() ] ) );
        data.expressionEvaluator.setReturnType( Object.class );
        data.expressionEvaluator.setThrownExceptions( new Class<?>[] { Exception.class } );
        data.expressionEvaluator.cook( realCondition );

        // Also create the argument data structure once...
        //
        data.argumentData = new Object[ data.argumentIndexes.size() ];
      }

      // This method can only accept the specified number of values...
      //
      for ( int x = 0; x < data.argumentIndexes.size(); x++ ) {
        int index = data.argumentIndexes.get( x );
        ValueMetaInterface outputValueMeta = data.outputRowMeta.getValueMeta( index );
        data.argumentData[ x ] = outputValueMeta.convertToNormalStorageType( r[ index ] );
      }

      Object formulaResult = data.expressionEvaluator.evaluate( data.argumentData );

      if ( formulaResult instanceof Boolean ) {
        return (Boolean) formulaResult;
      } else {
        throw new HopException( "The result of the filter expression must be a boolean and we got back : "
          + formulaResult.getClass().getName() );
      }
    } catch ( Exception e ) {
      throw new HopValueException( e );
    }
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (JavaFilterMeta) smi;
    data = (JavaFilterData) sdi;

    if ( super.init( smi, sdi ) ) {
      List<StreamInterface> targetStreams = meta.getStepIOMeta().getTargetStreams();
      data.trueStepname = targetStreams.get( 0 ).getStepname();
      data.falseStepname = targetStreams.get( 1 ).getStepname();

      if ( targetStreams.get( 0 ).getStepMeta() != null ^ targetStreams.get( 1 ).getStepMeta() != null ) {
        logError( BaseMessages.getString( PKG, "JavaFilter.Log.BothTrueAndFalseNeeded" ) );
      } else {
        data.chosesTargetSteps =
          targetStreams.get( 0 ).getStepMeta() != null && targetStreams.get( 1 ).getStepMeta() != null;

        return true;
      }
    }
    return false;
  }

}
