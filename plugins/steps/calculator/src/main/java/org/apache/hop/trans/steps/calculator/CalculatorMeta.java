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

package org.apache.hop.trans.steps.calculator;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

/**
 * Contains the meta-data for the Calculator step: calculates predefined formula's
 *
 * @since 08 september 2005
 */

@Step(
        id = "Calculator",
        image = "ui/images/CLC.svg",
        i18nPackageName = "i18n:org.apache.hop.trans.step.calculator",
        name = "BaseStep.TypeLongDesc.Calculator",
        description = "BaseStep.TypeTooltipDesc.Calculator",
        categoryDescription = "i18n:org.apache.hop.trans.step:BaseStep.Category.Transform"
)
public class CalculatorMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = CalculatorMeta.class; // for i18n purposes, needed by Translator2!!

  /**
   * The calculations to be performed
   */
  private CalculatorMetaFunction[] calculation;

  /**
   * Raise an error if file does not exist
   */
  private boolean failIfNoFile;

  public CalculatorMetaFunction[] getCalculation() {
    return calculation;
  }

  public void setCalculation( CalculatorMetaFunction[] calcTypes ) {
    this.calculation = calcTypes;
  }

  public boolean isFailIfNoFile() {
    return failIfNoFile;
  }

  public void setFailIfNoFile( boolean failIfNoFile ) {
    this.failIfNoFile = failIfNoFile;
  }

  public void allocate( int nrCalcs ) {
    calculation = new CalculatorMetaFunction[ nrCalcs ];
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    failIfNoFile = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "failIfNoFile" ) );

    int nrCalcs = XMLHandler.countNodes( stepnode, CalculatorMetaFunction.XML_TAG );
    allocate( nrCalcs );
    for ( int i = 0; i < nrCalcs; i++ ) {
      Node calcnode = XMLHandler.getSubNodeByNr( stepnode, CalculatorMetaFunction.XML_TAG, i );
      calculation[ i ] = new CalculatorMetaFunction( calcnode );
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "failIfNoFile", failIfNoFile ) );

    if ( calculation != null ) {
      for ( CalculatorMetaFunction aCalculation : calculation ) {
        retval.append( aCalculation.getXML() );
      }
    }

    return retval.toString();
  }

  @Override
  public boolean equals( Object obj ) {
    if ( obj != null && ( obj.getClass().equals( this.getClass() ) ) ) {
      CalculatorMeta m = (CalculatorMeta) obj;
      return ( getXML().equals( m.getXML() ) );
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode( calculation );
  }

  @Override
  public Object clone() {
    CalculatorMeta retval = (CalculatorMeta) super.clone();
    retval.setFailIfNoFile( isFailIfNoFile() );
    if ( calculation != null ) {
      retval.allocate( calculation.length );
      for ( int i = 0; i < calculation.length; i++ ) {
        ( retval.getCalculation() )[ i ] = (CalculatorMetaFunction) calculation[ i ].clone();
      }
    } else {
      retval.allocate( 0 );
    }
    return retval;
  }

  @Override
  public void setDefault() {
    failIfNoFile = true;
    calculation = new CalculatorMetaFunction[ 0 ];
  }

  @Override
  public void getFields( RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    for ( CalculatorMetaFunction fn : calculation ) {
      if ( !fn.isRemovedFromResult() ) {
        if ( !Utils.isEmpty( fn.getFieldName() ) ) { // It's a new field!
          ValueMetaInterface v = getValueMeta( fn, origin );
          row.addValueMeta( v );
        }
      }
    }
  }

  private ValueMetaInterface getValueMeta( CalculatorMetaFunction fn, String origin ) {
    ValueMetaInterface v;
    // What if the user didn't specify a data type?
    // In that case we look for the default data type
    //
    int defaultResultType = fn.getValueType();
    if ( defaultResultType == ValueMetaInterface.TYPE_NONE ) {
      defaultResultType = CalculatorMetaFunction.getCalcFunctionDefaultResultType( fn.getCalcType() );
    }
    try {
      v = ValueMetaFactory.createValueMeta( fn.getFieldName(), defaultResultType );
    } catch ( Exception ex ) {
      return null;
    }
    v.setLength( fn.getValueLength() );
    v.setPrecision( fn.getValuePrecision() );
    v.setOrigin( origin );
    v.setComments( fn.getCalcTypeDesc() );
    v.setConversionMask( fn.getConversionMask() );
    v.setDecimalSymbol( fn.getDecimalSymbol() );
    v.setGroupingSymbol( fn.getGroupingSymbol() );
    v.setCurrencySymbol( fn.getCurrencySymbol() );

    return v;
  }

  public RowMetaInterface getAllFields( RowMetaInterface inputRowMeta ) {
    RowMetaInterface rowMeta = inputRowMeta.clone();

    for ( CalculatorMetaFunction fn : getCalculation() ) {
      if ( !Utils.isEmpty( fn.getFieldName() ) ) { // It's a new field!
        ValueMetaInterface v = getValueMeta( fn, null );
        rowMeta.addValueMeta( v );
      }
    }
    return rowMeta;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "CalculatorMeta.CheckResult.ExpectedInputOk" ), stepMeta );
      remarks.add( cr );

      if ( prev == null || prev.size() == 0 ) {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
            PKG, "CalculatorMeta.CheckResult.ExpectedInputError" ), stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "CalculatorMeta.CheckResult.FieldsReceived", "" + prev.size() ), stepMeta );
        remarks.add( cr );
      }
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "CalculatorMeta.CheckResult.ExpectedInputError" ), stepMeta );
      remarks.add( cr );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new Calculator( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new CalculatorData();
  }

  @Override
  public String getDialogClassName(){
    return CalculatorDialog.class.getName();
  }
}
