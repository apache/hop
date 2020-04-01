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

package org.apache.hop.pipeline.steps.filterrows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepIOMeta;
import org.apache.hop.pipeline.step.StepIOMetaInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.step.errorhandling.Stream;
import org.apache.hop.pipeline.step.errorhandling.StreamIcon;
import org.apache.hop.pipeline.step.errorhandling.StreamInterface;
import org.apache.hop.pipeline.step.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/*
 * Created on 02-jun-2003
 *
 */
@InjectionSupported( localizationPrefix = "FilterRowsMeta.Injection." )
public class FilterRowsMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = FilterRowsMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * This is the main condition for the complete filter.
   *
   * @since version 2.1
   */
  private Condition condition;

  public FilterRowsMeta() {
    super(); // allocate BaseStepMeta
    condition = new Condition();
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  /**
   * @return Returns the condition.
   */
  public Condition getCondition() {
    return condition;
  }

  /**
   * @param condition The condition to set.
   */
  public void setCondition( Condition condition ) {
    this.condition = condition;
  }

  public void allocate() {
    condition = new Condition();
  }

  public Object clone() {
    FilterRowsMeta retval = (FilterRowsMeta) super.clone();

    retval.setTrueStepname( getTrueStepname() );
    retval.setFalseStepname( getFalseStepname() );

    if ( condition != null ) {
      retval.condition = (Condition) condition.clone();
    } else {
      retval.condition = null;
    }

    return retval;
  }

  public String getXML() throws HopException {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( XMLHandler.addTagValue( "send_true_to", getTrueStepname() ) );
    retval.append( XMLHandler.addTagValue( "send_false_to", getFalseStepname() ) );
    retval.append( "    <compare>" ).append( Const.CR );

    if ( condition != null ) {
      retval.append( condition.getXML() );
    }

    retval.append( "    </compare>" ).append( Const.CR );

    return retval.toString();
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {
      setTrueStepname( XMLHandler.getTagValue( stepnode, "send_true_to" ) );
      setFalseStepname( XMLHandler.getTagValue( stepnode, "send_false_to" ) );

      Node compare = XMLHandler.getSubNode( stepnode, "compare" );
      Node condnode = XMLHandler.getSubNode( compare, "condition" );

      // The new situation...
      if ( condnode != null ) {
        condition = new Condition( condnode );
      } else {
        // Old style condition: Line1 OR Line2 OR Line3: @deprecated!
        condition = new Condition();

        int nrkeys = XMLHandler.countNodes( compare, "key" );
        if ( nrkeys == 1 ) {
          Node knode = XMLHandler.getSubNodeByNr( compare, "key", 0 );

          String key = XMLHandler.getTagValue( knode, "name" );
          String value = XMLHandler.getTagValue( knode, "value" );
          String field = XMLHandler.getTagValue( knode, "field" );
          String comparator = XMLHandler.getTagValue( knode, "condition" );

          condition.setOperator( Condition.OPERATOR_NONE );
          condition.setLeftValuename( key );
          condition.setFunction( Condition.getFunction( comparator ) );
          condition.setRightValuename( field );
          condition.setRightExact( new ValueMetaAndData( "value", value ) );
        } else {
          for ( int i = 0; i < nrkeys; i++ ) {
            Node knode = XMLHandler.getSubNodeByNr( compare, "key", i );

            String key = XMLHandler.getTagValue( knode, "name" );
            String value = XMLHandler.getTagValue( knode, "value" );
            String field = XMLHandler.getTagValue( knode, "field" );
            String comparator = XMLHandler.getTagValue( knode, "condition" );

            Condition subc = new Condition();
            if ( i > 0 ) {
              subc.setOperator( Condition.OPERATOR_OR );
            } else {
              subc.setOperator( Condition.OPERATOR_NONE );
            }
            subc.setLeftValuename( key );
            subc.setFunction( Condition.getFunction( comparator ) );
            subc.setRightValuename( field );
            subc.setRightExact( new ValueMetaAndData( "value", value ) );

            condition.addCondition( subc );
          }
        }
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "FilterRowsMeta.Exception..UnableToLoadStepInfoFromXML" ), e );
    }
  }

  public void setDefault() {
    allocate();
  }

  @Override
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    List<StreamInterface> targetStreams = getStepIOMeta().getTargetStreams();
    for ( StreamInterface stream : targetStreams ) {
      stream.setStepMeta( StepMeta.findStep( steps, (String) stream.getSubject() ) );
    }
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // Clear the sortedDescending flag on fields used within the condition - otherwise the comparisons will be
    // inverted!!
    String[] conditionField = condition.getUsedFields();
    for ( int i = 0; i < conditionField.length; i++ ) {
      int idx = rowMeta.indexOfValue( conditionField[ i ] );
      if ( idx >= 0 ) {
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( idx );
        valueMeta.setSortedDescending( false );
      }
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    checkTarget( stepMeta, "true", getTrueStepname(), output ).ifPresent( remarks::add );
    checkTarget( stepMeta, "false", getFalseStepname(), output ).ifPresent( remarks::add );

    if ( condition.isEmpty() ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FilterRowsMeta.CheckResult.NoConditionSpecified" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FilterRowsMeta.CheckResult.ConditionSpecified" ), stepMeta );
    }
    remarks.add( cr );

    // Look up fields in the input stream <prev>
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FilterRowsMeta.CheckResult.StepReceivingFields", prev.size() + "" ), stepMeta );
      remarks.add( cr );

      List<String> orphanFields = getOrphanFields( condition, prev );
      if ( orphanFields.size() > 0 ) {
        error_message = BaseMessages.getString( PKG, "FilterRowsMeta.CheckResult.FieldsNotFoundFromPreviousStep" )
          + Const.CR;
        for ( String field : orphanFields ) {
          error_message += "\t\t" + field + Const.CR;
        }
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
            "FilterRowsMeta.CheckResult.AllFieldsFoundInInputStream" ), stepMeta );
      }
      remarks.add( cr );
    } else {
      error_message =
        BaseMessages.getString( PKG, "FilterRowsMeta.CheckResult.CouldNotReadFieldsFromPreviousStep" )
          + Const.CR;
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FilterRowsMeta.CheckResult.StepReceivingInfoFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FilterRowsMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  private Optional<CheckResult> checkTarget( StepMeta stepMeta, String target, String targetStepName,
                                             String[] output ) {
    if ( targetStepName != null ) {
      int trueTargetIdx = Const.indexOfString( targetStepName, output );
      if ( trueTargetIdx < 0 ) {
        return Optional.of( new CheckResult(
          CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "FilterRowsMeta.CheckResult.TargetStepInvalid", target, targetStepName ),
          stepMeta
        ) );
      }
    }
    return Optional.empty();
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new FilterRows( stepMeta, stepDataInterface, cnr, tr, pipeline );
  }

  public StepDataInterface getStepData() {
    return new FilterRowsData();
  }

  /**
   * Returns the Input/Output metadata for this step.
   */
  public StepIOMetaInterface getStepIOMeta() {
    StepIOMetaInterface ioMeta = super.getStepIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new StepIOMeta( true, true, false, false, false, false );

      ioMeta.addStream( new Stream( StreamType.TARGET, null, BaseMessages.getString(
        PKG, "FilterRowsMeta.InfoStream.True.Description" ), StreamIcon.TRUE, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, null, BaseMessages.getString(
        PKG, "FilterRowsMeta.InfoStream.False.Description" ), StreamIcon.FALSE, null ) );
      setStepIOMeta( ioMeta );
    }

    return ioMeta;
  }

  @Override
  public void resetStepIoMeta() {
  }

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata implications of that.
   *
   * @param stream The optional stream to handle.
   */
  public void handleStreamSelection( StreamInterface stream ) {
    // This step targets another step.
    // Make sure that we don't specify the same step for true and false...
    // If the user requests false, we blank out true and vice versa
    //
    List<StreamInterface> targets = getStepIOMeta().getTargetStreams();
    int index = targets.indexOf( stream );
    if ( index == 0 ) {
      // True
      //
      StepMeta falseStep = targets.get( 1 ).getStepMeta();
      if ( falseStep != null && falseStep.equals( stream.getStepMeta() ) ) {
        targets.get( 1 ).setStepMeta( null );
      }
    }
    if ( index == 1 ) {
      // False
      //
      StepMeta trueStep = targets.get( 0 ).getStepMeta();
      if ( trueStep != null && trueStep.equals( stream.getStepMeta() ) ) {
        targets.get( 0 ).setStepMeta( null );
      }
    }
  }

  @Override
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  /**
   * Get non-existing referenced input fields
   *
   * @param condition
   * @param prev
   * @return
   */
  public List<String> getOrphanFields( Condition condition, RowMetaInterface prev ) {
    List<String> orphans = new ArrayList<>();
    if ( condition == null || prev == null ) {
      return orphans;
    }
    String[] key = condition.getUsedFields();
    for ( int i = 0; i < key.length; i++ ) {
      if ( Utils.isEmpty( key[ i ] ) ) {
        continue;
      }
      ValueMetaInterface v = prev.searchValueMeta( key[ i ] );
      if ( v == null ) {
        orphans.add( key[ i ] );
      }
    }
    return orphans;
  }

  public String getTrueStepname() {
    return getTargetStepName( 0 );
  }

  @Injection( name = "SEND_TRUE_STEP" )
  public void setTrueStepname( String trueStepname ) {
    getStepIOMeta().getTargetStreams().get( 0 ).setSubject( trueStepname );
  }

  public String getFalseStepname() {
    return getTargetStepName( 1 );
  }

  @Injection( name = "SEND_FALSE_STEP" )
  public void setFalseStepname( String falseStepname ) {
    getStepIOMeta().getTargetStreams().get( 1 ).setSubject( falseStepname );
  }

  private String getTargetStepName( int streamIndex ) {
    StreamInterface stream = getStepIOMeta().getTargetStreams().get( streamIndex );
    return java.util.stream.Stream.of( stream.getStepname(), stream.getSubject() )
      .filter( Objects::nonNull )
      .findFirst().map( Object::toString ).orElse( null );
  }

  public String getConditionXML() {
    String conditionXML = null;
    try {
      conditionXML = condition.getXML();
    } catch ( HopValueException e ) {
      log.logError( e.getMessage() );
    }
    return conditionXML;
  }

  @Injection( name = "CONDITION" )
  public void setConditionXML( String conditionXML ) {
    try {
      this.condition = new Condition( conditionXML );
    } catch ( HopXMLException e ) {
      log.logError( e.getMessage() );
    }
  }
}
