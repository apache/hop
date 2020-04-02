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

package org.apache.hop.pipeline.transforms.filterrows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface.StreamType;
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
public class FilterRowsMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = FilterRowsMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * This is the main condition for the complete filter.
   *
   * @since version 2.1
   */
  private Condition condition;

  public FilterRowsMeta() {
    super(); // allocate BaseTransformMeta
    condition = new Condition();
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
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

    retval.setTrueTransformName( getTrueTransformName() );
    retval.setFalseTransformName( getFalseTransformName() );

    if ( condition != null ) {
      retval.condition = (Condition) condition.clone();
    } else {
      retval.condition = null;
    }

    return retval;
  }

  public String getXML() throws HopException {
    StringBuilder retval = new StringBuilder( 200 );

    retval.append( XMLHandler.addTagValue( "send_true_to", getTrueTransformName() ) );
    retval.append( XMLHandler.addTagValue( "send_false_to", getFalseTransformName() ) );
    retval.append( "    <compare>" ).append( Const.CR );

    if ( condition != null ) {
      retval.append( condition.getXML() );
    }

    retval.append( "    </compare>" ).append( Const.CR );

    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      setTrueTransformName( XMLHandler.getTagValue( transformNode, "send_true_to" ) );
      setFalseTransformName( XMLHandler.getTagValue( transformNode, "send_false_to" ) );

      Node compare = XMLHandler.getSubNode( transformNode, "compare" );
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
        PKG, "FilterRowsMeta.Exception..UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public void setDefault() {
    allocate();
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    List<StreamInterface> targetStreams = getTransformIOMeta().getTargetStreams();
    for ( StreamInterface stream : targetStreams ) {
      stream.setTransformMeta( TransformMeta.findTransform( transforms, (String) stream.getSubject() ) );
    }
  }

  public void getFields( IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // Clear the sortedDescending flag on fields used within the condition - otherwise the comparisons will be
    // inverted!!
    String[] conditionField = condition.getUsedFields();
    for ( int i = 0; i < conditionField.length; i++ ) {
      int idx = rowMeta.indexOfValue( conditionField[ i ] );
      if ( idx >= 0 ) {
        IValueMeta valueMeta = rowMeta.getValueMeta( idx );
        valueMeta.setSortedDescending( false );
      }
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    String error_message = "";

    checkTarget( transformMeta, "true", getTrueTransformName(), output ).ifPresent( remarks::add );
    checkTarget( transformMeta, "false", getFalseTransformName(), output ).ifPresent( remarks::add );

    if ( condition.isEmpty() ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FilterRowsMeta.CheckResult.NoConditionSpecified" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FilterRowsMeta.CheckResult.ConditionSpecified" ), transformMeta );
    }
    remarks.add( cr );

    // Look up fields in the input stream <prev>
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FilterRowsMeta.CheckResult.TransformReceivingFields", prev.size() + "" ), transformMeta );
      remarks.add( cr );

      List<String> orphanFields = getOrphanFields( condition, prev );
      if ( orphanFields.size() > 0 ) {
        error_message = BaseMessages.getString( PKG, "FilterRowsMeta.CheckResult.FieldsNotFoundFromPreviousTransform" )
          + Const.CR;
        for ( String field : orphanFields ) {
          error_message += "\t\t" + field + Const.CR;
        }
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
            "FilterRowsMeta.CheckResult.AllFieldsFoundInInputStream" ), transformMeta );
      }
      remarks.add( cr );
    } else {
      error_message =
        BaseMessages.getString( PKG, "FilterRowsMeta.CheckResult.CouldNotReadFieldsFromPreviousTransform" )
          + Const.CR;
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FilterRowsMeta.CheckResult.TransformReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FilterRowsMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    }
  }

  private Optional<CheckResult> checkTarget( TransformMeta transformMeta, String target, String targetTransformName,
                                             String[] output ) {
    if ( targetTransformName != null ) {
      int trueTargetIdx = Const.indexOfString( targetTransformName, output );
      if ( trueTargetIdx < 0 ) {
        return Optional.of( new CheckResult(
          CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "FilterRowsMeta.CheckResult.TargetTransformInvalid", target, targetTransformName ),
          transformMeta
        ) );
      }
    }
    return Optional.empty();
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData iTransformData, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new FilterRows( transformMeta, iTransformData, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new FilterRowsData();
  }

  /**
   * Returns the Input/Output metadata for this transform.
   */
  public TransformIOMetaInterface getTransformIOMeta() {
    TransformIOMetaInterface ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new TransformIOMeta( true, true, false, false, false, false );

      ioMeta.addStream( new Stream( StreamType.TARGET, null, BaseMessages.getString(
        PKG, "FilterRowsMeta.InfoStream.True.Description" ), StreamIcon.TRUE, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, null, BaseMessages.getString(
        PKG, "FilterRowsMeta.InfoStream.False.Description" ), StreamIcon.FALSE, null ) );
      setTransformIOMeta( ioMeta );
    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
  }

  /**
   * When an optional stream is selected, this method is called to handled the ETL metadata implications of that.
   *
   * @param stream The optional stream to handle.
   */
  public void handleStreamSelection( StreamInterface stream ) {
    // This transform targets another transform.
    // Make sure that we don't specify the same transform for true and false...
    // If the user requests false, we blank out true and vice versa
    //
    List<StreamInterface> targets = getTransformIOMeta().getTargetStreams();
    int index = targets.indexOf( stream );
    if ( index == 0 ) {
      // True
      //
      TransformMeta falseTransform = targets.get( 1 ).getTransformMeta();
      if ( falseTransform != null && falseTransform.equals( stream.getTransformMeta() ) ) {
        targets.get( 1 ).setTransformMeta( null );
      }
    }
    if ( index == 1 ) {
      // False
      //
      TransformMeta trueTransform = targets.get( 0 ).getTransformMeta();
      if ( trueTransform != null && trueTransform.equals( stream.getTransformMeta() ) ) {
        targets.get( 0 ).setTransformMeta( null );
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
  public List<String> getOrphanFields( Condition condition, IRowMeta prev ) {
    List<String> orphans = new ArrayList<>();
    if ( condition == null || prev == null ) {
      return orphans;
    }
    String[] key = condition.getUsedFields();
    for ( int i = 0; i < key.length; i++ ) {
      if ( Utils.isEmpty( key[ i ] ) ) {
        continue;
      }
      IValueMeta v = prev.searchValueMeta( key[ i ] );
      if ( v == null ) {
        orphans.add( key[ i ] );
      }
    }
    return orphans;
  }

  public String getTrueTransformName() {
    return getTargetTransformName( 0 );
  }

  @Injection( name = "SEND_TRUE_TRANSFORM" )
  public void setTrueTransformName( String trueTransformName ) {
    getTransformIOMeta().getTargetStreams().get( 0 ).setSubject( trueTransformName );
  }

  public String getFalseTransformName() {
    return getTargetTransformName( 1 );
  }

  @Injection( name = "SEND_FALSE_TRANSFORM" )
  public void setFalseTransformName( String falseTransformName ) {
    getTransformIOMeta().getTargetStreams().get( 1 ).setSubject( falseTransformName );
  }

  private String getTargetTransformName( int streamIndex ) {
    StreamInterface stream = getTransformIOMeta().getTargetStreams().get( streamIndex );
    return java.util.stream.Stream.of( stream.getTransformName(), stream.getSubject() )
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
