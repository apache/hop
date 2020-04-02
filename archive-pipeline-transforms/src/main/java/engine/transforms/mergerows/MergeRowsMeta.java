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

package org.apache.hop.pipeline.transforms.mergerows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */
@InjectionSupported( localizationPrefix = "MergeRows.Injection." )
public class MergeRowsMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = MergeRowsMeta.class; // for i18n purposes, needed by Translator!!

  @Injection( name = "FLAG_FIELD" )
  private String flagField;

  @Injection( name = "KEY_FIELDS" )
  private String[] keyFields;
  @Injection( name = "VALUE_FIELDS" )
  private String[] valueFields;

  /**
   * @return Returns the keyFields.
   */
  public String[] getKeyFields() {
    return keyFields;
  }

  /**
   * @param keyFields The keyFields to set.
   */
  public void setKeyFields( String[] keyFields ) {
    this.keyFields = keyFields;
  }

  /**
   * @return Returns the valueFields.
   */
  public String[] getValueFields() {
    return valueFields;
  }

  /**
   * @param valueFields The valueFields to set.
   */
  public void setValueFields( String[] valueFields ) {
    this.valueFields = valueFields;
  }

  public MergeRowsMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  /**
   * @return Returns the flagField.
   */
  public String getFlagField() {
    return flagField;
  }

  /**
   * @param flagField The flagField to set.
   */
  public void setFlagField( String flagField ) {
    this.flagField = flagField;
  }

  public void allocate( int nrKeys, int nrValues ) {
    keyFields = new String[ nrKeys ];
    valueFields = new String[ nrValues ];
  }

  @Override
  public Object clone() {
    MergeRowsMeta retval = (MergeRowsMeta) super.clone();
    int nrKeys = keyFields.length;
    int nrValues = valueFields.length;
    retval.allocate( nrKeys, nrValues );
    System.arraycopy( keyFields, 0, retval.keyFields, 0, nrKeys );
    System.arraycopy( valueFields, 0, retval.valueFields, 0, nrValues );
    return retval;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    <keys>" + Const.CR );
    for ( int i = 0; i < keyFields.length; i++ ) {
      retval.append( "      " + XMLHandler.addTagValue( "key", keyFields[ i ] ) );
    }
    retval.append( "    </keys>" + Const.CR );

    retval.append( "    <values>" + Const.CR );
    for ( int i = 0; i < valueFields.length; i++ ) {
      retval.append( "      " + XMLHandler.addTagValue( "value", valueFields[ i ] ) );
    }
    retval.append( "    </values>" + Const.CR );

    retval.append( XMLHandler.addTagValue( "flag_field", flagField ) );

    List<StreamInterface> infoStreams = getTransformIOMeta().getInfoStreams();
    retval.append( XMLHandler.addTagValue( "reference", infoStreams.get( 0 ).getTransformName() ) );
    retval.append( XMLHandler.addTagValue( "compare", infoStreams.get( 1 ).getTransformName() ) );
    retval.append( "    <compare>" + Const.CR );

    retval.append( "    </compare>" + Const.CR );

    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {

      Node keysnode = XMLHandler.getSubNode( transformNode, "keys" );
      Node valuesnode = XMLHandler.getSubNode( transformNode, "values" );

      int nrKeys = XMLHandler.countNodes( keysnode, "key" );
      int nrValues = XMLHandler.countNodes( valuesnode, "value" );

      allocate( nrKeys, nrValues );

      for ( int i = 0; i < nrKeys; i++ ) {
        Node keynode = XMLHandler.getSubNodeByNr( keysnode, "key", i );
        keyFields[ i ] = XMLHandler.getNodeValue( keynode );
      }

      for ( int i = 0; i < nrValues; i++ ) {
        Node valuenode = XMLHandler.getSubNodeByNr( valuesnode, "value", i );
        valueFields[ i ] = XMLHandler.getNodeValue( valuenode );
      }

      flagField = XMLHandler.getTagValue( transformNode, "flag_field" );

      List<StreamInterface> infoStreams = getTransformIOMeta().getInfoStreams();
      StreamInterface referenceStream = infoStreams.get( 0 );
      StreamInterface compareStream = infoStreams.get( 1 );

      compareStream.setSubject( XMLHandler.getTagValue( transformNode, "compare" ) );
      referenceStream.setSubject( XMLHandler.getTagValue( transformNode, "reference" ) );
    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "MergeRowsMeta.Exception.UnableToLoadTransformMeta" ), e );
    }
  }

  @Override
  public void setDefault() {
    flagField = "flagfield";
    allocate( 0, 0 );
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    List<StreamInterface> infoStreams = getTransformIOMeta().getInfoStreams();
    for ( StreamInterface stream : infoStreams ) {
      stream.setTransformMeta( TransformMeta.findTransform( transforms, (String) stream.getSubject() ) );
    }
  }

  public boolean chosesTargetTransforms() {
    return false;
  }

  public String[] getTargetTransforms() {
    return null;
  }

  @Override
  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, TransformMeta nextTransform,
                         VariableSpace space, IMetaStore metaStore ) throws HopTransformException {
    // We don't have any input fields here in "r" as they are all info fields.
    // So we just merge in the info fields.
    //
    if ( info != null ) {
      boolean found = false;
      for ( int i = 0; i < info.length && !found; i++ ) {
        if ( info[ i ] != null ) {
          r.mergeRowMeta( info[ i ], name );
          found = true;
        }
      }
    }

    if ( Utils.isEmpty( flagField ) ) {
      throw new HopTransformException( BaseMessages.getString( PKG, "MergeRowsMeta.Exception.FlagFieldNotSpecified" ) );
    }
    ValueMetaInterface flagFieldValue = new ValueMetaString( flagField );
    flagFieldValue.setOrigin( name );
    r.addValueMeta( flagFieldValue );

  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    List<StreamInterface> infoStreams = getTransformIOMeta().getInfoStreams();
    StreamInterface referenceStream = infoStreams.get( 0 );
    StreamInterface compareStream = infoStreams.get( 1 );

    if ( referenceStream.getTransformName() != null && compareStream.getTransformName() != null ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MergeRowsMeta.CheckResult.SourceTransformsOK" ), transformMeta );
      remarks.add( cr );
    } else if ( referenceStream.getTransformName() == null && compareStream.getTransformName() == null ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "MergeRowsMeta.CheckResult.SourceTransformsMissing" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "MergeRowsMeta.CheckResult.OneSourceTransformMissing" ), transformMeta );
      remarks.add( cr );
    }

    RowMetaInterface referenceRowMeta = null;
    RowMetaInterface compareRowMeta = null;
    try {
      referenceRowMeta = pipelineMeta.getPrevTransformFields( referenceStream.getTransformName() );
      compareRowMeta = pipelineMeta.getPrevTransformFields( compareStream.getTransformName() );
    } catch ( HopTransformException kse ) {
      new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
        PKG, "MergeRowsMeta.CheckResult.ErrorGettingPrevTransformFields" ), transformMeta );
    }
    if ( referenceRowMeta != null && compareRowMeta != null ) {
      boolean rowsMatch = false;
      try {
        MergeRows.checkInputLayoutValid( referenceRowMeta, compareRowMeta );
        rowsMatch = true;
      } catch ( HopRowException kre ) {
        rowsMatch = false;
      }
      if ( rowsMatch ) {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "MergeRowsMeta.CheckResult.RowDefinitionMatch" ), transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "MergeRowsMeta.CheckResult.RowDefinitionNotMatch" ), transformMeta );
        remarks.add( cr );
      }
    }
  }

  @Override
  public TransformInterface getTransform( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new MergeRows( transformMeta, transformDataInterface, cnr, tr, pipeline );
  }

  @Override
  public TransformDataInterface getTransformData() {
    return new MergeRowsData();
  }

  /**
   * Returns the Input/Output metadata for this transform.
   */
  @Override
  public TransformIOMetaInterface getTransformIOMeta() {
    TransformIOMetaInterface ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new TransformIOMeta( true, true, false, false, false, false );

      ioMeta.addStream( new Stream( StreamType.INFO, null, BaseMessages.getString(
        PKG, "MergeRowsMeta.InfoStream.FirstStream.Description" ), StreamIcon.INFO, null ) );
      ioMeta.addStream( new Stream( StreamType.INFO, null, BaseMessages.getString(
        PKG, "MergeRowsMeta.InfoStream.SecondStream.Description" ), StreamIcon.INFO, null ) );
      setTransformIOMeta( ioMeta );
    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] { PipelineType.Normal, };
  }

}
