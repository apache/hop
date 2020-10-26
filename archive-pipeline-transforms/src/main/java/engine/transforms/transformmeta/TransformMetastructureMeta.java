/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.transformmeta;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

public class TransformMetastructureMeta extends BaseTransformMeta implements ITransform {

  private static final Class<?> PKG = TransformMetastructureMeta.class; // for i18n purposes, needed by Translator!!

  private String fieldName;
  private String comments;
  private String typeName;
  private String positionName;
  private String lengthName;
  private String precisionName;
  private String originName;

  private boolean outputRowcount;
  private String rowcountField;

  @Override
  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    readData( transformNode );
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "      " ).append( XmlHandler.addTagValue( "outputRowcount", outputRowcount ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "rowcountField", rowcountField ) );

    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      outputRowcount = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "outputRowcount" ) );
      rowcountField = XmlHandler.getTagValue( transformNode, "rowcountField" );
    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load transform info from XML", e );
    }
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new TransformMetastructure( transformMeta, this, data, cnr, tr, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new TransformMetastructureData();
  }

  @Override
  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IHopMetadataProvider metadataProvider ) {
    CheckResult cr;

    cr = new CheckResult( ICheckResult.TYPE_RESULT_OK, "Not implemented", transformMeta );
    remarks.add( cr );

  }

  @Override
  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {
    // we create a new output row structure - clear r
    r.clear();

    this.setDefault();
    // create the new fields
    // Position
    IValueMeta positionFieldValue = new ValueMetaInteger( positionName );
    positionFieldValue.setOrigin( name );
    r.addValueMeta( positionFieldValue );
    // field name
    IValueMeta nameFieldValue = new ValueMetaString( fieldName );
    nameFieldValue.setOrigin( name );
    r.addValueMeta( nameFieldValue );
    // comments
    IValueMeta commentsFieldValue = new ValueMetaString( comments );
    nameFieldValue.setOrigin( name );
    r.addValueMeta( commentsFieldValue );
    // Type
    IValueMeta typeFieldValue = new ValueMetaString( typeName );
    typeFieldValue.setOrigin( name );
    r.addValueMeta( typeFieldValue );
    // Length
    IValueMeta lengthFieldValue = new ValueMetaInteger( lengthName );
    lengthFieldValue.setOrigin( name );
    r.addValueMeta( lengthFieldValue );
    // Precision
    IValueMeta precisionFieldValue = new ValueMetaInteger( precisionName );
    precisionFieldValue.setOrigin( name );
    r.addValueMeta( precisionFieldValue );
    // Origin
    IValueMeta originFieldValue = new ValueMetaString( originName );
    originFieldValue.setOrigin( name );
    r.addValueMeta( originFieldValue );

    if ( isOutputRowcount() ) {
      // RowCount
      IValueMeta v = new ValueMetaInteger( this.getRowcountField() );
      v.setOrigin( name );
      r.addValueMeta( v );
    }

  }

  @Override
  public void setDefault() {
    positionName = BaseMessages.getString( PKG, "TransformMetastructureMeta.PositionName" );
    fieldName = BaseMessages.getString( PKG, "TransformMetastructureMeta.FieldName" );
    comments = BaseMessages.getString( PKG, "TransformMetastructureMeta.Comments" );
    typeName = BaseMessages.getString( PKG, "TransformMetastructureMeta.TypeName" );
    lengthName = BaseMessages.getString( PKG, "TransformMetastructureMeta.LengthName" );
    precisionName = BaseMessages.getString( PKG, "TransformMetastructureMeta.PrecisionName" );
    originName = BaseMessages.getString( PKG, "TransformMetastructureMeta.OriginName" );

  }

  public boolean isOutputRowcount() {
    return outputRowcount;
  }

  public void setOutputRowcount( boolean outputRowcount ) {
    this.outputRowcount = outputRowcount;
  }

  public String getRowcountField() {
    return rowcountField;
  }

  public void setRowcountField( String rowcountField ) {
    this.rowcountField = rowcountField;
  }

}
