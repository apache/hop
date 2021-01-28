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

package org.apache.hop.pipeline.transforms.metastructure;

import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
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
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.w3c.dom.Node;

@Transform(
  id = "TransformMetaStructure",
  name = "i18n::TransformMetaStructure.Transform.Name",
  description = "i18n::TransformMetaStructure.Transform.Description",
  categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
  image = "MetaStructure.svg"
)
public class TransformMetaStructureMeta extends BaseTransformMeta implements ITransformMeta<TransformMetaStructure, TransformMetaStructureData> {

  private static Class<?> PKG = TransformMetaStructureMeta.class; // for i18n purposes, needed by Translator2!!

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
  public Object clone() {
    Object clone = super.clone();
    return clone;
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder( 500 );

    xml.append( "      " ).append( XmlHandler.addTagValue( "outputRowcount", outputRowcount ) );
    xml.append( "    " ).append( XmlHandler.addTagValue( "rowcountField", rowcountField ) );

    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {
      outputRowcount = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "outputRowcount" ) );
      rowcountField = XmlHandler.getTagValue( transformNode, "rowcountField" );
    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load transform info from Xml", e );
    }
  }


  @Override public ITransform createTransform( TransformMeta transformMeta, TransformMetaStructureData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new TransformMetaStructure( transformMeta, this, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public TransformMetaStructureData getTransformData() {
    return new TransformMetaStructureData();
  }

  @Override public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                               IHopMetadataProvider metadataProvider ) {
    CheckResult cr;
    cr = new CheckResult( ICheckResult.TYPE_RESULT_OK, "Not implemented", transformMeta );
    remarks.add( cr );

  }

  @Override public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform, IVariables variables, IHopMetadataProvider metadataProvider )
    throws HopTransformException {
    // we create a new output row structure - clear r
    inputRowMeta.clear();

    this.setDefault();
    // create the new fields
    // Position
    IValueMeta positionFieldValue = new ValueMetaInteger( positionName );
    positionFieldValue.setOrigin( name );
    inputRowMeta.addValueMeta( positionFieldValue );
    // field name
    IValueMeta nameFieldValue = new ValueMetaString( fieldName );
    nameFieldValue.setOrigin( name );
    inputRowMeta.addValueMeta( nameFieldValue );
    // comments
    IValueMeta commentsFieldValue = new ValueMetaString( comments );
    nameFieldValue.setOrigin( name );
    inputRowMeta.addValueMeta( commentsFieldValue );
    // Type
    IValueMeta typeFieldValue = new ValueMetaString( typeName );
    typeFieldValue.setOrigin( name );
    inputRowMeta.addValueMeta( typeFieldValue );
    // Length
    IValueMeta lengthFieldValue = new ValueMetaInteger( lengthName );
    lengthFieldValue.setOrigin( name );
    inputRowMeta.addValueMeta( lengthFieldValue );
    // Precision
    IValueMeta precisionFieldValue = new ValueMetaInteger( precisionName );
    precisionFieldValue.setOrigin( name );
    inputRowMeta.addValueMeta( precisionFieldValue );
    // Origin
    IValueMeta originFieldValue = new ValueMetaString( originName );
    originFieldValue.setOrigin( name );
    inputRowMeta.addValueMeta( originFieldValue );

    if ( isOutputRowcount() ) {
      // RowCount
      IValueMeta v = new ValueMetaInteger( this.getRowcountField() );
      v.setOrigin( name );
      inputRowMeta.addValueMeta( v );
    }

  }

  @Override
  public void setDefault() {
    positionName = BaseMessages.getString( PKG, "TransformMetaStructureMeta.PositionName" );
    fieldName = BaseMessages.getString( PKG, "TransformMetaStructureMeta.FieldName" );
    comments = BaseMessages.getString( PKG, "TransformMetaStructureMeta.Comments" );
    typeName = BaseMessages.getString( PKG, "TransformMetaStructureMeta.TypeName" );
    lengthName = BaseMessages.getString( PKG, "TransformMetaStructureMeta.LengthName" );
    precisionName = BaseMessages.getString( PKG, "TransformMetaStructureMeta.PrecisionName" );
    originName = BaseMessages.getString( PKG, "TransformMetaStructureMeta.OriginName" );

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
