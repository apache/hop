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

package org.apache.hop.pipeline.transforms.salesforceupdate;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceTransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "SalesforceUpdate",
    i18nPackageName = "org.apache.hop.pipeline.transforms.salesforceupdate",
    name = "SalesforceUpdate.TypeLongDesc.SalesforceUpdate",
    description = "SalesforceUpdate.TypeTooltipDesc.SalesforceUpdate",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    image = "SFUD.svg",
    documentationUrl = "Products/Salesforce_Update" )
public class SalesforceUpdateMeta extends SalesforceTransformMeta<SalesforceUpdate,SalesforceUpdateData> {
  private static Class<?> PKG = SalesforceUpdateMeta.class; // for i18n purposes, needed by Translator2!!

  /** Field value to update */
  private String[] updateLookup;

  /** boolean indicating if field uses External id */
  private Boolean[] useExternalId;

  /** Stream name to update value with */
  private String[] updateStream;

  /** Batch size */
  private String batchSize;

  private boolean rollbackAllChangesOnError;

  public SalesforceUpdateMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the rollbackAllChangesOnError.
   */
  public boolean isRollbackAllChangesOnError() {
    return rollbackAllChangesOnError;
  }

  /**
   * @param rollbackAllChangesOnError
   *          The rollbackAllChangesOnError to set.
   */
  public void setRollbackAllChangesOnError( boolean rollbackAllChangesOnError ) {
    this.rollbackAllChangesOnError = rollbackAllChangesOnError;
  }

  /**
   * @return Returns the updateLookup.
   */
  public String[] getUpdateLookup() {
    return updateLookup;
  }

  /**
   * @param updateLookup
   *          The updateLookup to set.
   */
  public void setUpdateLookup( String[] updateLookup ) {
    this.updateLookup = updateLookup;
  }

  /**
   * @return Returns the useExternalId.
   */
  public Boolean[] getUseExternalId() {
    return useExternalId;
  }

  /**
   * @param useExternalId
   *          The useExternalId to set.
   */
  public void setUseExternalId( Boolean[] useExternalId ) {
    this.useExternalId = useExternalId;
  }

  /**
   * @return Returns the updateStream.
   */
  public String[] getUpdateStream() {
    return updateStream;
  }

  /**
   * @param updateStream
   *          The updateStream to set.
   */
  public void setUpdateStream( String[] updateStream ) {
    this.updateStream = updateStream;
  }

  /**
   * @param batchSize
   */
  public void setBatchSize( String batchSize ) {
    this.batchSize = batchSize;
  }

  /**
   * @return Returns the batchSize.
   */
  public String getBatchSize() {
    return this.batchSize;
  }

  public int getBatchSizeInt() {
    return Const.toInt( this.batchSize, 10 );
  }

  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    super.loadXml( transformNode, metadataProvider );
    readData( transformNode );
  }

  public Object clone() {
    SalesforceUpdateMeta retval = (SalesforceUpdateMeta) super.clone();

    int nrvalues = updateLookup.length;

    retval.allocate( nrvalues );

    for ( int i = 0; i < nrvalues; i++ ) {
      retval.updateLookup[i] = updateLookup[i];
      retval.updateStream[i] = updateStream[i];
      retval.useExternalId[i] = useExternalId[i];
    }

    return retval;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( super.getXml() );
    retval.append( "    " + XmlHandler.addTagValue( "batchSize", getBatchSize() ) );

    retval.append( "    <fields>" + Const.CR );

    for ( int i = 0; i < updateLookup.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XmlHandler.addTagValue( "name", getUpdateLookup()[i] ) );
      retval.append( "        " ).append( XmlHandler.addTagValue( "field", getUpdateStream()[i] ) );
      retval.append( "        " ).append(
        XmlHandler.addTagValue( "useExternalId", getUseExternalId()[i].booleanValue() ) );
      retval.append( "      </field>" ).append( Const.CR );
    }

    retval.append( "      </fields>" + Const.CR );
    retval.append( "    " + XmlHandler.addTagValue( "rollbackAllChangesOnError", isRollbackAllChangesOnError() ) );
    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      setBatchSize( XmlHandler.getTagValue( transformNode, "batchSize" ) );

      Node fields = XmlHandler.getSubNode( transformNode, "fields" );
      int nrFields = XmlHandler.countNodes( fields, "field" );

      allocate( nrFields );

      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, "field", i );

        updateLookup[i] = XmlHandler.getTagValue( fnode, "name" );
        updateStream[i] = XmlHandler.getTagValue( fnode, "field" );
        if ( updateStream[i] == null ) {
          updateStream[i] = updateLookup[i]; // default: the same name!
        }
        String updateValue = XmlHandler.getTagValue( fnode, "useExternalId" );
        if ( updateValue == null ) {
          // default FALSE
          useExternalId[i] = Boolean.FALSE;
        } else {
          if ( updateValue.equalsIgnoreCase( "Y" ) ) {
            useExternalId[i] = Boolean.TRUE;
          } else {
            useExternalId[i] = Boolean.FALSE;
          }
        }

      }
      setRollbackAllChangesOnError(
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "rollbackAllChangesOnError" ) ) );
    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load step info from XML", e );
    }
  }

  public void allocate( int nrvalues ) {
    setUpdateLookup( new String[nrvalues] );
    setUpdateStream( new String[nrvalues] );
    setUseExternalId( new Boolean[nrvalues] );
  }

  public void setDefault() {
    super.setDefault();
    setBatchSize( "10" );

    allocate( 0 );

    setRollbackAllChangesOnError( false );
  }

  /* This function adds meta data to the rows being pushed out */
  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
    IVariables space,  IHopMetadataProvider metadataProvider ) throws HopTransformException {

  }

  @Override public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                               IHopMetadataProvider metadataProvider ) {
    super.check( remarks, pipelineMeta, transformMeta, prev, input, output, info, variables, metadataProvider );

    CheckResult cr;

    // See if we get input...
    if ( input != null && input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SalesforceUpdateMeta.CheckResult.NoInputExpected" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SalesforceUpdateMeta.CheckResult.NoInput" ), transformMeta );
    }
    remarks.add( cr );

    // check return fields
    if ( getUpdateLookup().length == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SalesforceUpdateMeta.CheckResult.NoFields" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SalesforceUpdateMeta.CheckResult.FieldsOk" ), transformMeta );
    }
    remarks.add( cr );
  }

  @Override public ITransform createTransform( TransformMeta transformMeta, SalesforceUpdateData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new SalesforceUpdate( transformMeta, this, data, copyNr, pipelineMeta, pipeline );
  }

  public SalesforceUpdateData getTransformData() {
    return new SalesforceUpdateData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

}
