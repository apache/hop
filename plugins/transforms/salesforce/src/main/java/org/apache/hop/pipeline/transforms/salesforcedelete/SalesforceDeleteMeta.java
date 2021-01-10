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

package org.apache.hop.pipeline.transforms.salesforcedelete;

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
  id = "SalesforceDelete",
  name = "i18n::SalesforceDelete.TypeLongDesc.SalesforceDelete",
  description = "i18n::SalesforceDelete.TypeTooltipDesc.SalesforceDelete",
  categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
  image = "SFD.svg",
  documentationUrl = "Products/Salesforce_Delete" )
public class SalesforceDeleteMeta extends SalesforceTransformMeta<SalesforceDelete, SalesforceDeleteData> {
  private static Class<?> PKG = SalesforceDeleteMeta.class; // For Translator

  /**
   * Deletefield
   */
  private String DeleteField;

  /**
   * Batch size
   */
  private String batchSize;

  private boolean rollbackAllChangesOnError;

  public SalesforceDeleteMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the rollbackAllChangesOnError.
   */
  public boolean isRollbackAllChangesOnError() {
    return rollbackAllChangesOnError;
  }

  /**
   * @param rollbackAllChangesOnError The rollbackAllChangesOnError to set.
   */
  public void setRollbackAllChangesOnError( boolean rollbackAllChangesOnError ) {
    this.rollbackAllChangesOnError = rollbackAllChangesOnError;
  }

  /**
   * @param DeleteField The DeleteField to set.
   */
  public void setDeleteField( String DeleteField ) {
    this.DeleteField = DeleteField;
  }

  /**
   * @return Returns the DeleteField.
   */
  public String getDeleteField() {
    return this.DeleteField;
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
    SalesforceDeleteMeta retval = (SalesforceDeleteMeta) super.clone();

    return retval;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder( super.getXml() );
    retval.append( "    " + XmlHandler.addTagValue( "DeleteField", getDeleteField() ) );
    retval.append( "    " + XmlHandler.addTagValue( "batchSize", getBatchSize() ) );
    retval.append( "    " + XmlHandler.addTagValue( "rollbackAllChangesOnError", isRollbackAllChangesOnError() ) );

    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXmlException {
    try {
      setDeleteField( XmlHandler.getTagValue( transformNode, "DeleteField" ) );

      setBatchSize( XmlHandler.getTagValue( transformNode, "batchSize" ) );
      setRollbackAllChangesOnError(
        "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "rollbackAllChangesOnError" ) ) );
    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load transform info from XML", e );
    }
  }

  public void setDefault() {
    super.setDefault();
    setModule( "Account" );
    setDeleteField( null );
    setBatchSize( "10" );
    setRollbackAllChangesOnError( false );
  }

  /* This function adds meta data to the rows being pushed out */
  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {

  }

  @Override public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta,
                               TransformMeta transformMeta, IRowMeta prev,
                               String[] input, String[] output, IRowMeta info,
                               IVariables variables,
                               IHopMetadataProvider metadataProvider ) {
    super.check( remarks, pipelineMeta, transformMeta, prev, input, output, info, variables, metadataProvider );
    CheckResult cr;

    // See if we get input...
    if ( input != null && input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SalesforceDeleteMeta.CheckResult.NoInputExpected" ), transformMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SalesforceDeleteMeta.CheckResult.NoInput" ), transformMeta );
    }
    remarks.add( cr );
  }

  @Override public ITransform createTransform( TransformMeta transformMeta, SalesforceDeleteData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
     return new SalesforceDelete( transformMeta, this, data, copyNr, pipelineMeta, pipeline );
  }

  @Override public SalesforceDeleteData getTransformData() {
    return new SalesforceDeleteData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

}
