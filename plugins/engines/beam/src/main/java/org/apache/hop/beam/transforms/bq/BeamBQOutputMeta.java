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

package org.apache.hop.beam.transforms.bq;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.Dummy;
import org.apache.hop.pipeline.transforms.dummy.DummyData;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.w3c.dom.Node;

@Transform(
        id = "BeamBQOutput",
        image = "beam-bq-output.svg",
        name = "Beam BigQuery Output",
        description = "Writes to a BigQuery table in Beam",
        categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.BigData",
        documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/beambigqueryoutput.html"
)
public class BeamBQOutputMeta extends BaseTransformMeta implements ITransformMeta<Dummy, DummyData> {

  public static final String PROJECT_ID = "project_id";
  public static final String DATASET_ID = "dataset_id";
  public static final String TABLE_ID = "table_id";
  public static final String CREATE_IF_NEEDED = "create_if_needed";
  public static final String TRUNCATE_TABLE = "truncate_table";
  public static final String FAIL_IF_NOT_EMPTY = "fail_if_not_empty";

  private String projectId;
  private String datasetId;
  private String tableId;
  private boolean creatingIfNeeded;
  private boolean truncatingTable;
  private boolean failingIfNotEmpty;

  @Override public void setDefault() {
    creatingIfNeeded = true;
  }

  @Override public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform, IVariables variables, IHopMetadataProvider metadataProvider )
    throws HopTransformException {

    // This is an endpoint in Beam, produces no further output
    //
    inputRowMeta.clear();
  }

  @Override public Dummy createTransform( TransformMeta transformMeta, DummyData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new Dummy( transformMeta, new DummyMeta(), data, copyNr, pipelineMeta, pipeline );
  }

  @Override public DummyData getTransformData() {
    return new DummyData();
  }

  @Override public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append( XmlHandler.addTagValue( PROJECT_ID, projectId ) );
    xml.append( XmlHandler.addTagValue( DATASET_ID, datasetId ) );
    xml.append( XmlHandler.addTagValue( TABLE_ID, tableId ) );
    xml.append( XmlHandler.addTagValue( CREATE_IF_NEEDED, creatingIfNeeded ) );
    xml.append( XmlHandler.addTagValue( TRUNCATE_TABLE, truncatingTable ) );
    xml.append( XmlHandler.addTagValue( FAIL_IF_NOT_EMPTY, failingIfNotEmpty ) );
    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    projectId = XmlHandler.getTagValue( transformNode, PROJECT_ID );
    datasetId = XmlHandler.getTagValue( transformNode, DATASET_ID );
    tableId = XmlHandler.getTagValue( transformNode, TABLE_ID );
    creatingIfNeeded = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, CREATE_IF_NEEDED ) );
    truncatingTable = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, TRUNCATE_TABLE ) );
    failingIfNotEmpty = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, FAIL_IF_NOT_EMPTY ) );
  }

  /**
   * Gets projectId
   *
   * @return value of projectId
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * @param projectId The projectId to set
   */
  public void setProjectId( String projectId ) {
    this.projectId = projectId;
  }

  /**
   * Gets datasetId
   *
   * @return value of datasetId
   */
  public String getDatasetId() {
    return datasetId;
  }

  /**
   * @param datasetId The datasetId to set
   */
  public void setDatasetId( String datasetId ) {
    this.datasetId = datasetId;
  }

  /**
   * Gets tableId
   *
   * @return value of tableId
   */
  public String getTableId() {
    return tableId;
  }

  /**
   * @param tableId The tableId to set
   */
  public void setTableId( String tableId ) {
    this.tableId = tableId;
  }

  /**
   * Gets creatingIfNeeded
   *
   * @return value of creatingIfNeeded
   */
  public boolean isCreatingIfNeeded() {
    return creatingIfNeeded;
  }

  /**
   * @param creatingIfNeeded The creatingIfNeeded to set
   */
  public void setCreatingIfNeeded( boolean creatingIfNeeded ) {
    this.creatingIfNeeded = creatingIfNeeded;
  }

  /**
   * Gets truncatingTable
   *
   * @return value of truncatingTable
   */
  public boolean isTruncatingTable() {
    return truncatingTable;
  }

  /**
   * @param truncatingTable The truncatingTable to set
   */
  public void setTruncatingTable( boolean truncatingTable ) {
    this.truncatingTable = truncatingTable;
  }

  /**
   * Gets failingIfNotEmpty
   *
   * @return value of failingIfNotEmpty
   */
  public boolean isFailingIfNotEmpty() {
    return failingIfNotEmpty;
  }

  /**
   * @param failingIfNotEmpty The failingIfNotEmpty to set
   */
  public void setFailingIfNotEmpty( boolean failingIfNotEmpty ) {
    this.failingIfNotEmpty = failingIfNotEmpty;
  }
}
