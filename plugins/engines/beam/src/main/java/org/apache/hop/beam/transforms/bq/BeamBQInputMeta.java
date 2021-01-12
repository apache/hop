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
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
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

import java.util.ArrayList;
import java.util.List;

@Transform(
        id = "BeamBQInput",
        name = "Beam BigQuery Input",
        description = "Reads from a BigQuery table in Beam",
        image = "beam-bq-input.svg",
        categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.BigData",
        documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/beambigqueryinput.html"
)
public class BeamBQInputMeta extends BaseTransformMeta implements ITransformMeta<Dummy, DummyData> {

  public static final String PROJECT_ID = "project_id";
  public static final String DATASET_ID = "dataset_id";
  public static final String TABLE_ID = "table_id";
  public static final String QUERY = "query";

  private String projectId;
  private String datasetId;
  private String tableId;
  private String query;

  private List<BQField> fields;

  public BeamBQInputMeta() {
    super();
    fields = new ArrayList<>();
  }

  @Override public void setDefault() {
  }

  @Override public Dummy createTransform( TransformMeta transformMeta, DummyData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new Dummy( transformMeta, new DummyMeta(), data, copyNr, pipelineMeta, pipeline );
  }

  @Override public DummyData getTransformData() {
    return new DummyData();
  }

  @Override public String getDialogClassName() {
    return BeamBQInputDialog.class.getName();
  }

  @Override public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform, IVariables variables, IHopMetadataProvider metadataProvider )
    throws HopTransformException {

    try {
      for ( BQField field : fields ) {
        int type = ValueMetaFactory.getIdForValueMeta( field.getHopType() );
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta( field.getNewNameOrName(), type, -1, -1 );
        valueMeta.setOrigin( name );
        inputRowMeta.addValueMeta( valueMeta );
      }
    } catch ( Exception e ) {
      throw new HopTransformException( "Error getting Beam BQ Input transform output", e );
    }
  }


  @Override public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();

    xml.append( XmlHandler.addTagValue( PROJECT_ID, projectId ) );
    xml.append( XmlHandler.addTagValue( DATASET_ID, datasetId ) );
    xml.append( XmlHandler.addTagValue( TABLE_ID, tableId ) );
    xml.append( XmlHandler.addTagValue( QUERY, query ) );

    xml.append( XmlHandler.openTag( "fields" ) );
    for ( BQField field : fields ) {
      xml.append( XmlHandler.openTag( "field" ) );
      xml.append( XmlHandler.addTagValue( "name", field.getName() ) );
      xml.append( XmlHandler.addTagValue( "new_name", field.getNewName() ) );
      xml.append( XmlHandler.addTagValue( "type", field.getHopType() ) );
      xml.append( XmlHandler.closeTag( "field" ) );
    }
    xml.append( XmlHandler.closeTag( "fields" ) );

    return xml.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {

    projectId = XmlHandler.getTagValue( transformNode, PROJECT_ID );
    datasetId = XmlHandler.getTagValue( transformNode, DATASET_ID );
    tableId = XmlHandler.getTagValue( transformNode, TABLE_ID );
    query = XmlHandler.getTagValue( transformNode, QUERY );

    Node fieldsNode = XmlHandler.getSubNode( transformNode, "fields" );
    List<Node> fieldNodes = XmlHandler.getNodes( fieldsNode, "field" );
    fields = new ArrayList<>();
    for ( Node fieldNode : fieldNodes ) {
      String name = XmlHandler.getTagValue( fieldNode, "name" );
      String newName = XmlHandler.getTagValue( fieldNode, "new_name" );
      String hopType = XmlHandler.getTagValue( fieldNode, "type" );
      fields.add( new BQField( name, newName, hopType ) );
    }
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
   * Gets query
   *
   * @return value of query
   */
  public String getQuery() {
    return query;
  }

  /**
   * @param query The query to set
   */
  public void setQuery( String query ) {
    this.query = query;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<BQField> getFields() {
    return fields;
  }

  /**
   * @param fields The fields to set
   */
  public void setFields( List<BQField> fields ) {
    this.fields = fields;
  }
}
