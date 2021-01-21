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

package org.apache.hop.testing.transforms.exectests;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.TestType;
import org.apache.hop.testing.UnitTestResult;
import org.w3c.dom.Node;

@Transform(
  id = "ExecuteTests",
  description = "Execute Unit Tests",
  name = "Execute Unit Tests",
  image = "executetests.svg",
  categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow"
)
public class ExecuteTestsMeta extends BaseTransformMeta implements ITransformMeta<ExecuteTests, ExecuteTestsData> {

  public static final String TAG_TEST_NAME_INPUT_FIELD = "test_name_input_field";
  public static final String TAG_TYPE_TO_EXECUTE = "type_to_execute";
  public static final String TAG_PIPELINE_NAME_FIELD = "pipeline_name_field";
  public static final String TAG_UNIT_TEST_NAME_FIELD = "unit_test_name_field";
  public static final String TAG_DATASET_NAME_FIELD = "data_set_name_field";
  public static final String TAG_TRANSFORM_NAME_FIELD = "transform_name_field";
  public static final String TAG_ERROR_FIELD = "error_field";
  public static final String TAG_COMMENT_FIELD = "comment_field";

  private String testNameInputField;
  private TestType typeToExecute;
  private String pipelineNameField;
  private String unitTestNameField;
  private String dataSetNameField;
  private String transformNameField;
  private String errorField;
  private String commentField;

  public ExecuteTestsMeta() {
    super();
  }

  @Override public void getFields( IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextTransform,
                                   IVariables variables, IHopMetadataProvider metadataProvider ) {
    IRowMeta rowMeta = UnitTestResult.getRowMeta();
    int index = 0;
    rowMeta.getValueMeta( index++ ).setName( variables.resolve( pipelineNameField ) );
    rowMeta.getValueMeta( index++ ).setName( variables.resolve( unitTestNameField ) );
    rowMeta.getValueMeta( index++ ).setName( variables.resolve( dataSetNameField ) );
    rowMeta.getValueMeta( index++ ).setName( variables.resolve( transformNameField ) );
    rowMeta.getValueMeta( index++ ).setName( variables.resolve( errorField ) );
    rowMeta.getValueMeta( index++ ).setName( variables.resolve( commentField ) );

    inputRowMeta.clear();
    inputRowMeta.addRowMeta( rowMeta );
  }

  @Override
  public String getXml() throws HopException {
    StringBuilder xml = new StringBuilder();
    xml.append( XmlHandler.addTagValue( TAG_TEST_NAME_INPUT_FIELD, testNameInputField ) );
    xml.append( XmlHandler.addTagValue( TAG_TYPE_TO_EXECUTE, typeToExecute == null ? TestType.DEVELOPMENT.name() : typeToExecute.name() ) );
    xml.append( XmlHandler.addTagValue( TAG_PIPELINE_NAME_FIELD, pipelineNameField ) );
    xml.append( XmlHandler.addTagValue( TAG_UNIT_TEST_NAME_FIELD, unitTestNameField ) );
    xml.append( XmlHandler.addTagValue( TAG_DATASET_NAME_FIELD, dataSetNameField ) );
    xml.append( XmlHandler.addTagValue( TAG_TRANSFORM_NAME_FIELD, transformNameField ) );
    xml.append( XmlHandler.addTagValue( TAG_ERROR_FIELD, errorField ) );
    xml.append( XmlHandler.addTagValue( TAG_COMMENT_FIELD, commentField ) );

    return xml.toString();
  }

  @Override
  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    try {

      testNameInputField = XmlHandler.getTagValue( transformNode, TAG_TEST_NAME_INPUT_FIELD );
      String typeDesc = XmlHandler.getTagValue( transformNode, TAG_TYPE_TO_EXECUTE );
      try {
        typeToExecute = TestType.valueOf( typeDesc );
      } catch ( Exception e ) {
        typeToExecute = TestType.DEVELOPMENT;
      }
      pipelineNameField = XmlHandler.getTagValue( transformNode, TAG_PIPELINE_NAME_FIELD );
      unitTestNameField = XmlHandler.getTagValue( transformNode, TAG_UNIT_TEST_NAME_FIELD );
      dataSetNameField = XmlHandler.getTagValue( transformNode, TAG_DATASET_NAME_FIELD );
      transformNameField = XmlHandler.getTagValue( transformNode, TAG_TRANSFORM_NAME_FIELD );
      errorField = XmlHandler.getTagValue( transformNode, TAG_ERROR_FIELD );
      commentField = XmlHandler.getTagValue( transformNode, TAG_COMMENT_FIELD );

    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load execute test transform details", e );
    }
  }

  @Override public ITransform createTransform( TransformMeta transformMeta, ExecuteTestsData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new ExecuteTests( transformMeta, this, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public ExecuteTestsData getTransformData() {
    return new ExecuteTestsData();
  }

  @Override
  public String getDialogClassName() {
    return ExecuteTestsDialog.class.getName();
  }

  @Override
  public void setDefault() {
    testNameInputField = null;
    pipelineNameField = "pipeline";
    unitTestNameField = "unittest";
    dataSetNameField = "dataset";
    transformNameField = "transform";
    errorField = "error";
    commentField = "comment";

  }


  /**
   * Gets testNameInputField
   *
   * @return value of testNameInputField
   */
  public String getTestNameInputField() {
    return testNameInputField;
  }

  /**
   * @param testNameInputField The testNameInputField to set
   */
  public void setTestNameInputField( String testNameInputField ) {
    this.testNameInputField = testNameInputField;
  }

  /**
   * Gets typeToExecute
   *
   * @return value of typeToExecute
   */
  public TestType getTypeToExecute() {
    return typeToExecute;
  }

  /**
   * @param typeToExecute The typeToExecute to set
   */
  public void setTypeToExecute( TestType typeToExecute ) {
    this.typeToExecute = typeToExecute;
  }

  /**
   * Gets pipelineNameField
   *
   * @return value of pipelineNameField
   */
  public String getPipelineNameField() {
    return pipelineNameField;
  }

  /**
   * @param pipelineNameField The pipelineNameField to set
   */
  public void setPipelineNameField( String pipelineNameField ) {
    this.pipelineNameField = pipelineNameField;
  }

  /**
   * Gets unitTestNameField
   *
   * @return value of unitTestNameField
   */
  public String getUnitTestNameField() {
    return unitTestNameField;
  }

  /**
   * @param unitTestNameField The unitTestNameField to set
   */
  public void setUnitTestNameField( String unitTestNameField ) {
    this.unitTestNameField = unitTestNameField;
  }

  /**
   * Gets dataSetNameField
   *
   * @return value of dataSetNameField
   */
  public String getDataSetNameField() {
    return dataSetNameField;
  }

  /**
   * @param dataSetNameField The dataSetNameField to set
   */
  public void setDataSetNameField( String dataSetNameField ) {
    this.dataSetNameField = dataSetNameField;
  }

  /**
   * Gets transform name field
   *
   * @return value of transform name field
   */
  public String getTransformNameField() {
    return transformNameField;
  }

  /**
   * @param transformNameField The transform name field to set
   */
  public void setTransformNameField( String transformNameField ) {
    this.transformNameField = transformNameField;
  }

  /**
   * Gets errorField
   *
   * @return value of errorField
   */
  public String getErrorField() {
    return errorField;
  }

  /**
   * @param errorField The errorField to set
   */
  public void setErrorField( String errorField ) {
    this.errorField = errorField;
  }

  /**
   * Gets commentField
   *
   * @return value of commentField
   */
  public String getCommentField() {
    return commentField;
  }

  /**
   * @param commentField The commentField to set
   */
  public void setCommentField( String commentField ) {
    this.commentField = commentField;
  }
}
