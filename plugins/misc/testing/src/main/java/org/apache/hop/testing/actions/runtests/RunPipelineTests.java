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

package org.apache.hop.testing.actions.runtests;

import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.testing.util.UnitTestUtil;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.pipeline.ActionPipeline;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Action(
    id = "RunPipelineTests",
    name = "i18n::RunPipelineTests.Name",
    description = "i18n::RunPipelineTests.Description",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    image = "Test_tube_icon.svg",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/runpipelinetests.html")
public class RunPipelineTests extends ActionBase implements IAction, Cloneable {

  public static final String TEST_NAMES = "test_names";
  public static final String TEST_NAME = "test_name";

   private List<String> testNames;

  public RunPipelineTests( String name, String description) {
    super(name, description);
    testNames = new ArrayList<>();
  }

  public RunPipelineTests() {
    this("", "");
  }

  @Override
  public RunPipelineTests clone() {
    return (RunPipelineTests) super.clone();
  }

  @Override
  public Result execute(Result prevResult, int nr) throws HopException {

    IHopMetadataSerializer<PipelineUnitTest> testSerializer =
        getMetadataProvider().getSerializer(PipelineUnitTest.class);

    AtomicBoolean success=new AtomicBoolean(true);

    for (String testName : testNames) {

      PipelineUnitTest test = testSerializer.load(testName);

      UnitTestUtil.executeUnitTest(
          test,
          this,
          getLogLevel(),
          getMetadataProvider(),
          this,
          // Something went wrong executing the pipeline itself.
          //
          (pipeline, result) -> {
            if (result.getNrErrors() > 0) {
              this.logError(
                  "There was an error running the pipeline for unit test '" + test.getName() + "'");
              success.set(false);
            }
          },
          // The pipeline ran fine so we can evaluate the test results
          (pipeline, testResults) -> {
            int errorCount = 0;
            for (UnitTestResult testResult : testResults) {
              if (testResult.isError()) {
                this.logError(
                    "Error in validating test data set '"
                        + testResult.getDataSetName()
                        + " : "
                        + testResult.getComment());
                errorCount++;
              }
            }
            if (errorCount > 0) {
              this.logError(
                  "There were test result evaluation errors in pipeline unit test '"
                      + test.getName());
              success.set(false);
            }
          },
          (test1, pipelineMeta, e) -> {
            this.logError(
                "There was an exception executing pipeline unit test '" + test.getName(), e);
            success.set(false);
          });
    }

    if (success.get()) {
      prevResult.setNrErrors(0);
      prevResult.setResult( true );
    } else {
      prevResult.setNrErrors(prevResult.getNrErrors() + 1);
      prevResult.setResult( false );
    }

    return prevResult;
  }


  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder();

    xml.append(super.getXml());


    xml.append(XmlHandler.openTag(TEST_NAMES));
    for (String testName : testNames) {
      xml.append(XmlHandler.openTag(TEST_NAME));
      xml.append(XmlHandler.addTagValue("name", testName));
      xml.append(XmlHandler.closeTag(TEST_NAME));
    }
    xml.append(XmlHandler.closeTag(TEST_NAMES));

    return xml.toString();
  }

  @Override
  public void loadXml(Node entryNode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    super.loadXml(entryNode);

    Node testNamesNode = XmlHandler.getSubNode(entryNode, TEST_NAMES);
    List<Node> testNameNodes = XmlHandler.getNodes(testNamesNode, TEST_NAME);
    testNames = new ArrayList<>();
    for (Node testNameNode : testNameNodes) {
      String name = XmlHandler.getTagValue(testNameNode, "name");
      testNames.add(name);
    }
  }

  @Override
  public String[] getReferencedObjectDescriptions() {
    String[] descriptions = new String[testNames.size()];
    for (int i=0;i<descriptions.length;i++) {
      descriptions[i] = "Pipeline of unit test : "+testNames.get(i);
    }
    return descriptions;
  }

  public boolean[] isReferencedObjectEnabled() {
    boolean[] enabled = new boolean[testNames.size()];
    for (int i=0;i<enabled.length;i++) {
      enabled[i] = true;
    }
    return enabled;
  }

  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {

    IHopMetadataSerializer<PipelineUnitTest> testSerializer = metadataProvider.getSerializer( PipelineUnitTest.class );
    String testName = testNames.get( index );
    PipelineUnitTest test = testSerializer.load( testName );
    return UnitTestUtil.loadTestPipeline( test, metadataProvider, variables );
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  /**
   * Gets testNames
   *
   * @return value of testNames
   */
  public List<String> getTestNames() {
    return testNames;
  }

  /** @param testNames The testNames to set */
  public void setTestNames(List<String> testNames) {
    this.testNames = testNames;
  }
}
