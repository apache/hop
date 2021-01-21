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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.UnitTestUtil;

import java.util.ArrayList;

public class ExecuteTests extends BaseTransform<ExecuteTestsMeta, ExecuteTestsData>
    implements ITransform<ExecuteTestsMeta, ExecuteTestsData> {

  public ExecuteTests(
      TransformMeta transformMeta,
      ExecuteTestsMeta meta,
      ExecuteTestsData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    try {
      data.hasPrevious = false;
      TransformMeta[] prevTransforms = getPipelineMeta().getPrevTransforms(getTransformMeta());
      if (prevTransforms.length > 0) {
        data.hasPrevious = true;

        if (StringUtils.isEmpty(meta.getTestNameInputField())) {
          log.logError(
              "When this transform receives input it wants the name of a field to get the unit test name from to determine which transforms to execute");
          setErrors(1);
          return false;
        }
      }
    } catch (Exception e) {
      log.logError("Error analyzing ", e);
      setErrors(1);
      return false;
    }

    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {

    if (first) {
      first = false;

      IHopMetadataSerializer<PipelineUnitTest> testSerializer =
          metadataProvider.getSerializer(PipelineUnitTest.class);

      // Read all the unit test names from the previous transform(s)
      //
      if (data.hasPrevious) {

        data.tests = new ArrayList<>();
        Object[] row = getRow();

        if (row == null) {
          // No input and as such no tests to execute. We're all done here.
          //
          setOutputDone();
          return false;
        }

        int inputFieldIndex = getInputRowMeta().indexOfValue(meta.getTestNameInputField());
        if (inputFieldIndex < 0) {
          throw new HopException(
              "Unable to find test name field '" + meta.getTestNameInputField() + "' in the input");
        }

        while (row != null) {
          String testName = getInputRowMeta().getString(row, inputFieldIndex);
          try {
            PipelineUnitTest pipelineUnitTest = testSerializer.load(testName);

            data.tests.add(pipelineUnitTest);
          } catch (Exception e) {
            throw new HopException("Unable to load test '" + testName + "'", e);
          }
          row = getRow();
        }
      } else {
        // Get all the unit tests from the meta store
        // Read them all, filter by type later below...
        //
        try {
          data.tests = new ArrayList<>();
          for (String testName : testSerializer.listObjectNames()) {
            PipelineUnitTest pipelineUnitTest = testSerializer.load(testName);
            if (meta.getTypeToExecute() == null
                || meta.getTypeToExecute() == pipelineUnitTest.getType()) {
              data.tests.add(pipelineUnitTest);
            }
          }
        } catch (HopException e) {
          throw new HopException("Unable to read pipeline unit tests from the metadata", e);
        }
      }

      data.testsIterator = data.tests.iterator();
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    // Execute one test per iteration.
    //
    if (data.testsIterator.hasNext()) {
      PipelineUnitTest test = data.testsIterator.next();

      UnitTestUtil.executeUnitTest(
          test,
          this,
          getLogLevel(),
          metadataProvider,
          this,
          // Evaluate the execution the pipeline itself.  Was there an error?
          //
          (pipeline, pipelineResult) -> {
            if (pipelineResult.getNrErrors() != 0) {
              // The pipeline had a failure, report this too.
              //
              Object[] row = RowDataUtil.allocateRowData(data.outputRowMeta.size());
              int index = 0;
              row[index++] = pipeline.getPipelineMeta().getName();
              row[index++] = null;
              row[index++] = null;
              row[index++] = null;
              row[index++] = Boolean.TRUE;
              row[index++] = pipelineResult.getLogText();

              ExecuteTests.this.putRow(data.outputRowMeta, row);
            }
          },
          // Evaluate the test results after a successful execution
          //
          (pipeline, testResults) -> {
            for (UnitTestResult testResult : testResults) {
              Object[] row = RowDataUtil.allocateRowData(data.outputRowMeta.size());
              int index = 0;
              row[index++] = testResult.getPipelineName();
              row[index++] = testResult.getUnitTestName();
              row[index++] = testResult.getDataSetName();
              row[index++] = testResult.getTransformName();
              row[index++] = testResult.isError();
              row[index++] = testResult.getComment();

              putRow(data.outputRowMeta, row);
            }
          },
          // If there was an exception due to a configuration error...
          //
          (test1, testPipelineMeta, e) -> {
            // Some configuration or setup error...
            //
            Object[] row = RowDataUtil.allocateRowData(data.outputRowMeta.size());
            int index = 0;
            row[index++] = testPipelineMeta == null ? null : testPipelineMeta.getName();
            row[index++] = test1.getName();
            row[index++] = null;
            row[index++] = null;
            row[index++] = Boolean.TRUE; // ERROR!
            row[index++] = e.getMessage() + " : " + Const.getStackTracker(e);

            ExecuteTests.this.putRow(data.outputRowMeta, row);
          });

      return true;
    } else {
      setOutputDone();
      return false;
    }
  }

  private PipelineMeta loadTestPipeline(PipelineUnitTest test) throws HopException {
    PipelineMeta unitTestPipelineMeta = null;
    // Environment substitution is not yet supported in the UI
    //
    String filename = test.calculateCompletePipelineFilename(this);
    if (StringUtils.isNotEmpty(filename)) {
      unitTestPipelineMeta = new PipelineMeta(filename, metadataProvider, true, this);
    }
    if (unitTestPipelineMeta == null) {
      throw new HopException(
          "Unable to find a valid pipeline filename in unit test '" + test.getName() + "'");
    }

    // Pass some data from the parent...
    //
    unitTestPipelineMeta.setMetadataProvider(metadataProvider);

    // clear and load attributes for unit test...
    //
    TestingGuiPlugin.selectUnitTest(unitTestPipelineMeta, test);

    return unitTestPipelineMeta;
  }
}
