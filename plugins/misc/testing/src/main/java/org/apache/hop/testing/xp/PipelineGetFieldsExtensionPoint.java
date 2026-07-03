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

package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.DataSetConst;

@ExtensionPoint(
    extensionPointId = "GetFieldsExtension",
    id = "PipelineGetFieldsExtensionPoint",
    description =
        "Intercept field determination for transforms that have a unit test input dataset mapped to them")
public class PipelineGetFieldsExtensionPoint
    implements IExtensionPoint<PipelineMeta.GetFieldsExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, PipelineMeta.GetFieldsExtension extension)
      throws HopException {
    PipelineMeta pipelineMeta = extension.getPipelineMeta();
    TransformMeta transformMeta = extension.getTransformMeta();

    if (pipelineMeta == null || transformMeta == null) {
      return;
    }

    // Check if a unit test is active in Hop GUI
    PipelineUnitTest unitTest = TestingGuiPlugin.getCurrentUnitTest(pipelineMeta);
    if (unitTest == null) {
      return;
    }

    // Check if this transform has an input location mapping
    PipelineUnitTestSetLocation inputLocation = unitTest.findInputLocation(transformMeta.getName());
    if (inputLocation == null) {
      return;
    }

    String dataSetName = inputLocation.getDataSetName();
    if (dataSetName == null) {
      return;
    }

    // Load dataset and get its fields
    DataSet dataSet;
    try {
      dataSet = extension.getMetadataProvider().getSerializer(DataSet.class).load(dataSetName);
    } catch (HopException e) {
      throw new HopException("Unable to load data set '" + dataSetName + "'");
    }

    if (dataSet != null) {
      IRowMeta outputRowMeta = DataSetConst.getTransformOutputFields(dataSet, inputLocation);
      if (outputRowMeta != null) {
        IRowMeta row = extension.getRow();
        for (int i = 0; i < outputRowMeta.size(); i++) {
          IValueMeta v = outputRowMeta.getValueMeta(i);
          v.setOrigin(extension.getName());
        }
        row.addRowMeta(outputRowMeta);
        extension.setHandled(true);
      }
    }
  }
}
