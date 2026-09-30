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

package org.apache.hop.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.Test;

class DataSetDefaultsTest {

  @Test
  void suggestsNameFolderAndFilenameFromPipelineAndTransform() throws Exception {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, "${PROJECT_HOME}/datasets");

    DataSet dataSet = new DataSet();
    DataSetDefaults.apply(
        dataSet, "${PROJECT_HOME}/pipelines/load-orders.hpl", "Table input", variables, null);

    assertEquals("ds-load-orders-Table input", dataSet.getName());
    assertEquals("${HOP_DATASETS_FOLDER}", dataSet.getFolderName());
    assertEquals("ds-load-orders-Table input.csv", dataSet.getBaseFilename());
  }

  @Test
  void usesPipelineDirectoryWhenDatasetsFolderIsNotConfigured() {
    DataSet dataSet = new DataSet();
    DataSetDefaults.apply(dataSet, "C:\\proj\\pipe.hpl", "Check", new Variables(), null);

    assertEquals("ds-pipe-Check", dataSet.getName());
    assertEquals("C:/proj", dataSet.getFolderName());
    assertEquals("ds-pipe-Check.csv", dataSet.getBaseFilename());
  }

  @Test
  void readsDatasetsFolderFromParentVariableSpace() {
    Variables parent = new Variables();
    parent.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, "/data/sets");
    Variables child = new Variables();
    child.setParentVariables(parent);

    DataSet dataSet = new DataSet();
    DataSetDefaults.apply(dataSet, "/tmp/pipe/load.hpl", "Check", child, null);

    assertEquals("${HOP_DATASETS_FOLDER}", dataSet.getFolderName());
  }

  @Test
  void blankDatasetsFolderFallsBackToPipelineDirectory() {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, "  ");

    DataSet dataSet = new DataSet();
    DataSetDefaults.apply(dataSet, "/tmp/pipe/load.hpl", null, variables, null);

    assertEquals("ds-load", dataSet.getName());
    assertEquals("/tmp/pipe", dataSet.getFolderName());
    assertEquals("ds-load.csv", dataSet.getBaseFilename());
  }

  @Test
  void sanitizesPathCharactersInTheTransformName() {
    DataSet dataSet = new DataSet();
    DataSetDefaults.apply(dataSet, "dir/a:b.hpl", "in/out", new Variables(), null);

    assertEquals("ds-a-b-in-out", dataSet.getName());
    assertEquals("dir", dataSet.getFolderName());
    assertEquals("ds-a-b-in-out.csv", dataSet.getBaseFilename());
  }

  @Test
  void suggestsFromTransformOnlyWhenThePipelineHasNoFilename() {
    DataSet dataSet = new DataSet();
    DataSetDefaults.apply(dataSet, null, "Check", new Variables(), null);

    assertEquals("ds-Check", dataSet.getName());
    assertNull(dataSet.getFolderName());
    assertEquals("ds-Check.csv", dataSet.getBaseFilename());
  }

  @Test
  void leavesNameEmptyWhenPipelineAndTransformAreUnknown() {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, "/data/sets");

    DataSet dataSet = new DataSet();
    DataSetDefaults.apply(dataSet, "   ", "  ", variables, null);

    assertNull(dataSet.getName());
    assertEquals("${HOP_DATASETS_FOLDER}", dataSet.getFolderName());
    assertEquals(DataSet.DEFAULT_BASE_FILENAME, dataSet.getBaseFilename());
  }

  @Test
  void doesNotReplaceValuesTheUserAlreadySet() {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, "/data/sets");

    DataSet dataSet = new DataSet();
    dataSet.setName("kept");
    dataSet.setFolderName("kept-folder");
    dataSet.setBaseFilename("kept.csv");
    DataSetDefaults.apply(dataSet, "/tmp/pipe.hpl", "Step", variables, null);

    assertEquals("kept", dataSet.getName());
    assertEquals("kept-folder", dataSet.getFolderName());
    assertEquals("kept.csv", dataSet.getBaseFilename());
  }

  @Test
  void replacesPlaceholderFilenameWhenTheNameIsAlreadySet() {
    DataSet dataSet = new DataSet();
    dataSet.setName("golden");
    dataSet.setFolderName("folder");

    DataSetDefaults.apply(dataSet, "/tmp/other.hpl", "Other", new Variables(), null);

    assertEquals("golden", dataSet.getName());
    assertEquals("folder", dataSet.getFolderName());
    assertEquals("golden.csv", dataSet.getBaseFilename());
  }

  @Test
  void secondApplyDoesNotReplaceTheSuggestion() {
    Variables variables = new Variables();
    variables.setVariable(DataSet.VARIABLE_HOP_DATASETS_FOLDER, "/data/sets");
    DataSet dataSet = new DataSet();

    DataSetDefaults.apply(dataSet, "/tmp/load.hpl", "Check", variables, null);
    DataSetDefaults.apply(dataSet, "/tmp/other.hpl", "Other", variables, null);

    assertEquals("ds-load-Check", dataSet.getName());
    assertEquals("${HOP_DATASETS_FOLDER}", dataSet.getFolderName());
    assertEquals("ds-load-Check.csv", dataSet.getBaseFilename());
  }

  @Test
  void avoidsAnExistingDataSetName() throws Exception {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    DataSet existing = new DataSet();
    existing.setName("ds-load-orders-Check");
    provider.getSerializer(DataSet.class).save(existing);
    DataSet second = new DataSet();
    second.setName("ds-load-orders-Check 2");
    provider.getSerializer(DataSet.class).save(second);

    DataSet dataSet = new DataSet();
    DataSetDefaults.apply(dataSet, "/p/load-orders.hpl", "Check", new Variables(), provider);

    assertEquals("ds-load-orders-Check 3", dataSet.getName());
    assertEquals("ds-load-orders-Check 3.csv", dataSet.getBaseFilename());
  }

  @Test
  void nullDataSetIsIgnored() {
    DataSetDefaults.apply(null, "/tmp/pipe.hpl", "Check", new Variables(), null);
  }
}
