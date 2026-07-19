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

package org.apache.hop.spark.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInput;
import org.apache.hop.pipeline.transforms.file.BaseFileInputMeta;
import org.apache.hop.pipeline.transforms.fileinput.text.TextFileInputMeta;
import org.apache.hop.spark.util.SparkConst;
import org.junit.jupiter.api.Test;

/**
 * Text File Input (and kin) resolve filename sources via {@code
 * findInputRowSet(accept_transform_name)} on a main hop — not an INFO stream. The Spark
 * mini-pipeline must re-bind that name to {@code _INJECTOR_}.
 */
class AcceptFilenamesBindingTest {

  @Test
  void bindRemapsTextFileInputAcceptTransformToMainInjector() {
    TextFileInputMeta meta = new TextFileInputMeta();
    meta.getFileInput().setAcceptingFilenames(true);
    meta.getFileInput().setAcceptingTransformName("source/test-file*.csv");
    meta.getFileInput().setAcceptingField("filename");

    assertTrue(HopMapPartitionsFn.bindAcceptingFilenamesToMainInjector(meta));
    assertEquals(
        SparkConst.INJECTOR_TRANSFORM_NAME, meta.getFileInput().getAcceptingTransformName());
  }

  @Test
  void bindIgnoresNonAcceptingFileInput() {
    TextFileInputMeta meta = new TextFileInputMeta();
    meta.getFileInput().setAcceptingFilenames(false);
    meta.getFileInput().setAcceptingTransformName("source");

    assertFalse(HopMapPartitionsFn.bindAcceptingFilenamesToMainInjector(meta));
    assertEquals("source", meta.getFileInput().getAcceptingTransformName());
  }

  @Test
  void bindIgnoresUnrelatedTransforms() {
    assertFalse(HopMapPartitionsFn.bindAcceptingFilenamesToMainInjector(new DummyMeta()));
    assertFalse(HopMapPartitionsFn.bindAcceptingFilenamesToMainInjector(null));
  }

  @Test
  void bindWorksForBaseFileInputMetaSubclass() {
    BaseFileInput fileInput = new BaseFileInput();
    fileInput.setAcceptingFilenames(true);
    fileInput.setAcceptingTransformName("Get File Names");
    BaseFileInputMeta<?, ?, BaseFileInput> meta =
        new BaseFileInputMeta<>() {
          @Override
          protected BaseFileInput getFileInput() {
            return fileInput;
          }

          @Override
          protected void setFileInput(BaseFileInput input) {
            // test double: fixed fileInput instance
          }

          @Override
          public String getEncoding() {
            return null;
          }
        };

    assertTrue(HopMapPartitionsFn.bindAcceptingFilenamesToMainInjector(meta));
    assertEquals(SparkConst.INJECTOR_TRANSFORM_NAME, meta.getAcceptingTransformName());
    assertEquals(SparkConst.INJECTOR_TRANSFORM_NAME, fileInput.getAcceptingTransformName());
  }
}
