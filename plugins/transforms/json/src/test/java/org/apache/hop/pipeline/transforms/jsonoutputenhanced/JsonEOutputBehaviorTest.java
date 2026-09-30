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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class JsonEOutputBehaviorTest {
  private final String base = "ram:///enhanced-json-" + UUID.randomUUID();

  @BeforeAll
  static void initialize() throws HopException {
    HopClientEnvironment.init();
  }

  @AfterEach
  void removeFiles() throws Exception {
    try (FileObject file = HopVfs.getFileObject(base)) {
      if (file.exists()) {
        file.deleteAll();
      }
    }
  }

  private JsonEOutputMeta meta(JsonEOutputMeta.OperationType operation) {
    JsonEOutputMeta meta = new JsonEOutputMeta();
    meta.setOperationType(operation);
    meta.setOutputValue("rows");
    meta.setJsonBloc("");
    meta.setEncoding("UTF-8");
    JsonEOutputField field = new JsonEOutputField();
    field.setFieldName("payload");
    field.setElementName("payload");
    meta.getOutputFields().add(field);
    meta.getFileSettings().setFileName(base + "/out");
    meta.getFileSettings().setExtension("json");
    meta.getFileSettings().setCreateParentFolder(true);
    meta.getFileSettings().setDoNotOpenNewFileInit(true);
    return meta;
  }

  private static IRowMeta rowMeta(String... names) {
    IRowMeta row = new RowMeta();
    for (String name : names) {
      row.addValueMeta(new ValueMetaString(name));
    }
    return row;
  }

  private static final class Harness implements AutoCloseable {
    final TransformMockHelper<JsonEOutputMeta, JsonEOutputData> helper;
    final List<RowMetaAndData> written = new ArrayList<>();
    final JsonEOutput transform;

    Harness(JsonEOutputMeta meta, IRowMeta inputMeta, Object[]... rows) {
      helper =
          new TransformMockHelper<>("enhanced JSON", JsonEOutputMeta.class, JsonEOutputData.class);
      when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
          .thenReturn(helper.iLogChannel);
      when(helper.pipeline.isRunning()).thenReturn(true);
      when(helper.transformMeta.getTransform()).thenReturn(meta);
      var iterator = Arrays.asList(rows).iterator();
      transform =
          new JsonEOutput(
              helper.transformMeta,
              meta,
              new JsonEOutputData(),
              0,
              helper.pipelineMeta,
              helper.pipeline) {
            @Override
            public Object[] getRow() {
              return iterator.hasNext() ? iterator.next() : null;
            }

            @Override
            public void putRow(IRowMeta rowMeta, Object[] row) {
              written.add(new RowMetaAndData(rowMeta.clone(), row.clone()));
            }
          };
      transform.setInputRowMeta(inputMeta);
    }

    void run() throws HopException {
      assertTrue(transform.init());
      while (transform.processRow()) {
        // Drain the real transform through EOF.
      }
    }

    @Override
    public void close() {
      try {
        transform.dispose();
      } finally {
        helper.cleanUp();
      }
    }
  }

  @Test
  void missingRuntimeGroupKeyIsAnActionableError() throws Exception {
    JsonEOutputMeta meta = meta(JsonEOutputMeta.OperationType.OUTPUT_VALUE);
    meta.getKeyFields().add(new JsonEOutputKeyField("gone"));
    try (Harness h = new Harness(meta, rowMeta("payload"), new Object[] {"x"})) {
      assertTrue(h.transform.init());
      HopException error = assertThrows(HopException.class, h.transform::processRow);
      assertTrue(error.getMessage().contains("gone"), error.getMessage());
    }
  }
}
