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

package org.apache.hop.pipeline.transforms.jsonnormalize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputField;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression: Hidden additional field must use {@code data.hidden} from {@code
 * FileObject.isHidden()}, not {@code Boolean.valueOf(data.path)}.
 */
class JsonNormalizeInputHiddenFieldTest {

  private static final String BASE_RAM_DIR = "ram:///jsonNormalizeHiddenTest/";

  private TransformMockHelper<JsonNormalizeInputMeta, JsonNormalizeInputData> helper;

  @BeforeAll
  static void init() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "json normalize hidden test",
            JsonNormalizeInputMeta.class,
            JsonNormalizeInputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() throws Exception {
    helper.cleanUp();
    try (FileObject baseDir = HopVfs.getFileObject(BASE_RAM_DIR)) {
      if (baseDir.exists()) {
        baseDir.deleteAll();
      }
    }
  }

  @Test
  void testHiddenFileFieldUsesFileIsHiddenFlag() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    helper.redirectLog(err, LogLevel.ERROR);

    final String path = BASE_RAM_DIR + "hidden-flag.json";
    try (FileObject fileObj = HopVfs.getFileObject(path)) {
      try (OutputStream out = fileObj.getContent().getOutputStream()) {
        out.write("{\"color\":\"green\"}".getBytes());
      }

      JsonInputField color = new JsonInputField();
      color.setName("color");
      color.setType(IValueMeta.TYPE_STRING);
      color.setPath("$.color");

      JsonNormalizeInputMeta meta = new JsonNormalizeInputMeta();
      meta.setDefault();
      meta.setInFields(true);
      meta.setIsAFile(true);
      meta.setFieldValue("in file");
      meta.setRemoveSourceField(true);
      meta.setRecordPath("$");
      meta.setIgnoreMissingField(true);
      meta.setPathField("dir path");
      meta.setIsHiddenField("is_hidden");
      List<JsonInputField> fields = new ArrayList<>();
      fields.add(color);
      meta.setInputFields(fields);

      JsonNormalizeInputData data = new JsonNormalizeInputData();
      IRowSet input = helper.getMockInputRowSet(new Object[][] {new Object[] {path}});
      IRowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaString("in file"));
      input.setRowMeta(rowMeta);

      JsonNormalizeInput transform =
          new JsonNormalizeInput(
              helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline) {
            @Override
            protected void fillFileAdditionalFields(JsonNormalizeInputData data, FileObject file)
                throws FileSystemException {
              super.fillFileAdditionalFields(data, file);
              data.hidden = true;
            }
          };
      transform.addRowSetToInputRowSets(input);
      transform.setInputRowMeta(rowMeta);
      transform.init();

      AtomicReference<Object[]> written = new AtomicReference<>();
      AtomicReference<IRowMeta> writtenMeta = new AtomicReference<>();
      transform.addRowListener(
          new RowAdapter() {
            @Override
            public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
              writtenMeta.set(rowMeta);
              written.set(row);
            }
          });

      for (int i = 0; i < 3; i++) {
        if (!transform.processRow()) {
          break;
        }
      }

      assertEquals(0, transform.getErrors(), err.toString());
      Object[] row = written.get();
      assertTrue(row != null, "expected one output row");
      IRowMeta outMeta = writtenMeta.get();
      int hiddenIdx = outMeta.indexOfValue("is_hidden");
      assertTrue(hiddenIdx >= 0);
      assertEquals(Boolean.TRUE, row[hiddenIdx], "is_hidden must use data.hidden, not path");
      // Sanity: path is not the literal "true", so the old Boolean.valueOf(path) bug would fail.
      int pathIdx = outMeta.indexOfValue("dir path");
      assertTrue(pathIdx >= 0);
      assertEquals(false, Boolean.valueOf(String.valueOf(row[pathIdx])));
    }
  }
}
