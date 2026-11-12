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

package org.apache.hop.pipeline.transforms.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoTabularColumn;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** The schema a file input transform reports for the file it reads, in its FILE_IO lineage. */
@SuppressWarnings({"rawtypes", "unchecked"})
class BaseFileInputTransformLineageTest {

  /** A file input that reads the fields "tag" and "amount" from its files. */
  static final class FileInput
      extends BaseFileInputTransform<BaseFileInputMeta, BaseFileInputTransformData> {
    final AtomicInteger fileFieldNamesCalls = new AtomicInteger();

    FileInput(
        TransformMeta transformMeta,
        BaseFileInputMeta meta,
        BaseFileInputTransformData data,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transformMeta, meta, data, 0, pipelineMeta, pipeline);
    }

    @Override
    protected IBaseFileInputReader createReader(
        BaseFileInputMeta meta, BaseFileInputTransformData data, FileObject file) {
      return null;
    }

    @Override
    public boolean failAfterBadFile(String errorMsg, boolean errorIgnored, boolean skipBadFiles) {
      return true;
    }

    @Override
    protected List<String> fileFieldNames() {
      fileFieldNamesCalls.incrementAndGet();
      return List.of("tag", "amount");
    }
  }

  private TransformMockHelper<BaseFileInputMeta, BaseFileInputTransformData> helper;
  private FileInput transform;
  private FileObject file;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    helper =
        new TransformMockHelper<>(
            "File input", BaseFileInputMeta.class, BaseFileInputTransformData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    BaseFileInput fileInput = new BaseFileInput();
    fileInput.setPassingThruFields(true);
    when(helper.iTransformMeta.getFileInput()).thenReturn(fileInput);
    BaseFileInputTransformData data = new BaseFileInputTransformData();
    transform =
        new FileInput(
            helper.transformMeta,
            helper.iTransformMeta,
            data,
            helper.pipelineMeta,
            helper.pipeline);

    // Passed through from the filename rows: "filename" and a "tag" of their own. The file's "tag"
    // comes after them and is renamed "tag_1" in the row; "rownum" is added by the transform.
    RowMeta passedThrough = new RowMeta();
    passedThrough.addValueMeta(new ValueMetaString("filename"));
    passedThrough.addValueMeta(new ValueMetaString("tag"));
    RowMeta output = new RowMeta();
    output.mergeRowMeta(passedThrough, "File input");
    output.addValueMeta(new ValueMetaInteger("tag"));
    output.addValueMeta(new ValueMetaNumber("amount"));
    output.addValueMeta(new ValueMetaInteger("rownum"));
    data.outputRowMeta = output;
    data.nrPassThruFields = passedThrough.size();

    file = HopVfs.getFileObject("ram:///lineage/orders.csv");
    try (OutputStream out = file.getContent().getOutputStream()) {
      out.write("7,12.5\n".getBytes(StandardCharsets.UTF_8));
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    file.delete();
    helper.cleanUp();
  }

  @Test
  void theFileSchemaIsTheFilesOwnFieldsWithTheirTypes() {
    LineageHub hub = mock(LineageHub.class);
    when(hub.isEnabled()).thenReturn(true);
    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      transform.emitLineageFileRead(file);
      transform.emitLineageFileRead(file);
    }

    ArgumentCaptor<LineageEvent> events = ArgumentCaptor.forClass(LineageEvent.class);
    verify(hub, times(2)).emit(events.capture());
    FileIoContentSchema schema =
        ((FileIoLineagePayload) events.getValue().getPayload()).getContentSchema();
    List<FileIoTabularColumn> columns = schema.getColumns();
    assertEquals(2, columns.size());
    assertEquals("tag", columns.get(0).getName());
    assertEquals("Integer", columns.get(0).getHopTypeName());
    assertEquals("amount", columns.get(1).getName());
    assertEquals("Number", columns.get(1).getHopTypeName());
    assertEquals("csv", schema.getFormatHint());
    // Worked out once, not on every file.
    assertEquals(1, transform.fileFieldNamesCalls.get());
  }

  @Test
  void nothingIsWorkedOutWithLineageOff() {
    LineageHub hub = mock(LineageHub.class);
    when(hub.isEnabled()).thenReturn(false);
    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      transform.emitLineageFileRead(file);
    }

    verify(hub, never()).emit(any());
    assertEquals(0, transform.fileFieldNamesCalls.get());
  }
}
