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

package org.apache.hop.lineage;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.VFS;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.FileIoPathSyntax;
import org.apache.hop.lineage.model.FileIoTabularColumn;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LineageFileIoEmitterTest {

  @Test
  void emitWorkflowActionFileIo_postsFileIoEventToLineageHub() {
    LineageHub hub = mock(LineageHub.class);

    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("wf1");
    meta.setFilename("/tmp/wf1.hwf");

    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> wf = mock(IWorkflowEngine.class);
    when(wf.getWorkflowMeta()).thenReturn(meta);
    when(wf.getLogChannelId()).thenReturn("wf-log-1");

    ActionBase action = mock(ActionBase.class);
    when(action.getName()).thenReturn("Move stuff");
    when(action.getPluginId()).thenReturn("MOVE_FILES");

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageFileIoEmitter.emitWorkflowActionFileIo(
          wf,
          action,
          FileIoOperation.MOVE,
          "file:///data/in.csv",
          "file:///archive/in.csv",
          42L,
          true,
          null);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) -> {
                  if (e.getKind() != LineageEventKind.FILE_IO) {
                    return false;
                  }
                  if (e.getContext().getSubjectType() != LineageSubjectType.ACTION) {
                    return false;
                  }
                  if (!"wf1".equals(e.getContext().getWorkflowName())) {
                    return false;
                  }
                  if (!"Move stuff".equals(e.getContext().getActionName())) {
                    return false;
                  }
                  if (!(e.getPayload() instanceof FileIoLineagePayload p)) {
                    return false;
                  }
                  return p.getOperation() == FileIoOperation.MOVE
                      && "file:///data/in.csv".equals(p.getSourceUri())
                      && "file:///archive/in.csv".equals(p.getTargetUri())
                      && p.getBytesTransferred() == 42L
                      && p.isSuccess()
                      && p.getContentSchema() == null;
                }));
  }

  @Test
  void emitWorkflowActionFileIo_withFileObjects_setsUriFromFileName() throws Exception {
    LineageHub hub = mock(LineageHub.class);

    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("wf1");
    meta.setFilename("/tmp/wf1.hwf");

    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> wf = mock(IWorkflowEngine.class);
    when(wf.getWorkflowMeta()).thenReturn(meta);
    when(wf.getLogChannelId()).thenReturn("wf-log-1");

    ActionBase action = mock(ActionBase.class);
    when(action.getName()).thenReturn("Move stuff");
    when(action.getPluginId()).thenReturn("MOVE_FILES");

    FileObject src = VFS.getManager().resolveFile("file:///tmp/hop-lineage-fileio-src.bin");
    FileObject dst = VFS.getManager().resolveFile("file:///tmp/hop-lineage-fileio-dst.bin");

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageFileIoEmitter.emitWorkflowActionFileIo(
          wf, action, FileIoOperation.MOVE, src, dst, 99L, true, null);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) -> {
                  if (!(e.getPayload() instanceof FileIoLineagePayload p)) {
                    return false;
                  }
                  return p.getOperation() == FileIoOperation.MOVE
                      && p.getSourceUri() != null
                      && p.getSourceUri().startsWith("file:")
                      && p.getTargetUri() != null
                      && p.getTargetUri().startsWith("file:")
                      && p.getBytesTransferred() == 99L
                      && p.getContentSchema() == null;
                }));
  }

  @Test
  void emitTransformFileIo_withContentSchema_passesPayload() {
    LineageHub hub = mock(LineageHub.class);

    ITransform tr = mock(ITransform.class);
    ILogChannel logCh = mock(ILogChannel.class);
    when(logCh.getLogChannelId()).thenReturn("tr-log-schema");
    when(tr.getLogChannel()).thenReturn(logCh);
    when(tr.getTransformName()).thenReturn("Json input");
    when(tr.getCopy()).thenReturn(0);
    when(tr.getTransformPluginId()).thenReturn("JSON_INPUT");

    org.apache.hop.pipeline.PipelineMeta pm = new org.apache.hop.pipeline.PipelineMeta();
    pm.setName("pipe-schema");
    pm.setFilename("/tmp/pipe-schema.hpl");
    @SuppressWarnings("unchecked")
    org.apache.hop.pipeline.engine.IPipelineEngine<org.apache.hop.pipeline.PipelineMeta> pipeline =
        mock(org.apache.hop.pipeline.engine.IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(pm);
    when(pipeline.getFilename()).thenReturn("/tmp/pipe-schema.hpl");
    when(pipeline.getLogChannelId()).thenReturn("pipe-log-schema");
    when(tr.getPipeline()).thenReturn(pipeline);

    FileIoContentSchema schema =
        FileIoContentSchema.tabularWithMergedTree(
            "json",
            java.util.List.of(
                new FileIoTabularColumn(
                    "id", "Integer", 0, 0, "$.id", FileIoPathSyntax.JSON_PATH, false)));

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageFileIoEmitter.emitTransformFileIo(
          tr, FileIoOperation.READ, "file:///tmp/x.json", null, 3L, true, null, schema);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) -> {
                  if (!(e.getPayload() instanceof FileIoLineagePayload p)) {
                    return false;
                  }
                  return p.getContentSchema() != null
                      && "json".equals(p.getContentSchema().getFormatHint())
                      && p.getContentSchema().getColumns().size() == 1
                      && p.getContentSchema().getStructureRoots().size() >= 0;
                }));
  }

  @Test
  void emitTransformFileIo_postsFileIoEventWithTransformContext() throws Exception {
    LineageHub hub = mock(LineageHub.class);

    ITransform tr = mock(ITransform.class);
    ILogChannel logCh = mock(ILogChannel.class);
    when(logCh.getLogChannelId()).thenReturn("tr-log-1");
    when(tr.getLogChannel()).thenReturn(logCh);
    when(tr.getTransformName()).thenReturn("Text File Input");
    when(tr.getCopy()).thenReturn(0);
    when(tr.getTransformPluginId()).thenReturn("TEXT_FILE_INPUT");

    org.apache.hop.pipeline.PipelineMeta pm = new org.apache.hop.pipeline.PipelineMeta();
    pm.setName("pipe1");
    pm.setFilename("/tmp/pipe1.hpl");
    @SuppressWarnings("unchecked")
    org.apache.hop.pipeline.engine.IPipelineEngine<org.apache.hop.pipeline.PipelineMeta> pipeline =
        mock(org.apache.hop.pipeline.engine.IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(pm);
    when(pipeline.getFilename()).thenReturn("/tmp/pipe1.hpl");
    when(pipeline.getLogChannelId()).thenReturn("pipe-log-1");
    when(tr.getPipeline()).thenReturn(pipeline);

    FileObject f = VFS.getManager().resolveFile("file:///tmp/hop-lineage-transform-read.bin");

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageFileIoEmitter.emitTransformFileIo(tr, FileIoOperation.READ, f, null, 5L, true, null);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) -> {
                  if (e.getContext().getSubjectType() != LineageSubjectType.TRANSFORM) {
                    return false;
                  }
                  if (!"Text File Input".equals(e.getContext().getTransformName())) {
                    return false;
                  }
                  if (!"pipe1".equals(e.getContext().getPipelineName())) {
                    return false;
                  }
                  if (!(e.getPayload() instanceof FileIoLineagePayload p)) {
                    return false;
                  }
                  return p.getOperation() == FileIoOperation.READ
                      && p.getBytesTransferred() == 5L
                      && p.getSourceUri() != null
                      && p.getSourceUri().startsWith("file:")
                      && p.getContentSchema() == null;
                }));
  }

  @Test
  void emitTransformFileIo_stringUris_postsFileIoEvent() throws Exception {
    LineageHub hub = mock(LineageHub.class);

    ITransform tr = mock(ITransform.class);
    ILogChannel logCh = mock(ILogChannel.class);
    when(logCh.getLogChannelId()).thenReturn("tr-log-2");
    when(tr.getLogChannel()).thenReturn(logCh);
    when(tr.getTransformName()).thenReturn("CSV input");
    when(tr.getCopy()).thenReturn(0);
    when(tr.getTransformPluginId()).thenReturn("CSV_INPUT");

    org.apache.hop.pipeline.PipelineMeta pm = new org.apache.hop.pipeline.PipelineMeta();
    pm.setName("pipe2");
    pm.setFilename("/tmp/pipe2.hpl");
    @SuppressWarnings("unchecked")
    org.apache.hop.pipeline.engine.IPipelineEngine<org.apache.hop.pipeline.PipelineMeta> pipeline =
        mock(org.apache.hop.pipeline.engine.IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(pm);
    when(pipeline.getFilename()).thenReturn("/tmp/pipe2.hpl");
    when(pipeline.getLogChannelId()).thenReturn("pipe-log-2");
    when(tr.getPipeline()).thenReturn(pipeline);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageFileIoEmitter.emitTransformFileIo(
          tr, FileIoOperation.READ, "file:///tmp/hop-lineage-string.csv", null, 7L, true, null);
    }

    verify(hub)
        .emit(
            argThat(
                (LineageEvent e) -> {
                  if (!(e.getPayload() instanceof FileIoLineagePayload p)) {
                    return false;
                  }
                  return p.getOperation() == FileIoOperation.READ
                      && "file:///tmp/hop-lineage-string.csv".equals(p.getSourceUri())
                      && p.getBytesTransferred() == 7L
                      && p.getContentSchema() == null;
                }));
  }

  @Test
  void emitWorkflowActionFileIo_nullWorkflowDoesNotTouchHub() {
    LineageHub hub = mock(LineageHub.class);
    ActionBase action = mock(ActionBase.class);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);

      LineageFileIoEmitter.emitWorkflowActionFileIo(
          null, action, FileIoOperation.DELETE, "file:///a", null, null, false, "x");
    }

    verify(hub, Mockito.never()).emit(Mockito.any());
  }
}
