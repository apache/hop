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

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineagePortableFilename;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.FileIoContentSchema;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Emits {@link LineageEventKind#FILE_IO} observations to {@link LineageHub} so sinks can record
 * which workflow actions or pipeline transforms touched which files.
 */
public final class LineageFileIoEmitter {

  private LineageFileIoEmitter() {}

  /**
   * Records a file/object-store operation performed by a workflow action during {@link
   * IAction#execute}.
   *
   * @param workflow running workflow engine (must not be null)
   * @param action the action instance performing the operation (must not be null)
   * @param operation logical operation kind
   * @param sourceUri VFS URI of the source (required for most operations)
   * @param targetUri VFS URI of the target (may be null for DELETE)
   * @param bytesTransferred byte length when known (e.g. file size moved/copied); may be null
   * @param success whether the operation completed without error
   * @param message optional short detail (failure reason, etc.)
   */
  public static void emitWorkflowActionFileIo(
      IWorkflowEngine<WorkflowMeta> workflow,
      IAction action,
      FileIoOperation operation,
      String sourceUri,
      String targetUri,
      Long bytesTransferred,
      boolean success,
      String message) {
    emitWorkflowActionFileIo(
        workflow,
        action,
        operation,
        sourceUri,
        targetUri,
        bytesTransferred,
        success,
        message,
        null);
  }

  /**
   * Same as {@link #emitWorkflowActionFileIo(IWorkflowEngine, IAction, FileIoOperation, String,
   * String, Long, boolean, String)} with optional file content schema.
   */
  public static void emitWorkflowActionFileIo(
      IWorkflowEngine<WorkflowMeta> workflow,
      IAction action,
      FileIoOperation operation,
      String sourceUri,
      String targetUri,
      Long bytesTransferred,
      boolean success,
      String message,
      FileIoContentSchema contentSchema) {
    if (workflow == null || action == null || operation == null) {
      return;
    }
    emit(
        workflow,
        action,
        new FileIoLineagePayload(
            operation, sourceUri, targetUri, bytesTransferred, success, message, contentSchema));
  }

  /**
   * Same as {@link #emitWorkflowActionFileIo(IWorkflowEngine, IAction, FileIoOperation, String,
   * String, Long, boolean, String)} but derives URIs from the {@link FileObject} instances.
   *
   * @param sourceFile may be null only when the operation has no meaningful source
   * @param targetFile may be null for DELETE
   */
  public static void emitWorkflowActionFileIo(
      IWorkflowEngine<WorkflowMeta> workflow,
      IAction action,
      FileIoOperation operation,
      FileObject sourceFile,
      FileObject targetFile,
      Long bytesTransferred,
      boolean success,
      String message) {
    if (workflow == null || action == null || operation == null) {
      return;
    }
    emitWorkflowActionFileIo(
        workflow,
        action,
        operation,
        LineageVfsIoSupport.uriString(sourceFile),
        LineageVfsIoSupport.uriString(targetFile),
        bytesTransferred,
        success,
        message,
        null);
  }

  private static void emit(
      IWorkflowEngine<WorkflowMeta> workflow, IAction action, FileIoLineagePayload payload) {
    WorkflowMeta meta = workflow.getWorkflowMeta();
    String workflowName = meta != null ? meta.getName() : null;
    String filename = meta != null ? meta.getFilename() : null;

    LineageContext.Builder ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.ACTION)
            .logChannelId(workflow.getLogChannelId())
            .workflowName(workflowName)
            .actionName(action.getName());
    if (!Utils.isEmpty(filename)) {
      ctx.hopFilename(filename);
      ctx.hopFilenamePortableKey(LineagePortableFilename.portableKey(filename, workflow));
    }
    ctx.putAttribute("workflowLogChannelId", workflow.getLogChannelId());
    if (!Utils.isEmpty(action.getPluginId())) {
      ctx.putAttribute("actionPluginId", action.getPluginId());
    }

    LineageHub.getInstance().emit(LineageEvent.of(LineageEventKind.FILE_IO, ctx.build(), payload));
  }

  /**
   * Records a VFS file/object-store operation performed by a pipeline transform using a raw URI.
   *
   * @param sourceUri source URI; may be null for write-only operations
   * @param targetUri target URI; may be null for read-only operations
   */
  public static void emitTransformFileIo(
      ITransform transform,
      FileIoOperation operation,
      String sourceUri,
      String targetUri,
      Long bytesTransferred,
      boolean success,
      String message) {
    emitTransformFileIo(
        transform, operation, sourceUri, targetUri, bytesTransferred, success, message, null);
  }

  /**
   * Same as {@link #emitTransformFileIo(ITransform, FileIoOperation, String, String, Long, boolean,
   * String)} with optional content schema (written/read columns and structure).
   */
  public static void emitTransformFileIo(
      ITransform transform,
      FileIoOperation operation,
      String sourceUri,
      String targetUri,
      Long bytesTransferred,
      boolean success,
      String message,
      FileIoContentSchema contentSchema) {
    if (transform == null || operation == null) {
      return;
    }
    LineageContext.Builder ctx = LineageRunLifecycleEmitter.transformContextBuilder(transform);
    if (ctx == null) {
      return;
    }
    if (!Utils.isEmpty(transform.getTransformPluginId())) {
      ctx.putAttribute("transformPluginId", transform.getTransformPluginId());
    }
    LineageHub.getInstance()
        .emit(
            LineageEvent.of(
                LineageEventKind.FILE_IO,
                ctx.build(),
                new FileIoLineagePayload(
                    operation,
                    sourceUri,
                    targetUri,
                    bytesTransferred,
                    success,
                    message,
                    contentSchema)));
  }

  /**
   * Records a VFS file/object-store operation performed by a pipeline transform (e.g. file input /
   * output with byte metrics).
   *
   * @param sourceFile optional source; may be null for write-only operations
   * @param targetFile optional target; may be null for read-only operations
   */
  public static void emitTransformFileIo(
      ITransform transform,
      FileIoOperation operation,
      FileObject sourceFile,
      FileObject targetFile,
      Long bytesTransferred,
      boolean success,
      String message) {
    emitTransformFileIo(
        transform, operation, sourceFile, targetFile, bytesTransferred, success, message, null);
  }

  /**
   * Same as {@link #emitTransformFileIo(ITransform, FileIoOperation, FileObject, FileObject, Long,
   * boolean, String)} with optional content schema.
   */
  public static void emitTransformFileIo(
      ITransform transform,
      FileIoOperation operation,
      FileObject sourceFile,
      FileObject targetFile,
      Long bytesTransferred,
      boolean success,
      String message,
      FileIoContentSchema contentSchema) {
    emitTransformFileIo(
        transform,
        operation,
        LineageVfsIoSupport.uriString(sourceFile),
        LineageVfsIoSupport.uriString(targetFile),
        bytesTransferred,
        success,
        message,
        contentSchema);
  }
}
