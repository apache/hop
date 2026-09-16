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
package org.apache.hop.metadata.validation;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.HopMetadataPropertyWalker;
import org.apache.hop.metadata.util.HopMetadataPropertyWalker.StringProperty;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/**
 * Warns when a transform or action references a relational database connection that is not in
 * project metadata.
 *
 * <p>This is an existence check only. It never opens a JDBC connection. Names that still contain a
 * variable token after resolving the current {@link IVariables} are skipped, because the name
 * cannot be decided at design time.
 */
public final class ReferencedDatabaseConnectionChecker {

  public static final String ERROR_NOT_ASSIGNED = "CONNECTION_NOT_ASSIGNED";
  public static final String ERROR_DOES_NOT_EXIST = "CONNECTION_DOES_NOT_EXIST";

  /**
   * The connection could not be looked up at all, so nothing is known about it. Reported at INFO:
   * it says something about the metadata being unreadable, not about the file being linted.
   */
  public static final String INFO_NOT_VERIFIED = "CONNECTION_NOT_VERIFIED";

  private static final Class<?> PKG = ReferencedDatabaseConnectionChecker.class;

  /** How much of a failure reason fits in the problems list before it stops being readable. */
  private static final int MAX_REASON_LENGTH = 200;

  private ReferencedDatabaseConnectionChecker() {}

  public static List<ICheckResult> checkPipeline(
      PipelineMeta pipelineMeta, IVariables variables, IHopMetadataProvider metadataProvider) {
    List<ICheckResult> remarks = new ArrayList<>();
    if (pipelineMeta == null) {
      return remarks;
    }
    for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
      remarks.addAll(checkTransform(transformMeta, variables, metadataProvider));
    }
    return remarks;
  }

  public static List<ICheckResult> checkWorkflow(
      WorkflowMeta workflowMeta, IVariables variables, IHopMetadataProvider metadataProvider) {
    List<ICheckResult> remarks = new ArrayList<>();
    if (workflowMeta == null) {
      return remarks;
    }
    for (ActionMeta actionMeta : workflowMeta.getActions()) {
      remarks.addAll(checkAction(actionMeta, variables, metadataProvider));
    }
    return remarks;
  }

  public static List<ICheckResult> checkTransform(
      TransformMeta transformMeta, IVariables variables, IHopMetadataProvider metadataProvider) {
    if (transformMeta == null || transformMeta.getTransform() == null) {
      return List.of();
    }
    return checkObject(
        transformMeta.getTransform(),
        BaseMessages.getString(PKG, "ReferencedDatabaseConnectionChecker.Kind.Transform"),
        transformMeta.getName(),
        transformMeta,
        variables,
        metadataProvider);
  }

  public static List<ICheckResult> checkAction(
      ActionMeta actionMeta, IVariables variables, IHopMetadataProvider metadataProvider) {
    if (actionMeta == null || actionMeta.getAction() == null) {
      return List.of();
    }
    return checkObject(
        actionMeta.getAction(),
        BaseMessages.getString(PKG, "ReferencedDatabaseConnectionChecker.Kind.Action"),
        actionMeta.getName(),
        actionMeta.getAction(),
        variables,
        metadataProvider);
  }

  /**
   * Check one metadata object for {@link HopMetadataPropertyType#RDBMS_CONNECTION} fields.
   *
   * @param metadataObject the transform or action metadata, or a nested POJO used in tests
   * @param ownerKind "Transform" or "Action" (already translated)
   * @param ownerName the transform or action name
   * @param source the check-result source, may be null
   */
  public static List<ICheckResult> checkObject(
      Object metadataObject,
      String ownerKind,
      String ownerName,
      ICheckResultSource source,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    List<ICheckResult> remarks = new ArrayList<>();
    if (metadataObject == null || metadataProvider == null) {
      return remarks;
    }

    IHopMetadataSerializer<DatabaseMeta> serializer;
    try {
      serializer = metadataProvider.getSerializer(DatabaseMeta.class);
    } catch (Exception e) {
      return remarks;
    }
    if (serializer == null) {
      return remarks;
    }

    for (StringProperty property :
        HopMetadataPropertyWalker.collectStrings(
            metadataObject, HopMetadataPropertyType.RDBMS_CONNECTION)) {
      ICheckResult remark =
          checkConnectionName(
              property.value(), ownerKind, ownerName, source, variables, serializer);
      if (remark != null) {
        remarks.add(remark);
      }
    }
    return remarks;
  }

  /**
   * Why the lookup failed, in one line fit for a table cell.
   *
   * <p>A Hop exception carries the message of everything it wrapped, over several lines and with
   * the root cause repeated once per level. That reads badly in the problems list, so this takes
   * the root cause alone, on a single line, and caps it.
   *
   * @param e the failure
   * @return a short single line reason, never null
   */
  private static String reason(Throwable e) {
    Throwable root = e;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String message = root.getMessage();
    if (message == null || message.isBlank()) {
      message = root.getClass().getSimpleName();
    }
    String oneLine = message.replaceAll("\\s+", " ").trim();
    return oneLine.length() > MAX_REASON_LENGTH
        ? oneLine.substring(0, MAX_REASON_LENGTH - 3) + "..."
        : oneLine;
  }

  private static ICheckResult checkConnectionName(
      String rawName,
      String ownerKind,
      String ownerName,
      ICheckResultSource source,
      IVariables variables,
      IHopMetadataSerializer<DatabaseMeta> serializer) {
    if (Utils.isEmpty(rawName)) {
      return new CheckResult(
          ICheckResult.TYPE_RESULT_WARNING,
          ERROR_NOT_ASSIGNED,
          BaseMessages.getString(
              PKG, "ReferencedDatabaseConnectionChecker.NotAssigned", ownerKind, ownerName),
          source);
    }

    String resolved = variables != null ? variables.resolve(rawName) : rawName;
    if (Utils.isEmpty(resolved)) {
      return new CheckResult(
          ICheckResult.TYPE_RESULT_WARNING,
          ERROR_NOT_ASSIGNED,
          BaseMessages.getString(
              PKG, "ReferencedDatabaseConnectionChecker.NotAssigned", ownerKind, ownerName),
          source);
    }
    if (StringUtil.containsVariableToken(resolved)) {
      return null;
    }

    try {
      if (serializer.exists(resolved)) {
        return null;
      }
    } catch (Exception e) {
      // Reaching the metadata failed, which says nothing about whether this connection is in it.
      // Reporting it as missing turns one unreachable metadata folder into a warning on every
      // connection in the project - see issue #8295. Saying nothing at all would be worse still:
      // an unreadable metadata folder would produce a clean report that means nothing. So say
      // what is actually known - that the connection could not be checked - at INFO.
      if (HopLogStore.isInitialized()) {
        LogChannel.GENERAL.logDebug(
            "Unable to look up database connection '"
                + resolved
                + "' referenced by "
                + ownerKind
                + " '"
                + ownerName
                + "' : "
                + e.getMessage());
      }
      return new CheckResult(
          ICheckResult.TYPE_RESULT_COMMENT,
          INFO_NOT_VERIFIED,
          BaseMessages.getString(
              PKG,
              "ReferencedDatabaseConnectionChecker.NotVerified",
              resolved,
              ownerKind,
              ownerName,
              reason(e)),
          source);
    }

    return new CheckResult(
        ICheckResult.TYPE_RESULT_WARNING,
        ERROR_DOES_NOT_EXIST,
        BaseMessages.getString(
            PKG,
            "ReferencedDatabaseConnectionChecker.DoesNotExist",
            resolved,
            ownerKind,
            ownerName),
        source);
  }
}
