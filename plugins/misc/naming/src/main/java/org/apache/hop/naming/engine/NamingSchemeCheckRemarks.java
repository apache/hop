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

package org.apache.hop.naming.engine;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.naming.engine.NamingSchemeValidator.Finding;
import org.apache.hop.naming.engine.NamingSchemeValidator.Severity;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;

/** Adds naming-scheme Verify remarks using the same walker as {@code hop naming-check}. */
public final class NamingSchemeCheckRemarks {

  private NamingSchemeCheckRemarks() {
    // utility
  }

  public static void addRemarks(
      Object root, String location, List<ICheckResult> remarks, IHopMetadataProvider provider) {
    if (root == null || remarks == null || provider == null) {
      return;
    }
    try {
      List<NamingScheme> schemes = provider.getSerializer(NamingScheme.class).loadAll();
      if (schemes == null || schemes.isEmpty()) {
        return;
      }
      for (Finding finding : NamingSchemeWalker.walk(root, location, schemes, null)) {
        if (finding.getSeverity() != Severity.ERROR) {
          continue;
        }
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR, finding.getMessage(), sourceOf(root, finding)));
      }
    } catch (Exception e) {
      // Verify must still complete when naming metadata is unavailable
    }
  }

  private static ICheckResultSource sourceOf(Object root, Finding finding) {
    String type = Const.NVL(finding.getTypeCode(), "");
    String actual = finding.getActual();
    if ("hop-transform".equals(type) && root instanceof PipelineMeta pipeline) {
      TransformMeta transform = pipeline.findTransform(actual);
      if (transform != null) {
        return transform;
      }
    }
    if ("hop-action".equals(type) && root instanceof WorkflowMeta workflow) {
      ActionMeta actionMeta = workflow.findAction(actual);
      if (actionMeta != null) {
        IAction action = actionMeta.getAction();
        if (action instanceof ICheckResultSource source) {
          return source;
        }
      }
    }
    return root instanceof ICheckResultSource source ? source : null;
  }
}
