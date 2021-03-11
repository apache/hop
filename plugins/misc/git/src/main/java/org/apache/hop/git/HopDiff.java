/*
 * Hop : The Hop Orchestration Platform
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.git;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

import java.util.Map;
import java.util.Optional;

public class HopDiff {
  public static String ATTR_GIT = "Git";
  public static String ATTR_STATUS = "Status";
  public static String ATTR_GIT_HOPS = "GitHops";

  public static final String UNCHANGED = "UNCHANGED";
  public static final String CHANGED = "CHANGED";
  public static final String REMOVED = "REMOVED";
  public static final String ADDED = "ADDED";

  public static PipelineMeta compareTransforms(
      PipelineMeta pipelineMeta1, PipelineMeta pipelineMeta2, boolean isForward) {
    pipelineMeta1
        .getTransforms()
        .forEach(
            transform -> {
              Optional<TransformMeta> transform2 =
                  pipelineMeta2.getTransforms().stream()
                      .filter(obj -> transform.getName().equals(obj.getName()))
                      .findFirst();
              String status = null;
              if (transform2.isPresent()) {
                Map<String, String> tmp = null;
                Map<String, String> tmp2 = null;
                try {
                  // AttributeMap("Git") cannot affect the XML comparison
                  tmp = transform.getAttributesMap().remove(ATTR_GIT);
                  tmp2 = transform2.get().getAttributesMap().remove(ATTR_GIT);
                  if (transform.getXml().equals(transform2.get().getXml())) {
                    status = UNCHANGED;
                  } else {
                    status = CHANGED;
                  }
                } catch (HopException e) {
                  e.printStackTrace();
                } finally {
                  transform.setAttributes(ATTR_GIT, tmp);
                  transform2.get().setAttributes(ATTR_GIT, tmp2);
                }
              } else {
                if (isForward) {
                  status = REMOVED;
                } else {
                  status = ADDED;
                }
              }
              transform.setAttribute(ATTR_GIT, ATTR_STATUS, status );
            });
    return pipelineMeta1;
  }

  public static PipelineMeta comparePipelineHops(
    PipelineMeta pipelineMeta1, PipelineMeta pipelineMeta2, boolean isForward) {
    pipelineMeta1
        .getPipelineHops()
        .forEach(
            hop -> {
              Optional<PipelineHopMeta> hop2 =
                  pipelineMeta2.getPipelineHops().stream()
                      .filter(otherHop -> hop.toString().equals(otherHop.toString()))
                      .findFirst();
              String status = null;
              if (!hop2.isPresent()) {
                if (isForward) {
                  status = REMOVED;
                } else {
                  status = ADDED;
                }
              }
              if (status != null) {
                pipelineMeta1.setAttribute(ATTR_GIT_HOPS, hop.toString(), status);
              }
            });
    return pipelineMeta1;
  }

  public static WorkflowMeta compareActions(
      WorkflowMeta workflowMeta1, WorkflowMeta workflowMeta2, boolean isForward) {
    workflowMeta1
        .getActions()
        .forEach(
            je -> {
              Optional<ActionMeta> je2 =
                  workflowMeta2.getActions().stream()
                      .filter(obj -> je.getName().equals(obj.getName()))
                      .findFirst();
              String status = null;
              if (je2.isPresent()) {
                Map<String, String> tmp = null;
                Map<String, String> tmp2 = null;
                // AttributeMap("Git") cannot affect the XML comparison
                tmp = je.getAttributesMap().remove(ATTR_GIT);
                tmp2 = je2.get().getAttributesMap().remove(ATTR_GIT);
                if (je.getXml().equals(je2.get().getXml())) {
                  status = UNCHANGED;
                } else {
                  status = CHANGED;
                }
                je.setAttributes(ATTR_GIT, tmp);
                je2.get().setAttributes(ATTR_GIT, tmp2);
              } else {
                if (isForward) {
                  status = REMOVED;
                } else {
                  status = ADDED;
                }
              }
              je.setAttribute(ATTR_GIT, ATTR_STATUS, status );
            });
    return workflowMeta1;
  }

  public static WorkflowMeta compareWorkflowHops(
    WorkflowMeta workflowMeta1, WorkflowMeta workflowMeta2, boolean isForward) {
    workflowMeta1
        .getWorkflowHops()
        .forEach(
            hop -> {
              Optional<WorkflowHopMeta> hop2 =
                  workflowMeta2.getWorkflowHops().stream()
                      .filter(otherHop -> hop.toString().equals(otherHop.toString()))
                      .findFirst();
              String status = null;
              if (!hop2.isPresent()) {
                if (isForward) {
                  status = REMOVED;
                } else {
                  status = ADDED;
                }
              }
              if (status != null) {
                workflowMeta1.setAttribute(ATTR_GIT_HOPS, hop.toString(), status);
              }
            });
    return workflowMeta1;
  }
}
