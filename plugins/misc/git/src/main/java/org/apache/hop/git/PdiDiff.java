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
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

import java.util.Map;
import java.util.Optional;

public class PdiDiff {
  public static String ATTR_GIT = "Git";
  public static String ATTR_STATUS = "Status";

  public static String UNCHANGED = "UNCHANGED";
  public static String CHANGED = "CHANGED";
  public static String REMOVED = "REMOVED";
  public static String ADDED = "ADDED";

  public static PipelineMeta compareTransforms(
      PipelineMeta transMeta1, PipelineMeta transMeta2, boolean isForward) {
    transMeta1
        .getTransforms()
        .forEach(
            transform -> {
              Optional<TransformMeta> transform2 =
                  transMeta2.getTransforms().stream()
                      .filter(obj -> transform.getName().equals(obj.getName()))
                      .findFirst();
              String status = null;
              if (transform2.isPresent()) {
                Map<String, String> tmp = null, tmp2 = null;
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
              transform.setAttribute(ATTR_GIT, ATTR_STATUS, status.toString());
            });
    return transMeta1;
  }

  public static WorkflowMeta compareJobEntries(
      WorkflowMeta jobMeta1, WorkflowMeta jobMeta2, boolean isForward) {
    jobMeta1
        .getActions()
        .forEach(
            je -> {
              Optional<ActionMeta> je2 =
                  jobMeta2.getActions().stream()
                      .filter(obj -> je.getName().equals(obj.getName()))
                      .findFirst();
              String status = null;
              if (je2.isPresent()) {
                Map<String, String> tmp = null, tmp2 = null;
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
              je.setAttribute(ATTR_GIT, ATTR_STATUS, status.toString());
            });
    return jobMeta1;
  }
}
