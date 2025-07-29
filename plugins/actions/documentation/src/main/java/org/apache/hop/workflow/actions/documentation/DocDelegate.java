/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.workflow.actions.documentation;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.parameters.UnknownParamException;

public abstract class DocDelegate {
  protected ActionDoc action;

  public DocDelegate(ActionDoc action) {
    this.action = action;
  }

  protected String calculateTargetDocumentationFile(
      FileObject targetFolder, FileObject sourceFile) {
    String base = FilenameUtils.removeExtension(sourceFile.getName().getBaseName());

    // A workflow and a pipeline can have the same base name but a different extension
    //
    if (action.isPipeline(sourceFile)) {
      return targetFolder.getName().getPath()
          + "/"
          + base
          + "-"
          + ActionDoc.STRING_PIPELINE
          + ".md";
    }
    if (action.isWorkflow(sourceFile)) {
      return targetFolder.getName().getPath()
          + "/"
          + base
          + "-"
          + ActionDoc.STRING_WORKFLOW
          + ".md";
    }
    return targetFolder.getName().getPath()
        + "/"
        + base
        + "-"
        + sourceFile.getName().getExtension()
        + ".md";
  }

  protected String calculateTargetImageFile(String name, String type) {
    return name + "-" + type + UUID.randomUUID().toString() + ".svg";
  }

  protected String formatDate(Date date) {
    if (date == null) {
      return "";
    }
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
  }

  protected void addDetails(AbstractMeta meta, StringBuilder content, String relativeFile)
      throws UnknownParamException {
    // Add an optional description
    //
    if (StringUtils.isNotEmpty(meta.getDescription())) {
      content.append("* Description : ").append(meta.getDescription()).append(Const.CR);
    }
    if (StringUtils.isNotEmpty(meta.getExtendedDescription())) {
      content
          .append("* Extended Description : ")
          .append(meta.getExtendedDescription())
          .append(Const.CR);
    }

    // Add some other details:
    //
    content
        .append("* Filename : ")
        .append(relativeFile)
        .append(Const.CR)
        .append("* Last modified: ")
        .append(formatDate(meta.getModifiedDate()))
        .append(Const.CR);

    // Include parameters?
    //
    if (action.isIncludingParameters() && meta.listParameters().length > 0) {
      content.append("- Parameters : ").append(Const.CR);
      for (String parameterName : meta.listParameters()) {
        content
            .append("    - ")
            .append(parameterName)
            .append(" : ")
            .append(meta.getParameterDescription(parameterName))
            .append(", default is [")
            .append(meta.getParameterDefault(parameterName))
            .append("]")
            .append(Const.CR);
      }
    }
    content.append(Const.CR);
  }
}
