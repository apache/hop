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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowSvgPainter;

public class WorkflowDocDelegate extends DocDelegate {
  public WorkflowDocDelegate(ActionDoc action) {
    super(action);
  }

  public void buildWorkflowDocumentation(
      Toc toc,
      FileObject targetRootFolder,
      FileObject sourceFolder,
      FileObject targetFolder,
      String relativeName,
      FileObject file)
      throws Exception {
    String targetFileName = calculateTargetDocumentationFile(targetFolder, file);

    // Calculate the relative path vs the target folder:
    //
    String relativeSourceFile = sourceFolder.getName().getRelativeName(file.getName());

    action.logBasic(" - documenting workflow: " + relativeSourceFile + " to " + targetFileName);

    WorkflowMeta workflowMeta =
        new WorkflowMeta(action, file.getName().getPath(), action.getMetadataProvider());
    String workflowName = workflowMeta.getName();

    StringBuilder content = new StringBuilder();

    // Header
    //
    content
        .append("---")
        .append(Const.CR)
        .append("title: Workflow ")
        .append(workflowName)
        .append(Const.CR)
        .append("---")
        .append(Const.CR)
        .append(Const.CR);

    addDetails(workflowMeta, content, relativeSourceFile);

    // Add the SVG image of the workflow
    //
    String workflowSvg = WorkflowSvgPainter.generateWorkflowSvg(workflowMeta, 1.0f, action);
    String relativeSvgFilename = calculateTargetImageFile(workflowName, ActionDoc.STRING_PIPELINE);
    String svgFilename =
        targetRootFolder.getName().getPath() + "/" + ActionDoc.ASSETS_IMAGES + relativeSvgFilename;
    action.saveFile(svgFilename, workflowSvg);

    // We need the relative path from this MD file to the SVG file.
    //
    String relativeSvgPath =
        HopVfs.getFileObject(targetFileName)
            .getParent()
            .getName()
            .getRelativeName(HopVfs.getFileObject(svgFilename).getName());

    content
        .append("## Image ")
        .append(Const.CR)
        .append(Const.CR)
        .append("![*Image of workflow : ")
        .append(workflowName)
        .append("*]")
        .append("(")
        .append(relativeSvgPath)
        .append(" \"")
        .append("Workflow ")
        .append(workflowName)
        .append("\"")
        .append(")")
        .append(Const.CR)
        .append(Const.CR);

    if (action.isIncludingNotes() && !workflowMeta.getNotes().isEmpty()) {
      content.append("## Notes : ").append(Const.CR).append(Const.CR);

      List<NotePadMeta> notes = new ArrayList<>(workflowMeta.getNotes());
      notes.sort(Comparator.comparing(NotePadMeta::getNote));
      for (NotePadMeta note : notes) {
        content.append(note.getNote()).append(Const.CR);
      }
      content.append(Const.CR);
    }

    // Now that we have the content, we can save the file.
    //
    action.saveFile(targetFileName, content.toString());

    // Add it to the table of content
    //
    String relativeTargetFilename =
        targetRootFolder.getName().getRelativeName(HopVfs.getFileObject(targetFileName).getName());
    toc.getEntries()
        .add(
            new TocEntry(
                relativeName,
                "Workflow",
                workflowName,
                relativeSourceFile,
                relativeTargetFilename));
  }
}
