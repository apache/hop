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
 */

package org.apache.hop.pipeline.transforms.types;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerFile;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.capabilities.FileTypeCapabilities;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.text.BaseTextExplorerFileType;

@HopFileTypePlugin(
    id = "MarkDownExplorerFileType",
    name = "MarkDown File Type",
    description = "MarkDown file handling in the explorer perspective",
    image = "markdown.svg")
public class MarkDownExplorerFileType
    extends BaseTextExplorerFileType<TextExplorerFileTypeHandler> {
  public static final Class<?> PKG = MarkDownExplorerFileType.class;

  public static final String ACTION_ID_NEW_MARKDOWN = "NewMarkdown";

  public MarkDownExplorerFileType() {
    super(
        "MarkDown File",
        ".md",
        new String[] {"*.md"},
        new String[] {"MarkDown files"},
        FileTypeCapabilities.getCapabilities(
            IHopFileType.CAPABILITY_NEW,
            IHopFileType.CAPABILITY_SAVE,
            IHopFileType.CAPABILITY_SAVE_AS,
            IHopFileType.CAPABILITY_CLOSE,
            IHopFileType.CAPABILITY_FILE_HISTORY,
            IHopFileType.CAPABILITY_COPY,
            IHopFileType.CAPABILITY_SELECT));
  }

  @Override
  public TextExplorerFileTypeHandler createFileTypeHandler(
      HopGui hopGui, ExplorerPerspective perspective, ExplorerFile file) {
    return new TextExplorerFileTypeHandler(hopGui, perspective, file);
  }

  @Override
  public IHopFileTypeHandler newFile(HopGui hopGui, IVariables variables) throws HopException {
    ExplorerFile explorerFile = new ExplorerFile("New markdown", null, this);
    ExplorerPerspective explorerPerspective = ExplorerPerspective.getInstance();
    TextExplorerFileTypeHandler fileHandler =
        new TextExplorerFileTypeHandler(hopGui, explorerPerspective, explorerFile);

    explorerPerspective.addFile(fileHandler);
    explorerPerspective.activate();

    return fileHandler;
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    HopGui hopGui = HopGui.getInstance();

    List<IGuiContextHandler> handlers = new ArrayList<>();

    GuiAction newAction =
        new GuiAction(
            ACTION_ID_NEW_MARKDOWN,
            GuiActionType.Create,
            BaseMessages.getString(PKG, "MarkDownFileType.GuiAction.NewMarkDown.Name"),
            BaseMessages.getString(PKG, "MarkDownFileType.GuiAction.NewMarkDown.Tooltip"),
            "markdown.svg",
            (shiftClicked, controlClicked, parameters) -> {
              try {
                newFile(hopGui, hopGui.getVariables());
              } catch (Exception e) {
                new ErrorDialog(
                    hopGui.getShell(),
                    BaseMessages.getString(
                        PKG, "MarkDownFileType.ErrorDialog.NewMarkDownCreation.Header"),
                    BaseMessages.getString(
                        PKG, "MarkDownFileType.ErrorDialog.NewMarkDownCreation.Message"),
                    e);
              }
            });
    newAction.setCategory("File");
    newAction.setCategoryOrder("1");

    handlers.add(new GuiContextHandler(ACTION_ID_NEW_MARKDOWN, List.of(newAction)));

    return handlers;
  }
}
