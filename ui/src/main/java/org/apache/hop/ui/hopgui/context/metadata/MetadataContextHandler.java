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

package org.apache.hop.ui.hopgui.context.metadata;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

public class MetadataContextHandler implements IGuiContextHandler {

  private static final Class<?> PKG = MetadataContextHandler.class;

  public static final String CONTEXT_ID = "HopGuiMetadataContext";
  public static final String CONST_METADATA = "Metadata";

  private HopGui hopGui;
  private IHopMetadataProvider metadataProvider;
  private Class<? extends IHopMetadata> metadataObjectClass;
  private MetadataManager<? extends IHopMetadata> metadataManager;

  public MetadataContextHandler(
      HopGui hopGui,
      IHopMetadataProvider metadataProvider,
      Class<? extends IHopMetadata> metadataObjectClass) {
    this.hopGui = hopGui;
    this.metadataProvider = metadataProvider;
    this.metadataObjectClass = metadataObjectClass;

    metadataManager =
        new MetadataManager<>(
            hopGui.getVariables(), metadataProvider, metadataObjectClass, hopGui.getShell());
    metadataManager.setClassLoader(metadataObjectClass.getClassLoader());
  }

  @Override
  public String getContextId() {
    return CONTEXT_ID;
  }

  @Override
  public List<GuiAction> getSupportedActions() {

    HopMetadata hopMetadata = HopMetadataUtil.getHopMetadataAnnotation(metadataObjectClass);

    List<GuiAction> actions = new ArrayList<>();

    GuiAction newAction =
        new GuiAction(
            "CREATE_" + TranslateUtil.translate(hopMetadata.name(), metadataObjectClass),
            GuiActionType.Create,
            TranslateUtil.translate(hopMetadata.name(), metadataObjectClass),
            "Creates a new "
                + TranslateUtil.translate(hopMetadata.name(), metadataObjectClass)
                + " : "
                + TranslateUtil.translate(hopMetadata.description(), metadataObjectClass),
            hopMetadata.image(),
            (shiftClicked, controlClicked, parameters) ->
                metadataManager.newMetadataWithEditor(""));
    newAction.setClassLoader(metadataObjectClass.getClassLoader());
    newAction.setCategory(CONST_METADATA);
    newAction.setCategoryOrder("2");
    actions.add(newAction);

    GuiAction editAction =
        new GuiAction(
            "EDIT_" + TranslateUtil.translate(hopMetadata.name(), metadataObjectClass),
            GuiActionType.Modify,
            TranslateUtil.translate(hopMetadata.name(), metadataObjectClass),
            "Edits a "
                + TranslateUtil.translate(hopMetadata.name(), metadataObjectClass)
                + " : "
                + TranslateUtil.translate(hopMetadata.description(), metadataObjectClass),
            hopMetadata.image(),
            (shiftClicked, controlClicked, parameters) -> metadataManager.editMetadata());
    editAction.setClassLoader(metadataObjectClass.getClassLoader());
    editAction.setCategory(CONST_METADATA);
    editAction.setCategoryOrder("2");
    actions.add(editAction);

    GuiAction deleteAction =
        new GuiAction(
            "DELETE_" + TranslateUtil.translate(hopMetadata.name(), metadataObjectClass),
            GuiActionType.Delete,
            TranslateUtil.translate(hopMetadata.name(), metadataObjectClass),
            "After confirmation this deletes a "
                + TranslateUtil.translate(hopMetadata.name(), metadataObjectClass)
                + " : "
                + TranslateUtil.translate(hopMetadata.description(), metadataObjectClass),
            hopMetadata.image(),
            (shiftClicked, controlClicked, parameters) -> metadataManager.deleteMetadata());
    deleteAction.setClassLoader(metadataObjectClass.getClassLoader());
    deleteAction.setCategory(CONST_METADATA);
    deleteAction.setCategoryOrder("2");
    actions.add(deleteAction);

    // Database meta
    if (metadataObjectClass.isAssignableFrom(DatabaseMeta.class)) {
      GuiAction databaseClearCacheAction =
          new GuiAction(
              "DATABASE_CLEAR_CACHE",
              GuiActionType.Custom,
              BaseMessages.getString(PKG, "HopGui.Context.Database.Menu.ClearDatabaseCache.Label"),
              BaseMessages.getString(
                  PKG, "HopGui.Context.Database.Menu.ClearDatabaseCache.Tooltip"),
              null,
              (shiftClicked, controlClicked, parameters) ->
                  DbCache.getInstance().clear((String) parameters[0]));
      newAction.setClassLoader(metadataObjectClass.getClassLoader());
      newAction.setCategory(CONST_METADATA);
      newAction.setCategoryOrder("3");
      actions.add(databaseClearCacheAction);
    }

    return actions;
  }
}
