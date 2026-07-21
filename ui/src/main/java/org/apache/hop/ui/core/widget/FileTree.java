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

package org.apache.hop.ui.core.widget;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.util.ReflectionUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/**
 * This widget created a file tree with checkboxes, it can also find dependent files when selecting
 * one
 */
public class FileTree extends Composite {
  private static final Class<?> PKG = FileTree.class;

  @Getter private Set<FileObject> fileObjects;
  private final AtomicBoolean findDependencies;
  private static final String FILE_OBJECT = "fileObject";
  private static final String FOLDER = "folder";
  private static final String FILE = "file";
  private final Tree tree;

  /**
   * Guard against re-entrant SWT CHECK events. Programmatic {@link TreeItem#setChecked(boolean)}
   * can fire nested selection events on some platforms (notably GTK); those must not undo a cascade
   * that is already in progress.
   */
  private boolean ignoreCheckEvents;

  public FileTree(Composite composite, FileObject rootFolder, String rootFolderName)
      throws FileSystemException {
    super(composite, SWT.NONE);
    this.fileObjects = new LinkedHashSet<>();
    this.findDependencies = new AtomicBoolean(true);

    this.setLayout(new FormLayout());
    PropsUi.setLook(this);

    Button btnFindDependencies = new Button(this, SWT.CHECK);
    PropsUi.setLook(btnFindDependencies);
    btnFindDependencies.setText(
        BaseMessages.getString(PKG, "FileTreeWidget.IncludeDependencies.Label"));
    btnFindDependencies.setSelection(true);
    btnFindDependencies.addListener(
        SWT.Selection, event -> findDependencies.set(btnFindDependencies.getSelection()));
    btnFindDependencies.setLayoutData(new FormDataBuilder().left().fullWidth().bottom().result());

    tree = new HopTree(this, SWT.CHECK | SWT.MULTI | SWT.BORDER);
    PropsUi.setLook(tree);
    tree.setLayoutData(
        new FormDataBuilder()
            .top()
            .left()
            .right()
            .bottom(btnFindDependencies, -PropsUi.getMargin())
            .result());

    TreeItem rootItem = new TreeItem(tree, SWT.NONE);
    rootItem.setText(rootFolderName);
    rootItem.setImage(GuiResource.getInstance().getImageFolder());
    rootItem.setData("type", FOLDER);
    rootItem.setData(FILE_OBJECT, rootFolder);
    populateFolder(rootFolder, rootItem);

    tree.addListener(
        SWT.Selection,
        event -> {
          if (event.detail != SWT.CHECK || ignoreCheckEvents) {
            return;
          }
          TreeItem item = (TreeItem) event.item;
          if (item == null || item.isDisposed()) {
            return;
          }
          boolean checked = item.getChecked();
          // Snapshot multi-selection before we change checkboxes / deselect.
          TreeItem[] multiSelection = tree.getSelection();

          ignoreCheckEvents = true;
          try {
            // Always process the item that was checked/unchecked so folder selection
            // propagates to all children.
            selectionEvent(rootItem, item, checked);
            // Also apply the same state to any other multi-selected items.
            for (TreeItem treeItem : multiSelection) {
              if (treeItem != item && !treeItem.isDisposed()) {
                treeItem.setChecked(checked);
                selectionEvent(rootItem, treeItem, checked);
              }
            }
            tree.deselectAll();
          } finally {
            ignoreCheckEvents = false;
          }
        });

    // Select everything by default so export works without manual checkbox interaction.
    // Guard ignoreCheckEvents so programmatic setChecked does not fire nested handlers.
    ignoreCheckEvents = true;
    try {
      cascadeCheck(rootItem, true);
    } finally {
      ignoreCheckEvents = false;
    }
    expandAll(rootItem);
  }

  /** Expand the given item and all descendants so the default selection is visible. */
  private void expandAll(TreeItem item) {
    item.setExpanded(true);
    for (TreeItem child : item.getItems()) {
      if (FOLDER.equals(child.getData("type"))) {
        expandAll(child);
      }
    }
  }

  /**
   * Selection event method, combines a couple of actions
   *
   * @param rootItem the main tree
   * @param treeItem the treeItem to check
   * @param checked true when enabling a checkbox, false when disabling
   */
  private void selectionEvent(TreeItem rootItem, TreeItem treeItem, boolean checked) {
    Object type = treeItem.getData("type");
    if (FOLDER.equals(type)) {
      // Cascade to every descendant. Do not resolve pipeline/workflow dependencies while
      // cascading: every file under the folder is included, and dependency resolution would
      // re-enter checkbox updates and can undo the cascade (empty export).
      cascadeCheck(treeItem, checked);
      // Update grayed/checked state of ancestors only — cascadeCheck already set this folder.
      checkGrayedItems(treeItem.getParentItem());
    } else {
      FileObject fileObject = (FileObject) treeItem.getData(FILE_OBJECT);
      if (findDependencies.get()) {
        addDependencies(fileObject, rootItem, checked);
      }
      if (checked) {
        fileObjects.add(fileObject);
        treeItem.setGrayed(false);
        treeItem.setChecked(true);
      } else {
        fileObjects.remove(fileObject);
        treeItem.setGrayed(false);
        treeItem.setChecked(false);
      }
      checkGrayedItems(treeItem.getParentItem());
    }
  }

  /**
   * Add the folders and files in the Tree
   *
   * @param folder folder to fetch the child items for
   * @param folderItem root Tree item to attach the new subitems to
   * @throws org.apache.commons.vfs2.FileSystemException Exception if something happens to the file
   *     system
   */
  private void populateFolder(FileObject folder, TreeItem folderItem) throws FileSystemException {
    FileObject[] children = folder.getChildren();
    Arrays.sort(
        children,
        Comparator.comparing(
            child -> child.getName().getBaseName(), String.CASE_INSENSITIVE_ORDER));

    // Add the folders
    for (FileObject child : children) {
      if (child.isFolder()) {
        String baseFilename = child.getName().getBaseName();
        if (baseFilename.startsWith(".")) {
          continue;
        }
        TreeItem childFolderItem = new TreeItem(folderItem, SWT.NONE);
        childFolderItem.setImage(GuiResource.getInstance().getImageFolder());
        childFolderItem.setText(child.getName().getBaseName());
        childFolderItem.setData("type", FOLDER);
        childFolderItem.setData(FILE_OBJECT, child);
        populateFolder(child, childFolderItem);
      }
    }

    // Add Files
    for (final FileObject child : children) {
      if (child.isFile()) {
        String baseFilename = child.getName().getBaseName();
        if (baseFilename.startsWith(".")) {
          continue;
        }
        TreeItem childItem = new TreeItem(folderItem, SWT.NONE);
        childItem.setImage(GuiResource.getInstance().getImageFile());
        childItem.setText(child.getName().getBaseName());
        childItem.setData("type", FILE);
        childItem.setData(FILE_OBJECT, child);
      }
    }
  }

  /**
   * Search for the linked workflow/pipelines
   *
   * @param file to search dependencies for
   * @param rootItem the tree to mark the files as included
   * @param checked true when enabling a checkbox, false when disabling
   */
  void addDependencies(FileObject file, TreeItem rootItem, boolean checked) {
    List<FileObject> dependencies = new ArrayList<>();
    if (file.getName().getURI().endsWith(".hwf") || file.getName().getURI().endsWith(".hpl")) {
      dependencies.addAll(findDependencies(file.getName().getURI()));
    }
    for (FileObject dependency : dependencies) {
      checkItemsByFileObject(rootItem, dependency.getName().getURI(), checked);
    }
    if (checked) {
      fileObjects.addAll(dependencies);
    } else {
      dependencies.forEach(fileObjects::remove);
    }
  }

  /**
   * Recursively set the checked state of a folder and all descendants, and update {@link
   * #fileObjects} for every file under that folder.
   *
   * @param item folder or file tree item
   * @param checked true when enabling a checkbox, false when disabling
   */
  void cascadeCheck(TreeItem item, boolean checked) {
    item.setGrayed(false);
    item.setChecked(checked);
    for (TreeItem child : item.getItems()) {
      Object type = child.getData("type");
      if (FILE.equals(type)) {
        FileObject fileObject = (FileObject) child.getData(FILE_OBJECT);
        if (checked) {
          fileObjects.add(fileObject);
        } else {
          fileObjects.remove(fileObject);
        }
      }
      cascadeCheck(child, checked);
    }
  }

  /**
   * Set Grayed to checkboxes on the given folder and its ancestors, based on child state.
   *
   * @param parentItem The start folder to start checking (typically the parent of the changed item)
   */
  void checkGrayedItems(TreeItem parentItem) {
    if (parentItem == null || parentItem.isDisposed()) {
      return;
    }
    TreeItem[] items = parentItem.getItems();
    boolean allChecked = items.length > 0;
    boolean atLeastOneChecked = false;
    for (TreeItem treeItem : items) {
      if (!treeItem.getChecked() || treeItem.getGrayed()) {
        allChecked = false;
      }
      atLeastOneChecked = treeItem.getChecked() || atLeastOneChecked;
    }
    if (allChecked) {
      parentItem.setGrayed(false);
      parentItem.setChecked(true);
    } else if (atLeastOneChecked) {
      parentItem.setGrayed(true);
      parentItem.setChecked(true);
    } else {
      parentItem.setGrayed(false);
      parentItem.setChecked(false);
    }
    checkGrayedItems(parentItem.getParentItem());
  }

  /**
   * search for linked pipelines/workflows for a specific filepath This will do a recursive search
   * and find all linked objects
   *
   * @param filePath to search linked object for
   * @return a list of file objects that are linked to the current file
   */
  public List<FileObject> findDependencies(String filePath) {
    List<FileObject> dependencies = new ArrayList<>();

    List<HopMetadataPropertyType> properties =
        new ArrayList<>(
            Arrays.asList(
                HopMetadataPropertyType.PIPELINE_FILE,
                HopMetadataPropertyType.WORKFLOW_FILE,
                HopMetadataPropertyType.HOP_FILE));

    try {
      HopGui hopGui = HopGui.getInstance();
      if (filePath.endsWith("hpl")) {
        PipelineMeta pipelineMeta =
            new PipelineMeta(filePath, hopGui.getMetadataProvider(), hopGui.getVariables());
        List<TransformMeta> transformMetasMetas = pipelineMeta.getTransforms();
        for (TransformMeta transformMeta : transformMetasMetas) {
          List<Field> fields =
              getAllFields(new LinkedList<>(), transformMeta.getTransform().getClass());

          fields.removeIf(
              field ->
                  !field.isAnnotationPresent(HopMetadataProperty.class)
                      || !properties.contains(
                          field
                              .getAnnotation(HopMetadataProperty.class)
                              .hopMetadataPropertyType()));

          for (Field field : fields) {
            String fileFound =
                hopGui
                    .getVariables()
                    .resolve(
                        (String)
                            ReflectionUtil.getFieldValue(
                                transformMeta.getTransform(), field.getName(), false));

            if (fileFound != null && !fileFound.equals(filePath)) {
              FileObject fileObject = HopVfs.getFileObject(fileFound);
              if (fileObject.exists() && !dependencies.contains(fileObject)) {
                dependencies.add(fileObject);
                dependencies.addAll(findDependencies(fileObject.getName().getURI()));
              }
            }
          }
        }
      } else {
        WorkflowMeta workflowMeta =
            new WorkflowMeta(hopGui.getVariables(), filePath, hopGui.getMetadataProvider());
        List<ActionMeta> actionMetas = workflowMeta.getActions();
        for (ActionMeta actionMeta : actionMetas) {
          List<Field> fields = getAllFields(new LinkedList<>(), actionMeta.getAction().getClass());

          fields.removeIf(
              field ->
                  !field.isAnnotationPresent(HopMetadataProperty.class)
                      || !properties.contains(
                          field
                              .getAnnotation(HopMetadataProperty.class)
                              .hopMetadataPropertyType()));

          for (Field field : fields) {
            String fileFound =
                hopGui
                    .getVariables()
                    .resolve(
                        (String)
                            ReflectionUtil.getFieldValue(
                                actionMeta.getAction(), field.getName(), false));
            if (fileFound != null && !fileFound.equals(filePath)) {
              FileObject fileObject = HopVfs.getFileObject(fileFound);
              if (fileObject.exists() && !dependencies.contains(fileObject)) {
                dependencies.add(fileObject);
                dependencies.addAll(findDependencies(fileObject.getName().getURI()));
              }
            }
          }
        }
      }

    } catch (HopException | FileSystemException e) {
      return dependencies;
    }
    return dependencies;
  }

  /**
   * Find and enable/disable a treeItem using the filename
   *
   * @param rootItem main Tree that needs to be updated
   * @param file to find and update
   * @param checked true when enabled, false when disabled
   */
  void checkItemsByFileObject(TreeItem rootItem, String file, boolean checked) {
    TreeItem[] children = rootItem.getItems();
    for (TreeItem treeItem : children) {
      Object type = treeItem.getData("type");
      if (FOLDER.equals(type)) {
        checkItemsByFileObject(treeItem, file, checked);
      }
      if (FILE.equals(type)
          && ((FileObject) treeItem.getData(FILE_OBJECT)).getName().getURI().equals(file)) {
        treeItem.setGrayed(false);
        treeItem.setChecked(checked);
        if (checked) {
          fileObjects.add((FileObject) treeItem.getData(FILE_OBJECT));
        } else {
          fileObjects.remove((FileObject) treeItem.getData(FILE_OBJECT));
        }
        checkGrayedItems(treeItem.getParentItem());
      }
    }
  }

  /**
   * Get all class fields and fields from superclasses
   *
   * @param fields List to populate
   * @param type the class to retrieve the fields for
   * @return a List of class fields and the fields of all super classes
   */
  public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
    fields.addAll(Arrays.asList(type.getDeclaredFields()));

    if (type.getSuperclass() != null) {
      getAllFields(fields, type.getSuperclass());
    }

    return fields;
  }
}
