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

package org.apache.hop.ui.hopgui.perspective;

import java.nio.charset.Charset;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.dnd.ByteArrayTransfer;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DragSourceListener;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.dnd.DropTargetListener;
import org.eclipse.swt.dnd.FileTransfer;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.TransferData;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Listener;

public class TabItemReorder {
  private final IHopPerspective perspective;
  private CTabItem dragItem;

  /**
   * Tab under the cursor during a tab drag; drop will swap with this tab. Painted as drop
   * indicator.
   */
  private CTabItem dropTargetTab;

  public TabItemReorder(IHopPerspective perspective, CTabFolder folder) {
    this.perspective = perspective;

    final DragSource source = new DragSource(folder, DND.DROP_MOVE);
    source.setTransfer(TabTransfer.INSTANCE);
    source.addDragListener(
        new DragSourceListener() {
          private Image dragImage;

          @Override
          public void dragStart(DragSourceEvent event) {
            Point point = folder.toControl(folder.getDisplay().getCursorLocation());
            dragItem = folder.getItem(point);

            if (dragItem == null) {
              return;
            }
            Rectangle columnBounds = dragItem.getBounds();
            if (dragImage != null) {
              dragImage.dispose();
              dragImage = null;
            }
            if (EnvironmentUtils.getInstance().isWeb()) {
              dragImage = GuiResource.getInstance().getImageHop();
            } else {
              GC gc = null;
              try {
                gc = new GC(folder);
                dragImage =
                    new Image(Display.getCurrent(), columnBounds.width, columnBounds.height);
                gc.copyArea(dragImage, columnBounds.x, columnBounds.y);
                gc.dispose();
              } finally {
                if (gc != null) {
                  gc.dispose();
                }
              }
            }

            event.image = dragImage;
          }

          @Override
          public void dragSetData(DragSourceEvent event) {
            event.data = dragItem;
          }

          @Override
          public void dragFinished(DragSourceEvent event) {
            dragItem = null;
            if (EnvironmentUtils.getInstance().isWeb()) {
              return;
            }
            if (dragImage != null) {
              dragImage.dispose();
              dragImage = null;
            }
          }
        });

    DropTarget dropTarget = new DropTarget(folder, DND.DROP_MOVE | DND.DROP_COPY | DND.DROP_LINK);
    dropTarget.setTransfer(
        TabTransfer.INSTANCE, TextTransfer.getInstance(), FileTransfer.getInstance());

    // Paint a drop indicator (highlight) on the tab we're about to swap with
    Listener paintListener =
        event -> {
          if (dropTargetTab == null || dragItem == null || dropTargetTab.isDisposed()) {
            return;
          }
          Rectangle b = dropTargetTab.getBounds();
          if (b.width <= 0 || b.height <= 0) {
            return;
          }
          GC gc = event.gc;
          gc.setLineWidth(2);
          gc.setForeground(folder.getDisplay().getSystemColor(SWT.COLOR_LIST_SELECTION));
          gc.drawRectangle(b.x, b.y, b.width, b.height);
        };
    folder.addListener(SWT.Paint, paintListener);

    dropTarget.addDropListener(
        new DropTargetListener() {
          private boolean isFileDrop;

          @Override
          public void dragEnter(DropTargetEvent event) {
            isFileDrop = isFileTransferType(event);
            if (isFileDrop) {
              event.currentDataType = getFileTransferDataType(event);
              if (event.detail == DND.DROP_DEFAULT) {
                event.detail = preferredFileDropOperation(event);
              }
            }
            handleDragEvent(event);
          }

          @Override
          public void dragLeave(DropTargetEvent event) {
            handleDragEvent(event);
            if (dropTargetTab != null) {
              dropTargetTab = null;
              folder.redraw();
            }
          }

          @Override
          public void dragOperationChanged(DropTargetEvent event) {
            if (isFileDrop) {
              if (event.detail == DND.DROP_DEFAULT) {
                event.detail = preferredFileDropOperation(event);
              }
            }
            handleDragEvent(event);
          }

          @Override
          public void dragOver(DropTargetEvent event) {
            if (!isFileDrop) {
              isFileDrop = isFileTransferType(event);
              if (isFileDrop) {
                event.currentDataType = getFileTransferDataType(event);
              }
            }
            if (isFileDrop) {
              if (event.detail == DND.DROP_DEFAULT) {
                event.detail = preferredFileDropOperation(event);
              }
            }
            handleDragEvent(event);
            // Update drop indicator for tab reorder
            if (!isFileDrop && dragItem != null && event.detail != DND.DROP_NONE) {
              Point p = folder.toControl(folder.getDisplay().getCursorLocation());
              CTabItem over = folder.getItem(p);
              CTabItem newTarget = (over != null && over != dragItem) ? over : null;
              if (newTarget != dropTargetTab) {
                dropTargetTab = newTarget;
                folder.redraw();
              }
            } else if (dropTargetTab != null) {
              dropTargetTab = null;
              folder.redraw();
            }
          }

          @Override
          public void drop(DropTargetEvent event) {
            handleDragEvent(event);
            if (dropTargetTab != null) {
              dropTargetTab = null;
              folder.redraw();
            }
            if (event.data instanceof String[] paths
                && perspective instanceof IFileDropReceiver receiver) {
              perspective.setDropTargetFolder(folder);
              receiver.openDroppedFiles(paths);
              return;
            }
            if (event.detail == DND.DROP_MOVE) {
              moveTabs(folder, event);
            }
          }

          @Override
          public void dropAccept(DropTargetEvent event) {
            handleDragEvent(event);
          }

          private boolean isFileTransferType(DropTargetEvent event) {
            if (event.dataTypes == null) {
              return false;
            }
            FileTransfer ft = FileTransfer.getInstance();
            for (int i = 0; i < event.dataTypes.length; i++) {
              if (ft.isSupportedType(event.dataTypes[i])) {
                return true;
              }
            }
            return false;
          }

          private TransferData getFileTransferDataType(DropTargetEvent event) {
            if (event.dataTypes == null) {
              return null;
            }
            FileTransfer ft = FileTransfer.getInstance();
            for (int i = 0; i < event.dataTypes.length; i++) {
              if (ft.isSupportedType(event.dataTypes[i])) {
                return event.dataTypes[i];
              }
            }
            return null;
          }

          private int preferredFileDropOperation(DropTargetEvent event) {
            if ((event.operations & DND.DROP_MOVE) != 0) {
              return DND.DROP_MOVE;
            }
            if ((event.operations & DND.DROP_COPY) != 0) {
              return DND.DROP_COPY;
            }
            if ((event.operations & DND.DROP_LINK) != 0) {
              return DND.DROP_LINK;
            }
            return DND.DROP_NONE;
          }

          private void handleDragEvent(DropTargetEvent event) {
            if (isFileDrop && perspective instanceof IFileDropReceiver) {
              if (event.dataTypes != null
                  && !FileTransfer.getInstance().isSupportedType(event.currentDataType)) {
                event.currentDataType = getFileTransferDataType(event);
              }
              if (event.currentDataType != null
                  && FileTransfer.getInstance().isSupportedType(event.currentDataType)) {
                event.detail = preferredFileDropOperation(event);
                event.feedback = DND.FEEDBACK_NONE;
                return;
              }
            }
            if (!isDropSupported(folder, event)) {
              event.detail = DND.DROP_NONE;
            } else {
              event.detail = DND.DROP_MOVE;
            }
            event.feedback = DND.FEEDBACK_SELECT;
          }

          private boolean isDropSupported(CTabFolder folder, DropTargetEvent event) {
            if (dragItem != null && !dragItem.isDisposed()) {
              Point point = folder.toControl(folder.getDisplay().getCursorLocation());
              return folder.getItem(new Point(point.x, point.y)) != null;
            }
            return hasActiveTabTransfer(event);
          }

          private boolean hasActiveTabTransfer(DropTargetEvent event) {
            if (event.dataTypes == null) {
              return false;
            }
            for (TransferData td : event.dataTypes) {
              if (TabTransfer.INSTANCE.isSupportedType(td)) {
                return true;
              }
            }
            return false;
          }
        });
  }

  private void moveTabs(CTabFolder folder, DropTargetEvent event) {
    CTabItem sourceItem = this.dragItem;

    if (sourceItem == null || sourceItem.isDisposed()) {
      sourceItem = null;
    }

    if (sourceItem == null && event.data instanceof CTabItem transferredItem) {
      if (!transferredItem.isDisposed()) {
        sourceItem = transferredItem;
      }
    }

    if (sourceItem == null) {
      return;
    }

    if (sourceItem.getParent() != folder) {
      moveTabBetweenFolders(sourceItem, folder);
      return;
    }

    Point point = folder.toControl(folder.getDisplay().getCursorLocation());
    CTabItem dropItem = folder.getItem(new Point(point.x, point.y));
    if (dropItem != null) {
      Control dragControl = sourceItem.getControl();
      String dragText = sourceItem.getText();
      Image dragImage = sourceItem.getImage();
      String dragToolTip = sourceItem.getToolTipText();
      boolean dragShowClose = sourceItem.getShowClose();
      Font dragFont = sourceItem.getFont();
      IHopFileTypeHandler dragFileTypeHandler = (IHopFileTypeHandler) sourceItem.getData();
      IHopFileTypeHandler dropFileTypeHandler = (IHopFileTypeHandler) dropItem.getData();

      updateTabItemHandler(dragFileTypeHandler, dropItem);
      updateTabItemHandler(dropFileTypeHandler, sourceItem);

      sourceItem.setText(dropItem.getText());
      sourceItem.setImage(dropItem.getImage());
      sourceItem.setToolTipText(dropItem.getToolTipText());
      sourceItem.setFont(dropItem.getFont());
      sourceItem.setData(dropItem.getData());
      sourceItem.setShowClose(dropItem.getShowClose());
      sourceItem.setControl(dropItem.getControl());

      dropItem.setText(dragText);
      dropItem.setImage(dragImage);
      dropItem.setToolTipText(dragToolTip);
      dropItem.setFont(dragFont);
      dropItem.setData(dragFileTypeHandler);
      dropItem.setShowClose(dragShowClose);
      dropItem.setControl(dragControl);

      folder.setSelection(dropItem);
    }
  }

  private void moveTabBetweenFolders(CTabItem srcItem, CTabFolder dstFolder) {
    CTabFolder srcFolder = srcItem.getParent();
    Control control = srcItem.getControl();
    String text = srcItem.getText();
    Image image = srcItem.getImage();
    String tooltip = srcItem.getToolTipText();
    Font font = srcItem.getFont();
    IHopFileTypeHandler data = (IHopFileTypeHandler) srcItem.getData();
    boolean showClose = srcItem.getShowClose();

    control.setParent(dstFolder);

    CTabItem newItem = new CTabItem(dstFolder, SWT.CLOSE);
    newItem.setText(text);
    newItem.setImage(image);
    newItem.setToolTipText(tooltip);
    newItem.setFont(font);
    newItem.setData(data);
    newItem.setShowClose(showClose);
    newItem.setControl(control);

    updateTabItemHandler(data, newItem);

    srcItem.dispose();
    dstFolder.setSelection(newItem);

    perspective.onTabMovedBetweenFolders(srcFolder, dstFolder);
  }

  private void updateTabItemHandler(IHopFileTypeHandler fileTypeHandler, CTabItem tabItem) {
    for (TabItemHandler item : perspective.getItems()) {
      if (fileTypeHandler.equals(item.getTypeHandler())) {
        item.setTabItem(tabItem);
      }
    }
  }

  public static final class TabTransfer extends ByteArrayTransfer {

    public static final TabTransfer INSTANCE = new TabTransfer();
    private static final String TYPE_NAME =
        "TabTransfer.CTabItem Transfer" + System.currentTimeMillis() + ":" + INSTANCE.hashCode();
    private static final int TYPEID = registerType(TYPE_NAME);
    private CTabItem item;
    private long startTime;

    private TabTransfer() {}

    @Override
    protected int[] getTypeIds() {
      return new int[] {TYPEID};
    }

    @Override
    protected String[] getTypeNames() {
      return new String[] {TYPE_NAME};
    }

    @Override
    public void javaToNative(Object object, TransferData transferData) {
      item = (CTabItem) object;
      startTime = System.currentTimeMillis();
      if (transferData != null) {
        super.javaToNative(
            String.valueOf(startTime).getBytes(Charset.defaultCharset()), transferData);
      }
    }

    @Override
    public Object nativeToJava(TransferData transferData) {
      byte[] bytes = (byte[]) super.nativeToJava(transferData);
      if (bytes == null) {
        return null;
      }
      long startTime = Long.parseLong(new String(bytes));
      return (this.startTime == startTime) ? item : null;
    }
  }
}
