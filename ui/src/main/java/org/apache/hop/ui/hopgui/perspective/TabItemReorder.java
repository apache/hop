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
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.TransferData;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;

public class TabItemReorder {
  private final IHopPerspective perspective;
  private CTabItem dragItem;

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
            if (EnvironmentUtils.getInstance().isWeb()) {
              return;
            }
            if (dragImage != null) {
              dragImage.dispose();
              dragImage = null;
            }
          }
        });

    DropTarget dropTarget = new DropTarget(folder, DND.DROP_MOVE);
    dropTarget.setTransfer(TabTransfer.INSTANCE, TextTransfer.getInstance());
    dropTarget.addDropListener(
        new DropTargetListener() {
          @Override
          public void dragEnter(DropTargetEvent event) {
            handleDragEvent(event);
          }

          @Override
          public void dragLeave(DropTargetEvent event) {
            handleDragEvent(event);
          }

          @Override
          public void dragOperationChanged(DropTargetEvent event) {
            handleDragEvent(event);
          }

          @Override
          public void dragOver(DropTargetEvent event) {
            handleDragEvent(event);
          }

          @Override
          public void drop(DropTargetEvent event) {
            handleDragEvent(event);
            if (event.detail == DND.DROP_MOVE) {
              moveTabs(folder, event);
            }
          }

          @Override
          public void dropAccept(DropTargetEvent event) {
            handleDragEvent(event);
          }

          private void handleDragEvent(DropTargetEvent event) {
            if (!isDropSupported(folder, event)) {
              event.detail = DND.DROP_NONE;
            } else {
              event.detail = DND.DROP_MOVE;
            }
            event.feedback = DND.FEEDBACK_SELECT;
          }

          private boolean isDropSupported(CTabFolder folder, DropTargetEvent event) {
            if (dragItem == null) {
              return false;
            }
            Point point = folder.toControl(folder.getDisplay().getCursorLocation());
            return folder.getItem(new Point(point.x, point.y)) != null;
          }
        });
  }

  private void moveTabs(CTabFolder folder, DropTargetEvent event) {
    Point point = folder.toControl(folder.getDisplay().getCursorLocation());
    CTabItem dropItem = folder.getItem(new Point(point.x, point.y));
    if (dropItem != null && dragItem != null) {
      Control dragControl = dragItem.getControl();
      String dragText = dragItem.getText();
      Image dragImage = dragItem.getImage();
      String dragToolTip = dragItem.getToolTipText();
      boolean dragShowClose = dragItem.getShowClose();
      Font dragFont = dragItem.getFont();
      IHopFileTypeHandler dragFileTypeHandler = (IHopFileTypeHandler) dragItem.getData();
      IHopFileTypeHandler dropFileTypeHandler = (IHopFileTypeHandler) dropItem.getData();

      updateTabItemHandler(dragFileTypeHandler, dropItem);
      updateTabItemHandler(dropFileTypeHandler, dragItem);

      dragItem.setText(dropItem.getText());
      dragItem.setImage(dropItem.getImage());
      dragItem.setToolTipText(dropItem.getToolTipText());
      dragItem.setFont(dropItem.getFont());
      dragItem.setData(dropItem.getData());
      dragItem.setShowClose(dropItem.getShowClose());
      dragItem.setControl(dropItem.getControl());

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
