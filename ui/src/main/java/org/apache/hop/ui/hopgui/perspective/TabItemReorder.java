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
import org.apache.hop.core.logging.LogChannel;
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

/**
 * Drag-and-drop reordering and splitting of editor tabs, on the desktop and in Hop Web alike.
 *
 * <ul>
 *   <li>Drop a tab on another tab of the same folder to reorder them.
 *   <li>Drop a tab in the outer {@link #EDGE_FRACTION} band of a folder to split it off into a new
 *       pane on that side (right / bottom / left / top).
 *   <li>Drop a tab into another open pane to move it there.
 * </ul>
 *
 * <p>All of it works in Hop Web too, with three RAP-specific adjustments: the drop frame is a child
 * composite of the folder rather than a floating shell ({@link TabDropFrame}); the tab being
 * dragged is settled from the MouseDown that RAP delivers just after the DragStart ({@link
 * #settleDragItem}); and the tab is shared across the panes of a perspective ({@link
 * #activeDragItem()}), because a pane that did not start the drag has no {@code dragItem} of its
 * own and RAP has dropped the transfer types from the event by the time the drop is accepted.
 */
public class TabItemReorder {

  /** Fraction of the folder width/height near an edge that triggers a split-on-drop. */
  private static final double EDGE_FRACTION = 0.3;

  private final IHopPerspective perspective;
  private CTabItem dragItem;

  /**
   * Tab the pointer last went down on, captured on {@code SWT.MouseDown} (which always precedes a
   * drag). {@code dragStart} uses this instead of {@link Display#getCursorLocation()}, which reads
   * stale on the first native drag of a macOS session — leaving {@code dragItem} null and the first
   * drop a silent no-op until a second try.
   */
  private CTabItem mouseDownItem;

  /**
   * True from {@code dragStart} to {@code dragFinished} of a drag that started on this folder. Hop
   * Web needs it: RAP hands the folder its {@code DragStart} before the {@code MouseDown} of the
   * same request, so {@link #dragItem} can only be settled from {@link #mouseDownItem} once the
   * drag is under way, and only by the folder the drag started on (see {@link #settleDragItem}).
   */
  private boolean dragging;

  /**
   * Tab under the cursor during a tab drag; drop will swap with this tab. Painted as drop
   * indicator.
   */
  private CTabItem dropTargetTab;

  /** Current drop zone under the cursor (one of {@link IHopPerspective}'s {@code DROP_ZONE_*}). */
  private int dropZone = IHopPerspective.DROP_ZONE_CENTER;

  /**
   * Last zone computed while genuinely dragging over the folder (updated in {@code dragOver}). Not
   * reset by {@code dragLeave}, so {@code drop} can fall back to it when the drop event's own
   * coordinates come through degenerate (seen on the first macOS drag of a session).
   */
  private int lastDragOverZone = IHopPerspective.DROP_ZONE_CENTER;

  /**
   * The frame marking where the drop would land: around the tab to swap with (Hop Web only, the
   * desktop paints that one on the folder) or around the half of the folder an edge drop would
   * split off.
   */
  private final TabDropFrame dropFrame = TabDropFrame.create();

  /** What the frame currently shows, so it is only touched when that changes. */
  private int shownZone = IHopPerspective.DROP_ZONE_CENTER;

  private CTabItem shownTab;
  private CTabFolder shownFolder;

  public TabItemReorder(IHopPerspective perspective, CTabFolder folder) {
    this.perspective = perspective;
    folder.addListener(SWT.Dispose, e -> dropFrame.dispose());

    // Remember which tab the pointer went down on: dragStart can't reliably re-derive it from the
    // cursor location on the first macOS drag of a session, and in Hop Web it runs before this
    // listener. Forget it again on release so that a later drag never picks up a stale tab.
    folder.addListener(
        SWT.MouseDown,
        e -> {
          if (e.button == 1) {
            mouseDownItem = folder.getItem(new Point(e.x, e.y));
            // In Hop Web, DragStart of the same request may have already run and left the drag
            // waiting for this tab (it runs before this listener). Settle it now, in the same
            // request as DragStart, so even a fast drop - one that reaches the target pane before
            // any drag-over settles the tab there - already has it. Otherwise the merge only works
            // when the pointer lingers long enough for a source drag-over to fire first.
            settleDragItem(folder);
          }
        });
    folder.addListener(SWT.MouseUp, e -> mouseDownItem = null);

    final DragSource source = new DragSource(folder, DND.DROP_MOVE);
    source.setTransfer(TabTransfer.INSTANCE);
    source.addDragListener(
        new DragSourceListener() {
          private Image dragImage;

          @Override
          public void dragStart(DragSourceEvent event) {
            dragItem = itemBeingDragged(folder);
            perspective.setDraggedTabItem(dragItem);

            if (dragItem == null) {
              if (EnvironmentUtils.getInstance().isWeb()) {
                // RAP delivers this before the MouseDown of the same request, and by the time the
                // drag threshold is passed the pointer has often left the tab strip. Keep the drag
                // alive: settleDragItem picks the tab up from that MouseDown on the next event.
                dragging = true;
                return;
              }
              // Couldn't identify the tab (e.g. drag not started from a tab): cancel cleanly rather
              // than begin a data-less drag that would silently do nothing on drop.
              event.doit = false;
              return;
            }
            dragging = true;
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
            settleDragItem(folder);
            event.data = dragItem;
          }

          @Override
          public void dragFinished(DragSourceEvent event) {
            dragItem = null;
            mouseDownItem = null;
            dragging = false;
            perspective.setDraggedTabItem(null);
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
        TabTransfer.INSTANCE,
        TextTransfer.getInstance(),
        FileTransfer.getInstance(),
        MetadataTransfer.INSTANCE);

    // Paint a drop indicator (highlight) on the tab we're about to swap with. RAP delivers no paint
    // events for a folder; Hop Web shows the drop frame around that tab instead (updateDropFrame).
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
          private boolean isMetadataDrop;

          @Override
          public void dragEnter(DropTargetEvent event) {
            settleDragItem(folder);
            lastDragOverZone = IHopPerspective.DROP_ZONE_CENTER;
            isFileDrop = isFileTransferType(event);
            isMetadataDrop = isMetadataTransferType(event);
            if (isFileDrop) {
              event.currentDataType = getFileTransferDataType(event);
              if (event.detail == DND.DROP_DEFAULT) {
                event.detail = preferredFileDropOperation(event);
              }
            } else if (isMetadataDrop) {
              event.currentDataType = getMetadataTransferDataType(event);
              event.detail = DND.DROP_MOVE;
            }
            handleDragEvent(event);
          }

          @Override
          public void dragLeave(DropTargetEvent event) {
            handleDragEvent(event);
            clearDropFeedback(folder);
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
            settleDragItem(folder);
            if (!isFileDrop && !isMetadataDrop) {
              isFileDrop = isFileTransferType(event);
              isMetadataDrop = isMetadataTransferType(event);
              if (isFileDrop) {
                event.currentDataType = getFileTransferDataType(event);
              } else if (isMetadataDrop) {
                event.currentDataType = getMetadataTransferDataType(event);
              }
            }
            if (isFileDrop) {
              if (event.detail == DND.DROP_DEFAULT) {
                event.detail = preferredFileDropOperation(event);
              }
            } else if (isMetadataDrop) {
              event.detail = DND.DROP_MOVE;
            }
            handleDragEvent(event);
            // Update drop indicator (tab reorder) and split zone (edge drop) feedback.
            boolean tabDrag =
                !isFileDrop
                    && !isMetadataDrop
                    && (activeDragItem() != null || hasActiveTabTransfer(event));
            if (tabDrag && event.detail != DND.DROP_NONE) {
              Point p = eventPoint(folder, event);
              CTabItem over = folder.getItem(p);
              CTabItem newTarget = (over != null && over != dragItem) ? over : null;
              if (newTarget != dropTargetTab) {
                dropTargetTab = newTarget;
                folder.redraw();
              }
              // Over the tab strip = reorder/insert (center); elsewhere = possible edge split.
              int newZone =
                  (over != null) ? IHopPerspective.DROP_ZONE_CENTER : computeDropZone(folder, p);
              // Don't advertise a split that would be a no-op: dragging the sole tab of this folder
              // to its own edge can't split (Hop keeps one tab per file).
              if (newZone != IHopPerspective.DROP_ZONE_CENTER
                  && dragItem != null
                  && dragItem.getParent() == folder
                  && folder.getItemCount() <= 1) {
                newZone = IHopPerspective.DROP_ZONE_CENTER;
              }
              dropZone = newZone;
              // Remember the zone while we're genuinely over the folder; drop() falls back to this
              // if its own event coordinates come through degenerate.
              lastDragOverZone = newZone;
              updateDropFrame(folder);
            } else {
              clearDropFeedback(folder);
            }
          }

          @Override
          public void drop(DropTargetEvent event) {
            settleDragItem(folder);
            handleDragEvent(event);
            // Resolve the drop point from the event's own display coordinates, which — unlike
            // Display.getCursorLocation() — are the actual drop location and are reliable even on
            // the first macOS drag of a session (where getCursorLocation() can read stale, making a
            // split silently no-op and needing a second drop). Fall back to the last drag-over zone
            // only when the event point is degenerate (outside the folder).
            Point dropPoint = eventPoint(folder, event);
            int zone = resolveDropZone(folder, dropPoint);
            clearDropFeedback(folder);
            if (isMetadataTransferType(event)
                && event.data instanceof String[] metadataData
                && metadataData.length >= 2
                && perspective instanceof IMetadataDropReceiver receiver) {
              receiver.openDroppedMetadata(metadataData[0], metadataData[1]);
              return;
            }
            if (event.data instanceof String[] paths
                && perspective instanceof IFileDropReceiver receiver) {
              perspective.setDropTargetFolder(folder);
              receiver.openDroppedFiles(paths);
              return;
            }
            boolean tabDrag = activeDragItem() != null || hasActiveTabTransfer(event);
            if (LogChannel.UI.isDebug() && tabDrag) {
              LogChannel.UI.logDebug(
                  "Tab drop: detail="
                      + event.detail
                      + " zone="
                      + zone
                      + " point="
                      + dropPoint
                      + " dragItem="
                      + (dragItem == null ? "null" : dragItem.getText())
                      + " targetItems="
                      + folder.getItemCount());
            }
            if (event.detail == DND.DROP_MOVE) {
              moveTabs(folder, event, zone, dropPoint);
            } else if (tabDrag && isUnambiguousTabDrop(folder, zone, dropPoint)) {
              // detail resolved to something other than MOVE (seen intermittently on the first
              // macOS drag). When the drop is unambiguous — an edge split, or a move from another
              // folder — complete it rather than silently dropping it. Ambiguous same-folder centre
              // drops are left alone to avoid a spurious self-swap.
              LogChannel.UI.logDebug(
                  "Tab drop arrived with detail=" + event.detail + "; completing unambiguous move");
              moveTabs(folder, event, zone, dropPoint);
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
            if (isMetadataDrop && perspective instanceof IMetadataDropReceiver) {
              if (event.dataTypes != null
                  && !MetadataTransfer.INSTANCE.isSupportedType(event.currentDataType)) {
                event.currentDataType = getMetadataTransferDataType(event);
              }
              if (event.currentDataType != null
                  && MetadataTransfer.INSTANCE.isSupportedType(event.currentDataType)) {
                event.detail = DND.DROP_MOVE;
                event.feedback = DND.FEEDBACK_NONE;
                return;
              }
            }
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
            CTabItem item = activeDragItem();
            if (item != null) {
              // A tab dragged in from another pane can always land here (join or split), and its
              // drop point maps unreliably across panes in Hop Web, so accept without resolving it.
              if (item.getParent() != folder) {
                return true;
              }
              // Use the event's own coordinates rather than Display.getCursorLocation(): the latter
              // can read stale on the first macOS drag, wrongly collapsing an edge drop to CENTER
              // and forcing event.detail to DROP_NONE (the "first drop does nothing" bug).
              Point point = eventPoint(folder, event);
              if (folder.getItem(point) != null) {
                return true;
              }
              // A same-folder split must leave a tab behind (>1 tab).
              boolean edge = computeDropZone(folder, point) != IHopPerspective.DROP_ZONE_CENTER;
              return edge && folder.getItemCount() > 1;
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

          private boolean isMetadataTransferType(DropTargetEvent event) {
            if (event.dataTypes == null) {
              return false;
            }
            for (TransferData td : event.dataTypes) {
              if (MetadataTransfer.INSTANCE.isSupportedType(td)) {
                return true;
              }
            }
            return false;
          }

          private TransferData getMetadataTransferDataType(DropTargetEvent event) {
            if (event.dataTypes == null) {
              return null;
            }
            for (TransferData td : event.dataTypes) {
              if (MetadataTransfer.INSTANCE.isSupportedType(td)) {
                return td;
              }
            }
            return null;
          }
        });
  }

  /**
   * The tab being dragged from {@code folder}. Prefers the tab captured on mouse-down (reliable on
   * every drag, including the first macOS drag); falls back to the cursor location only, which
   * returns a tab solely when the pointer is genuinely over one — so a drag begun in the empty body
   * resolves to null (and is cancelled) rather than grabbing the selected tab.
   */
  private CTabItem itemBeingDragged(CTabFolder folder) {
    if (mouseDownItem != null
        && !mouseDownItem.isDisposed()
        && mouseDownItem.getParent() == folder) {
      return mouseDownItem;
    }
    if (EnvironmentUtils.getInstance().isWeb()) {
      // The pointer is wherever the drag threshold was passed, which need not be the pressed tab
      // (or any tab): the MouseDown that follows is the only reliable source, see settleDragItem.
      return null;
    }
    return folder.getItem(folder.toControl(folder.getDisplay().getCursorLocation()));
  }

  /**
   * Settle the tab being dragged once the drag is under way: in Hop Web {@code dragStart} may have
   * run before the MouseDown that names the tab (see {@link #dragging}). Only the folder the drag
   * started on does this; another folder's last pressed tab has nothing to do with the drag.
   */
  private void settleDragItem(CTabFolder folder) {
    if (dragging
        && dragItem == null
        && mouseDownItem != null
        && !mouseDownItem.isDisposed()
        && mouseDownItem.getParent() == folder) {
      dragItem = mouseDownItem;
      perspective.setDraggedTabItem(dragItem);
    }
  }

  /**
   * The tab this drag is carrying: {@link #dragItem} on the folder the drag started on, or the tab
   * the perspective is holding on any other folder of the same perspective. This is what lets a
   * drop into a <em>different</em> pane complete: that pane's own {@code dragItem} is null, and in
   * Hop Web the transfer types are no longer on the drop event by the time the drop is accepted.
   */
  private CTabItem activeDragItem() {
    if (dragItem != null && !dragItem.isDisposed()) {
      return dragItem;
    }
    CTabItem shared = perspective.getDraggedTabItem();
    return (shared != null && !shared.isDisposed()) ? shared : null;
  }

  private void moveTabs(CTabFolder folder, DropTargetEvent event, int zone, Point dropPoint) {
    CTabItem sourceItem = this.dragItem;

    if (sourceItem == null || sourceItem.isDisposed()) {
      sourceItem = null;
    }

    if (sourceItem == null && event.data instanceof CTabItem transferredItem) {
      if (!transferredItem.isDisposed()) {
        sourceItem = transferredItem;
      }
    }

    // A drop into another pane: this folder was not the drag source, so its own dragItem is null
    // and (in Hop Web) the transfer may not have delivered event.data. Fall back to the tab the
    // perspective is holding for the drag.
    if (sourceItem == null) {
      CTabItem shared = perspective.getDraggedTabItem();
      if (shared != null && !shared.isDisposed()) {
        sourceItem = shared;
      }
    }

    if (sourceItem == null) {
      return;
    }

    // Edge-zone drop: ask the perspective to split the target folder and move into the new pane.
    // Skip when the source is the sole tab of the target folder (splitting would be a no-op).
    if (zone != IHopPerspective.DROP_ZONE_CENTER
        && !(sourceItem.getParent() == folder && folder.getItemCount() <= 1)) {
      CTabFolder dest = perspective.resolveDropFolderForZone(folder, zone);
      if (dest != null && !dest.isDisposed() && dest != sourceItem.getParent()) {
        moveTabBetweenFolders(sourceItem, dest);
        return;
      }
    }

    if (sourceItem.getParent() != folder) {
      moveTabBetweenFolders(sourceItem, folder);
      return;
    }

    CTabItem dropItem = folder.getItem(dropPoint);
    if (dropItem != null && dropItem != sourceItem) {
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

  /**
   * The drop point in folder-local coordinates, taken from the drop-target event's own display
   * coordinates ({@code event.x}/{@code event.y}). These are the actual drop location and stay
   * accurate even when {@link Display#getCursorLocation()} reads stale (first macOS drag of a
   * session), which is what made the first split-drop silently do nothing.
   */
  private Point eventPoint(CTabFolder folder, DropTargetEvent event) {
    return folder.toControl(event.x, event.y);
  }

  /**
   * The drop zone for a point: over a tab = CENTER, otherwise the edge band. Falls back to the last
   * zone shown during {@code dragOver} only when the point is degenerate (outside the folder), so a
   * genuine centre drop is never turned into a split.
   */
  private int resolveDropZone(CTabFolder folder, Point p) {
    Point size = folder.getSize();
    boolean inside = p.x >= 0 && p.y >= 0 && p.x <= size.x && p.y <= size.y;
    if (!inside) {
      return lastDragOverZone;
    }
    if (folder.getItem(p) != null) {
      return IHopPerspective.DROP_ZONE_CENTER;
    }
    return computeDropZone(folder, p);
  }

  /**
   * Whether a tab drop is unambiguous enough to complete even when the DnD operation resolved to
   * something other than {@code DROP_MOVE} (an intermittent first-macOS-drag quirk): an edge split,
   * or a drop landing over a real (different) tab. Same-folder centre drops with nothing under the
   * cursor are treated as ambiguous and left alone.
   */
  private boolean isUnambiguousTabDrop(CTabFolder folder, int zone, Point dropPoint) {
    if (zone != IHopPerspective.DROP_ZONE_CENTER) {
      return true;
    }
    CTabItem over = folder.getItem(dropPoint);
    return over != null && over != dragItem;
  }

  /**
   * Determine which drop zone the point falls in: the outer {@link #EDGE_FRACTION} band on each
   * side maps to that edge (split), the middle maps to {@code CENTER} (drop into the folder as-is).
   */
  private int computeDropZone(CTabFolder folder, Point p) {
    Point size = folder.getSize();
    if (size.x <= 0 || size.y <= 0) {
      return IHopPerspective.DROP_ZONE_CENTER;
    }
    double fx = p.x / (double) size.x;
    double fy = p.y / (double) size.y;
    if (fx < 0 || fx > 1 || fy < 0 || fy > 1) {
      return IHopPerspective.DROP_ZONE_CENTER;
    }
    double left = fx;
    double right = 1 - fx;
    double top = fy;
    double bottom = 1 - fy;
    double min = Math.min(Math.min(left, right), Math.min(top, bottom));
    if (min > EDGE_FRACTION) {
      return IHopPerspective.DROP_ZONE_CENTER;
    }
    if (min == left) {
      return IHopPerspective.DROP_ZONE_WEST;
    }
    if (min == right) {
      return IHopPerspective.DROP_ZONE_EAST;
    }
    if (min == top) {
      return IHopPerspective.DROP_ZONE_NORTH;
    }
    return IHopPerspective.DROP_ZONE_SOUTH;
  }

  /** Reset all drop feedback: tab-swap highlight and the split-zone overlay. */
  private void clearDropFeedback(CTabFolder folder) {
    if (dropTargetTab != null) {
      dropTargetTab = null;
      if (!folder.isDisposed()) {
        folder.redraw();
      }
    }
    dropZone = IHopPerspective.DROP_ZONE_CENTER;
    hideDropFrame();
  }

  /**
   * Show (or move) the frame marking where the drop would land, or take it down when the drop is a
   * plain centre drop with nothing to mark.
   */
  private void updateDropFrame(CTabFolder folder) {
    if (folder.isDisposed()) {
      hideDropFrame();
      return;
    }
    // The desktop paints the tab highlight itself; Hop Web gets no paint events and frames the tab.
    CTabItem tab = EnvironmentUtils.getInstance().isWeb() ? dropTargetTab : null;
    int zone = tab != null ? IHopPerspective.DROP_ZONE_CENTER : dropZone;
    if (tab == null && zone == IHopPerspective.DROP_ZONE_CENTER) {
      hideDropFrame();
      return;
    }
    // Nothing changed since the frame was last shown: leave it alone. Moving it on every drag-over
    // event (they fire continuously) is what makes it flicker.
    if (tab == shownTab && zone == shownZone && folder == shownFolder) {
      return;
    }
    Rectangle r = tab != null ? tab.getBounds() : zoneRectangle(folder, zone);
    if (r == null || r.width <= 0 || r.height <= 0) {
      hideDropFrame();
      return;
    }
    dropFrame.show(folder, r);
    shownTab = tab;
    shownZone = zone;
    shownFolder = folder;
  }

  private void hideDropFrame() {
    dropFrame.hide();
    shownTab = null;
    shownZone = IHopPerspective.DROP_ZONE_CENTER;
    shownFolder = null;
  }

  /** The half of the folder (in folder coordinates) that a split-drop in {@code zone} would use. */
  private Rectangle zoneRectangle(CTabFolder folder, int zone) {
    Point size = folder.getSize();
    if (size.x <= 0 || size.y <= 0) {
      return null;
    }
    int x = 0;
    int y = 0;
    int w = size.x;
    int h = size.y;
    switch (zone) {
      case IHopPerspective.DROP_ZONE_WEST -> w = size.x / 2;
      case IHopPerspective.DROP_ZONE_EAST -> {
        x = size.x / 2;
        w = size.x - x;
      }
      case IHopPerspective.DROP_ZONE_NORTH -> h = size.y / 2;
      case IHopPerspective.DROP_ZONE_SOUTH -> {
        y = size.y / 2;
        h = size.y - y;
      }
      default -> {
        return null;
      }
    }
    return new Rectangle(x, y, w, h);
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
