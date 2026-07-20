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
import org.eclipse.swt.graphics.Region;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;

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
   * Overlay marking where a split-on-drop would land; a hollow frame (see {@link #overlayRegion})
   * so the cursor passes through its centre to the folder underneath instead of stealing the drag.
   */
  private Shell zoneOverlay;

  /** The frame-shaped region applied to {@link #zoneOverlay}; disposed with it. */
  private Region overlayRegion;

  /** Zone/folder the overlay currently reflects, so we only touch the Shell when they change. */
  private int shownZone = IHopPerspective.DROP_ZONE_CENTER;

  private CTabFolder shownFolder;

  public TabItemReorder(IHopPerspective perspective, CTabFolder folder) {
    this.perspective = perspective;

    // Remember which tab the pointer went down on: dragStart can't reliably re-derive it from the
    // cursor location on the first macOS drag of a session.
    folder.addListener(
        SWT.MouseDown,
        e -> {
          if (e.button == 1) {
            mouseDownItem = folder.getItem(new Point(e.x, e.y));
          }
        });

    final DragSource source = new DragSource(folder, DND.DROP_MOVE);
    source.setTransfer(TabTransfer.INSTANCE);
    source.addDragListener(
        new DragSourceListener() {
          private Image dragImage;

          @Override
          public void dragStart(DragSourceEvent event) {
            dragItem = itemBeingDragged(folder);

            if (dragItem == null) {
              // Couldn't identify the tab (e.g. drag not started from a tab): cancel cleanly rather
              // than begin a data-less drag that would silently do nothing on drop.
              event.doit = false;
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
        TabTransfer.INSTANCE,
        TextTransfer.getInstance(),
        FileTransfer.getInstance(),
        MetadataTransfer.INSTANCE);

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
          private boolean isMetadataDrop;

          @Override
          public void dragEnter(DropTargetEvent event) {
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
                !isFileDrop && !isMetadataDrop && (dragItem != null || hasActiveTabTransfer(event));
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
              updateZoneOverlay(folder);
            } else {
              clearDropFeedback(folder);
            }
          }

          @Override
          public void drop(DropTargetEvent event) {
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
            boolean tabDrag = dragItem != null || hasActiveTabTransfer(event);
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
            if (dragItem != null && !dragItem.isDisposed()) {
              // Use the event's own coordinates rather than Display.getCursorLocation(): the latter
              // can read stale on the first macOS drag, wrongly collapsing an edge drop to CENTER
              // and forcing event.detail to DROP_NONE (the "first drop does nothing" bug).
              Point point = eventPoint(folder, event);
              if (folder.getItem(point) != null) {
                return true;
              }
              // Allow an edge drop to split. A same-folder split must leave a tab behind (>1 tab);
              // a cross-folder drop can always land, so only require an edge zone there.
              boolean sameFolder = dragItem.getParent() == folder;
              boolean edge = computeDropZone(folder, point) != IHopPerspective.DROP_ZONE_CENTER;
              return edge && (!sameFolder || folder.getItemCount() > 1);
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
    return folder.getItem(folder.toControl(folder.getDisplay().getCursorLocation()));
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
    // Drag-to-split relies on native DnD + floating overlays, which don't behave under RAP, so on
    // the web every drop is a plain centre drop (no edge splits). This is the single choke point
    // for edge zones (drag feedback, drop routing and isDropSupported all go through here).
    if (EnvironmentUtils.getInstance().isWeb()) {
      return IHopPerspective.DROP_ZONE_CENTER;
    }
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
    hideZoneOverlay();
  }

  /** Show (or move) the translucent overlay marking where an edge-drop split would land. */
  private void updateZoneOverlay(CTabFolder folder) {
    if (EnvironmentUtils.getInstance().isWeb()) {
      return; // Floating overlays have no faithful equivalent under RAP.
    }
    if (dropZone == IHopPerspective.DROP_ZONE_CENTER || folder.isDisposed()) {
      hideZoneOverlay();
      return;
    }
    // Nothing changed since the overlay was last shown: leave the Shell untouched. Repositioning it
    // on every drag-over event (they fire continuously) is what makes it flicker.
    if (dropZone == shownZone
        && folder == shownFolder
        && zoneOverlay != null
        && !zoneOverlay.isDisposed()
        && zoneOverlay.getVisible()) {
      return;
    }
    Rectangle r = zoneRectangleDisplay(folder, dropZone);
    if (r == null || r.width <= 0 || r.height <= 0) {
      hideZoneOverlay();
      return;
    }
    try {
      if (zoneOverlay == null || zoneOverlay.isDisposed()) {
        zoneOverlay = new Shell(folder.getShell(), SWT.NO_TRIM | SWT.ON_TOP);
        zoneOverlay.setBackground(folder.getDisplay().getSystemColor(SWT.COLOR_LIST_SELECTION));
        zoneOverlay.addDisposeListener(e -> disposeOverlayRegion());
      }
      zoneOverlay.setBounds(r);
      applyFrameRegion(r.width, r.height);
      if (!zoneOverlay.getVisible()) {
        zoneOverlay.setVisible(true);
      }
      shownZone = dropZone;
      shownFolder = folder;
    } catch (Exception e) {
      hideZoneOverlay();
    }
  }

  /**
   * Shape {@link #zoneOverlay} as a hollow rectangle frame of the given size. The cut-out centre is
   * not part of the window, so the drag cursor passes through it to the folder underneath (no
   * enter/leave oscillation), and only the thin border is painted (no compositing flicker).
   */
  private void applyFrameRegion(int width, int height) {
    int border = Math.max(3, Math.min(8, Math.min(width, height) / 12));
    Region region = new Region(zoneOverlay.getDisplay());
    region.add(0, 0, width, height);
    if (width > 2 * border && height > 2 * border) {
      region.subtract(border, border, width - 2 * border, height - 2 * border);
    }
    zoneOverlay.setRegion(region);
    disposeOverlayRegion();
    overlayRegion = region;
  }

  private void disposeOverlayRegion() {
    if (overlayRegion != null && !overlayRegion.isDisposed()) {
      overlayRegion.dispose();
    }
    overlayRegion = null;
  }

  private void hideZoneOverlay() {
    if (zoneOverlay != null && !zoneOverlay.isDisposed() && zoneOverlay.getVisible()) {
      zoneOverlay.setVisible(false);
    }
    shownZone = IHopPerspective.DROP_ZONE_CENTER;
    shownFolder = null;
  }

  /**
   * The half of the folder (in display coordinates) that a split-drop in {@code zone} would use.
   */
  private Rectangle zoneRectangleDisplay(CTabFolder folder, int zone) {
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
    Point topLeft = folder.toDisplay(x, y);
    return new Rectangle(topLeft.x, topLeft.y, w, h);
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
