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
package org.apache.hop.core.undo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Gzip-compressed XML snapshots of a metadata document for undo/redo.
 *
 * <p>Unlike {@link ChangeAction}, this stores the whole document so restore cannot miss a field.
 * Capture uses {@link XmlMetadataUtil#serializeObjectToXml(Object)} (no license header, no
 * formatter) so snapshots stay small and stable.
 */
public class XmlSnapshotUndo<M> {

  @FunctionalInterface
  public interface ContentRestorer<M> {
    void restore(M target, Node xmlRoot, IHopMetadataProvider metadataProvider, String filename)
        throws HopException;
  }

  private final Class<M> modelClass;
  private final String xmlRootTag;
  private final ContentRestorer<M> restorer;
  private final IntSupplier maxUndo;

  private final List<byte[]> undoStack = new ArrayList<>();
  private final List<byte[]> redoStack = new ArrayList<>();
  private boolean applyingSnapshot;

  public XmlSnapshotUndo(
      Class<M> modelClass, String xmlRootTag, ContentRestorer<M> restorer, IntSupplier maxUndo) {
    this.modelClass = modelClass;
    this.xmlRootTag = xmlRootTag;
    this.restorer = restorer;
    this.maxUndo = maxUndo != null ? maxUndo : () -> Const.MAX_UNDO;
  }

  public void clear() {
    undoStack.clear();
    redoStack.clear();
  }

  public boolean canUndo() {
    return !undoStack.isEmpty();
  }

  public boolean canRedo() {
    return !redoStack.isEmpty();
  }

  public boolean isApplyingSnapshot() {
    return applyingSnapshot;
  }

  public int getUndoSize() {
    return undoStack.size();
  }

  public int getRedoSize() {
    return redoStack.size();
  }

  public void markChange(M model, IHopMetadataProvider metadataProvider) throws HopException {
    if (applyingSnapshot || model == null) {
      return;
    }
    pushSnapshot(captureSnapshot(model, metadataProvider));
  }

  public void pushSnapshot(byte[] snapshot) {
    if (applyingSnapshot || snapshot == null) {
      return;
    }
    undoStack.add(snapshot);
    trimStack(undoStack);
    redoStack.clear();
  }

  public byte[] captureSnapshot(M model, IHopMetadataProvider metadataProvider)
      throws HopException {
    if (model == null) {
      throw new HopException("Cannot capture snapshot of a null model");
    }
    try {
      String xml = XmlHandler.aroundTag(xmlRootTag, XmlMetadataUtil.serializeObjectToXml(model));
      return compress(xml);
    } catch (Exception e) {
      throw new HopException("Error capturing " + modelClass.getSimpleName() + " snapshot", e);
    }
  }

  /**
   * Restore the previous snapshot into {@code current}. The live document is pushed onto the redo
   * stack first.
   *
   * @return {@code true} if a snapshot was applied
   */
  public boolean undo(M current, IHopMetadataProvider metadataProvider, String filename)
      throws HopException {
    if (!canUndo() || current == null) {
      return false;
    }
    applyingSnapshot = true;
    try {
      redoStack.add(captureSnapshot(current, metadataProvider));
      trimStack(redoStack);
      byte[] previous = undoStack.remove(undoStack.size() - 1);
      restoreInto(previous, current, metadataProvider, filename);
      return true;
    } finally {
      applyingSnapshot = false;
    }
  }

  /**
   * Restore the next redo snapshot into {@code current}. The live document is pushed onto the undo
   * stack first.
   *
   * @return {@code true} if a snapshot was applied
   */
  public boolean redo(M current, IHopMetadataProvider metadataProvider, String filename)
      throws HopException {
    if (!canRedo() || current == null) {
      return false;
    }
    applyingSnapshot = true;
    try {
      undoStack.add(captureSnapshot(current, metadataProvider));
      trimStack(undoStack);
      byte[] next = redoStack.remove(redoStack.size() - 1);
      restoreInto(next, current, metadataProvider, filename);
      return true;
    } finally {
      applyingSnapshot = false;
    }
  }

  public void restoreInto(
      byte[] snapshot, M target, IHopMetadataProvider metadataProvider, String filename)
      throws HopException {
    try {
      String xml = decompress(snapshot);
      Document document = XmlHandler.loadXmlString(xml);
      Node rootNode = XmlHandler.getSubNode(document, xmlRootTag);
      if (rootNode == null) {
        rootNode = document.getDocumentElement();
      }
      restorer.restore(target, rootNode, metadataProvider, filename);
    } catch (Exception e) {
      throw new HopException("Error restoring " + modelClass.getSimpleName() + " snapshot", e);
    }
  }

  /**
   * Gzip headers include a timestamp, so compressed bytes of identical XML are not equal. Compare
   * inflated XML instead.
   */
  public static boolean sameXmlContent(byte[] left, byte[] right) {
    if (left == right) {
      return true;
    }
    if (left == null || right == null) {
      return false;
    }
    try {
      return decompress(left).equals(decompress(right));
    } catch (IOException e) {
      return false;
    }
  }

  static byte[] compress(String xml) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(xml.length());
    try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
      gzip.write(xml.getBytes(StandardCharsets.UTF_8));
    }
    return baos.toByteArray();
  }

  static String decompress(byte[] compressed) throws IOException {
    try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed));
        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      gzip.transferTo(baos);
      return baos.toString(StandardCharsets.UTF_8);
    }
  }

  private void trimStack(List<byte[]> stack) {
    int max = maxUndo.getAsInt();
    if (max < 1) {
      max = 1;
    }
    while (stack.size() > max) {
      stack.remove(0);
    }
  }
}
