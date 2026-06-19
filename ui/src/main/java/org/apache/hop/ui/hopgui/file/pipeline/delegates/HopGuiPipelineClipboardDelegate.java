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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.base.BaseHopMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginLoaderException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.workflow.WorkflowMeta;
import org.jspecify.annotations.NonNull;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

@Getter
@Setter
public class HopGuiPipelineClipboardDelegate {
  private static final Class<?> PKG = HopGui.class;

  private static final String XML_TAG_PIPELINE_TRANSFORMS = "pipeline-transforms";
  private static final String XML_TAG_TRANSFORMS = "transforms";

  private HopGui hopGui;
  private HopGuiPipelineGraph pipelineGraph;

  public HopGuiPipelineClipboardDelegate(HopGui hopGui, HopGuiPipelineGraph pipelineGraph) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
    ILogChannel log = hopGui.getLog();
  }

  public void toClipboard(String clipText) {
    try {
      GuiResource.getInstance().toClipboard(clipText);
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Title"),
          BaseMessages.getString(PKG, "HopGui.Dialog.ExceptionCopyToClipboard.Message"),
          e);
    }
  }

  public String fromClipboard() {
    try {
      return GuiResource.getInstance().fromClipboard();
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Title"),
          BaseMessages.getString(PKG, "HopGui.Dialog.ExceptionPasteFromClipboard.Message"),
          e);
      return null;
    }
  }

  public void pasteXml(PipelineMeta pipelineMeta, String clipboardContent, Point location) {
    try {
      Document doc = XmlHandler.loadXmlString(clipboardContent);
      Node pipelineNode = XmlHandler.getSubNode(doc, XML_TAG_PIPELINE_TRANSFORMS);
      // Unselect all, reselect pasted transforms...
      pipelineMeta.unselectAll();

      Node transformsNode = XmlHandler.getSubNode(pipelineNode, XML_TAG_TRANSFORMS);
      List<Node> transformNodes = XmlHandler.getNodes(transformsNode, TransformMeta.XML_TAG);
      ArrayList<String> transformOldNames = new ArrayList<>();

      Point min = new Point(99999999, 99999999);

      List<TransformMeta> transforms = deSerializeTransforms(location, transformNodes, min);
      List<NotePadMeta> notes = deSerializeNotes(location, pipelineNode, min);
      List<PipelineHopMeta> hops = deSerializePipelineHops(pipelineNode, transforms);

      // This is the offset:
      Point offset;
      if (location != null) {
        offset = new Point(location.x - min.x, location.y - min.y);
      } else {
        offset = new Point(-min.x, -min.y);
      }

      // Undo/redo object positions...
      int[] position = new int[transforms.size()];

      for (int i = 0; i < transforms.size(); i++) {
        Point p = transforms.get(i).getLocation();
        String name = transforms.get(i).getName();

        PropsUi.setLocation(transforms.get(i), p.x + offset.x, p.y + offset.y);

        // Check the name, find alternative...
        transformOldNames.add(name);
        transforms.get(i).setName(pipelineMeta.getAlternativeTransformName(name));
        pipelineMeta.addTransform(transforms.get(i));
        position[i] = pipelineMeta.indexOfTransform(transforms.get(i));
        transforms.get(i).setSelected(true);
      }

      // Add the hops too...
      for (PipelineHopMeta hop : hops) {
        pipelineMeta.addPipelineHop(hop);
      }

      // Add the notes...
      for (NotePadMeta note : notes) {
        Point p = note.getLocation();
        PropsUi.setLocation(note, p.x + offset.x, p.y + offset.y);
        pipelineMeta.addNote(note);
        note.setSelected(true);
      }

      // Set the source and target transforms ...
      for (TransformMeta transform : transforms) {
        ITransformMeta smi = transform.getTransform();
        smi.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
      }

      // Set the error handling hops
      Node errorHandlingNode =
          XmlHandler.getSubNode(pipelineNode, PipelineMeta.XML_TAG_TRANSFORM_ERROR_HANDLING);
      int nrErrorHandlers =
          XmlHandler.countNodes(errorHandlingNode, TransformErrorMeta.XML_ERROR_TAG);
      for (int i = 0; i < nrErrorHandlers; i++) {
        Node transformErrorMetaNode =
            XmlHandler.getSubNodeByNr(errorHandlingNode, TransformErrorMeta.XML_ERROR_TAG, i);
        TransformErrorMeta transformErrorMeta =
            new TransformErrorMeta(transformErrorMetaNode, pipelineMeta.getTransforms());

        // Handle pasting multiple times, need to update source and target transform names
        int srcTransformPos =
            transformOldNames.indexOf(transformErrorMeta.getSourceTransform().getName());
        int tgtTransformPos = -1;
        if (transformErrorMeta.getTargetTransform() != null) {
          tgtTransformPos =
              transformOldNames.indexOf(transformErrorMeta.getTargetTransform().getName());
        }
        TransformMeta sourceTransform =
            pipelineMeta.findTransform(transforms.get(srcTransformPos).getName());
        if (sourceTransform != null) {
          sourceTransform.setTransformErrorMeta(transformErrorMeta);
        }
        if (tgtTransformPos >= 0) {
          TransformMeta targetTransform =
              pipelineMeta.findTransform(transforms.get(tgtTransformPos).getName());
          transformErrorMeta.setSourceTransform(sourceTransform);
          transformErrorMeta.setTargetTransform(targetTransform);
        }
      }

      // Save undo information too...
      hopGui.undoDelegate.addUndoNew(
          pipelineMeta, transforms.toArray(new TransformMeta[0]), position, false);

      int[] hopPos = new int[hops.size()];
      for (int i = 0; i < hops.size(); i++) {
        hopPos[i] = pipelineMeta.indexOfPipelineHop(hops.get(i));
      }
      hopGui.undoDelegate.addUndoNew(
          pipelineMeta, hops.toArray(new PipelineHopMeta[0]), hopPos, true);

      int[] notePos = new int[notes.size()];
      for (int i = 0; i < notes.size(); i++) {
        notePos[i] = pipelineMeta.indexOfNote(notes.get(i));
      }
      hopGui.undoDelegate.addUndoNew(
          pipelineMeta, notes.toArray(new NotePadMeta[0]), notePos, true);
    } catch (HopException e) {
      // See if this was different (non-XML) content
      //
      pasteNoXmlContent(pipelineMeta, clipboardContent, location);
    }
    pipelineGraph.redraw();
  }

  private static @NonNull List<PipelineHopMeta> deSerializePipelineHops(
      Node pipelineNode, List<TransformMeta> transforms) throws HopXmlException {
    Node hopsNode = XmlHandler.getSubNode(pipelineNode, PipelineMeta.XML_TAG_ORDER);
    List<Node> hopNodes = XmlHandler.getNodes(hopsNode, BaseHopMeta.XML_HOP_TAG);
    List<PipelineHopMeta> hops = new ArrayList<>();

    for (Node hopNode : hopNodes) {
      hops.add(new PipelineHopMeta(hopNode, transforms));
    }
    return hops;
  }

  private static @NonNull List<NotePadMeta> deSerializeNotes(
      Point location, Node pipelineNode, Point min) throws HopXmlException {
    Node notesNode = XmlHandler.getSubNode(pipelineNode, WorkflowMeta.XML_TAG_NOTEPADS);
    List<Node> noteNodes = XmlHandler.getNodes(notesNode, NotePadMeta.XML_TAG);

    List<NotePadMeta> notes = new ArrayList<>();
    for (Node noteNode : noteNodes) {
      NotePadMeta note = new NotePadMeta(noteNode);
      notes.add(note);
      if (location != null) {
        Point p = note.getLocation();
        if (min.x > p.x) {
          min.x = p.x;
        }
        if (min.y > p.y) {
          min.y = p.y;
        }
      }
    }
    return notes;
  }

  private List<TransformMeta> deSerializeTransforms(
      Point location, List<Node> transformNodes, Point min)
      throws HopXmlException, HopPluginLoaderException {
    List<TransformMeta> transforms = new ArrayList<>();
    for (Node transformNode : transformNodes) {
      TransformMeta transform = new TransformMeta(transformNode, hopGui.getMetadataProvider());
      transforms.add(transform);

      if (location != null) {
        Point p = transform.getLocation();
        if (min.x > p.x) {
          min.x = p.x;
        }
        if (min.y > p.y) {
          min.y = p.y;
        }
      }
    }
    return transforms;
  }

  private void pasteNoXmlContent(
      PipelineMeta pipelineMeta, String clipboardContent, Point location) {
    try {
      // Add a notepad
      //
      NotePadMeta notePadMeta =
          new NotePadMeta(
              clipboardContent,
              location.x,
              location.y,
              ConstUi.NOTE_MIN_SIZE,
              ConstUi.NOTE_MIN_SIZE);
      // Apply grid snapping to ensure correct initial size
      PropsUi.setSize(notePadMeta, ConstUi.NOTE_MIN_SIZE, ConstUi.NOTE_MIN_SIZE);
      pipelineMeta.addNote(notePadMeta);
      hopGui.undoDelegate.addUndoNew(
          pipelineMeta,
          new NotePadMeta[] {notePadMeta},
          new int[] {pipelineMeta.indexOfNote(notePadMeta)});
      shiftLocation(location);
    } catch (Exception e) {
      // "Error pasting transforms...",
      // "I was unable to paste transforms to this pipeline"
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "HopGui.Dialog.UnablePasteTransforms.Title"),
          BaseMessages.getString(PKG, "HopGui.Dialog.UnablePasteTransforms.Message"),
          e);
    }
  }

  public void shiftLocation(Point location) {
    location.x = location.x + (int) (10 * PropsUi.getInstance().getZoomFactor());
    location.y = location.y + (int) (5 * PropsUi.getInstance().getZoomFactor());
  }

  public void copySelected(
      PipelineMeta pipelineMeta, List<TransformMeta> transforms, List<NotePadMeta> notes) {
    if (transforms == null || transforms.isEmpty() && notes.isEmpty()) {
      return;
    }

    StringBuilder xml = new StringBuilder(5000).append(XmlHandler.getXmlHeader());
    try {
      xml.append(XmlHandler.openTag(XML_TAG_PIPELINE_TRANSFORMS)).append(Const.CR);
      serializeTransformsToXml(transforms, xml);
      serializePipelineHopsToXml(pipelineMeta, transforms, xml);
      serializeNotesToXml(notes, xml);
      serializeTransformErrorHandlingToXml(transforms, xml);
      xml.append(XmlHandler.closeTag(XML_TAG_PIPELINE_TRANSFORMS)).append(Const.CR);

      toClipboard(xml.toString());
    } catch (Exception ex) {
      new ErrorDialog(hopGui.getActiveShell(), "Error", "Error encoding to XML", ex);
    }
  }

  private static void serializeTransformErrorHandlingToXml(
      List<TransformMeta> transforms, StringBuilder xml) throws HopException {
    xml.append(XmlHandler.openTag(PipelineMeta.XML_TAG_TRANSFORM_ERROR_HANDLING)).append(Const.CR);
    for (TransformMeta transform : transforms) {
      if (transform.getTransformErrorMeta() != null) {
        xml.append(transform.getTransformErrorMeta().getXml()).append(Const.CR);
      }
    }
    xml.append(XmlHandler.closeTag(PipelineMeta.XML_TAG_TRANSFORM_ERROR_HANDLING)).append(Const.CR);
  }

  private static void serializeNotesToXml(List<NotePadMeta> notes, StringBuilder xml) {
    xml.append(XmlHandler.openTag(PipelineMeta.XML_TAG_NOTEPADS)).append(Const.CR);
    if (notes != null) {
      for (NotePadMeta note : notes) {
        xml.append(note.getXml());
      }
    }
    xml.append(XmlHandler.closeTag(PipelineMeta.XML_TAG_NOTEPADS)).append(Const.CR);
  }

  private static void serializePipelineHopsToXml(
      PipelineMeta pipelineMeta, List<TransformMeta> transforms, StringBuilder xml)
      throws HopException {
    xml.append(XmlHandler.openTag(PipelineMeta.XML_TAG_ORDER)).append(Const.CR);
    for (TransformMeta transform1 : transforms) {
      for (TransformMeta transform2 : transforms) {
        if (transform1 != transform2) {
          PipelineHopMeta hop = pipelineMeta.findPipelineHop(transform1, transform2, true);
          if (hop != null) {
            // Ok, we found one...
            xml.append(hop.getXml()).append(Const.CR);
          }
        }
      }
    }
    xml.append(XmlHandler.closeTag(PipelineMeta.XML_TAG_ORDER)).append(Const.CR);
  }

  private static void serializeTransformsToXml(List<TransformMeta> transforms, StringBuilder xml)
      throws HopException {
    xml.append(XmlHandler.openTag(XML_TAG_TRANSFORMS)).append(Const.CR);
    for (TransformMeta transform : transforms) {
      xml.append(transform.getXml());
    }
    xml.append(XmlHandler.closeTag(XML_TAG_TRANSFORMS)).append(Const.CR);
  }
}
