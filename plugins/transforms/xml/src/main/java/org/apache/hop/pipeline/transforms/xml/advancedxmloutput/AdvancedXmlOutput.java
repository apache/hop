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

package org.apache.hop.pipeline.transforms.xml.advancedxmloutput;

import java.io.File;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Runtime engine for the Advanced XML Output transform.
 *
 * <p>Walks the configured {@link XmlNode} tree once at first-row time, splitting it into a "prefix
 * path" (root → loop's parent) with optional group-by ancestors, a "loop subtree" emitted for each
 * input row, and a "suffix" of closing tags. Group-by ancestors collapse consecutive input rows
 * that share the same group key into a single occurrence of the group element. Memory profile is
 * O(largest group); the writer is StAX-streaming.
 */
public class AdvancedXmlOutput extends BaseTransform<AdvancedXmlOutputMeta, AdvancedXmlOutputData> {

  private static final String EOL = "\n";
  private static final XMLOutputFactory XML_OUT_FACTORY = XMLOutputFactory.newInstance();
  private static final XMLInputFactory XML_IN_FACTORY = createSecureInputFactory();

  private OutputStream outputStream;

  /** The currently-open path of {@link XmlNode}s above the loop, deepest first. */
  private final Deque<XmlNode> openStack = new ArrayDeque<>();

  public AdvancedXmlOutput(
      TransformMeta transformMeta,
      AdvancedXmlOutputMeta meta,
      AdvancedXmlOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  // ---------------------------------------------------------------------------
  // BaseTransform overrides
  // ---------------------------------------------------------------------------

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    data.splitnr = 0;
    if (meta.getFileSupport().isDoNotOpenNewFileInit()) {
      return true;
    }
    if (openNewFile()) {
      return true;
    }
    logError("Couldn't open file " + meta.getFileSupport().getFileName());
    setErrors(1L);
    stopAll();
    return false;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow();

    if (first) {
      first = false;
      if (r == null) {
        // No rows at all
        if (data.fileOpen && meta.getFileSupport().isDoNotCreateEmptyFile()) {
          discardEmptyFile();
        } else if (data.fileOpen) {
          // Open file, then immediately close as empty (root + decl).
          finalizeOpenPath();
          closeFile();
        }
        setOutputDone();
        return false;
      }
      data.inputRowMeta = getInputRowMeta();
      resolveTreeStructure();
    }

    // Handle split BEFORE writing the row so the new file's prefix is fresh.
    int splitEvery = meta.getFileSupport().getSplitEvery();
    if (r != null
        && getLinesOutput() > 0
        && splitEvery > 0
        && (getLinesOutput() % splitEvery) == 0) {
      finalizeOpenPath();
      closeFile();
      if (!openNewFile()) {
        logError("Unable to open new file (split #" + data.splitnr + ")");
        setErrors(1);
        return false;
      }
    }

    if (r == null) {
      finalizeOpenPath();
      if (!data.rowsWrittenToCurrentFile && meta.getFileSupport().isDoNotCreateEmptyFile()) {
        discardEmptyFile();
      } else {
        closeFile();
      }
      setOutputDone();
      return false;
    }

    // Lazy open if the user asked us to defer file creation.
    if (!data.fileOpen && meta.getFileSupport().isDoNotOpenNewFileInit()) {
      if (!openNewFile()) {
        logError("Couldn't open file " + meta.getFileSupport().getFileName());
        setErrors(1);
        return false;
      }
    }

    try {
      writeRowToTree(r);
    } catch (Exception e) {
      throw new HopException(
          "Error writing XML row :" + e + Const.CR + "Row: " + getInputRowMeta().getString(r), e);
    }

    putRow(getInputRowMeta(), r);

    if (checkFeedback(getLinesOutput()) && isBasic()) {
      logBasic("linenr " + getLinesOutput());
    }
    return true;
  }

  @Override
  public void dispose() {
    try {
      finalizeOpenPath();
      if (!data.rowsWrittenToCurrentFile && meta.getFileSupport().isDoNotCreateEmptyFile()) {
        discardEmptyFile();
      } else {
        closeFile();
      }
    } catch (Exception e) {
      logError("Error closing XML output", e);
    }
    super.dispose();
  }

  // ---------------------------------------------------------------------------
  // Tree resolution / first-row setup
  // ---------------------------------------------------------------------------

  /**
   * Walk the tree once to locate the loop node, the path from root to the loop's parent, and the
   * ordered list of group-by ancestors. Resolves field indices for every node that references an
   * input field.
   */
  private void resolveTreeStructure() throws HopException {
    XmlNode root = meta.getRootNode();
    if (root == null) {
      throw new HopException("XML tree is not configured.");
    }

    // Validate
    List<String> errors = AdvancedXmlOutputValidator.validate(root, data.inputRowMeta);
    if (!errors.isEmpty()) {
      throw new HopException(String.join("; ", errors));
    }

    // Locate the (single) loop node and the path to it.
    List<XmlNode> pathToLoop = new ArrayList<>();
    if (!findPathToLoop(root, pathToLoop)) {
      throw new HopException(
          "Could not locate the loop element in the XML tree (mark exactly one node as loop).");
    }

    // pathToLoop = [root, ..., loop].  pathToLoopParent = [root, ..., loop's parent].
    data.loopNode = pathToLoop.get(pathToLoop.size() - 1);
    data.pathToLoopParent = new ArrayList<>(pathToLoop.subList(0, pathToLoop.size() - 1));

    // Group-by ancestors of the loop, in document order (top → bottom).
    data.groupByPath = new ArrayList<>();
    for (XmlNode anc : data.pathToLoopParent) {
      if (anc.isGroupBy()) {
        data.groupByPath.add(anc);
      }
    }

    // Resolve field indices for every node with a mapped field, anywhere in the tree.
    data.fieldIndex = new IdentityHashMap<>();
    resolveFieldIndices(root);
  }

  private boolean findPathToLoop(XmlNode node, List<XmlNode> path) {
    path.add(node);
    if (node.isLoop()) {
      return true;
    }
    if (node.getChildren() != null) {
      for (XmlNode child : node.getChildren()) {
        if (findPathToLoop(child, path)) {
          return true;
        }
      }
    }
    path.remove(path.size() - 1);
    return false;
  }

  private void resolveFieldIndices(XmlNode node) throws HopException {
    if (!Utils.isEmpty(node.getMappedField())) {
      int idx = data.inputRowMeta.indexOfValue(node.getMappedField());
      if (idx < 0) {
        throw new HopException(
            "Field ["
                + node.getMappedField()
                + "] not found in input row for node '"
                + node.getName()
                + "'.");
      }
      data.fieldIndex.put(node, idx);
      // Apply formatting overrides to the value-meta (clone-safe; we work on input row meta clone)
      IValueMeta vm = data.inputRowMeta.getValueMeta(idx);
      if (vm != null) {
        if (!Utils.isEmpty(node.getFormat())) {
          vm.setConversionMask(node.getFormat());
        }
        if (node.getLength() > 0 || node.getPrecision() > 0) {
          vm.setLength(node.getLength(), node.getPrecision());
        }
        if (!Utils.isEmpty(node.getCurrencySymbol())) {
          vm.setCurrencySymbol(node.getCurrencySymbol());
        }
        if (!Utils.isEmpty(node.getDecimalSymbol())) {
          vm.setDecimalSymbol(node.getDecimalSymbol());
        } else if (!Utils.isEmpty(meta.getDefaultDecimalSeparator())) {
          vm.setDecimalSymbol(meta.getDefaultDecimalSeparator());
        }
        if (!Utils.isEmpty(node.getGroupingSymbol())) {
          vm.setGroupingSymbol(node.getGroupingSymbol());
        } else if (!Utils.isEmpty(meta.getDefaultGroupingSeparator())) {
          vm.setGroupingSymbol(meta.getDefaultGroupingSeparator());
        }
      }
    }
    if (node.getChildren() != null) {
      for (XmlNode c : node.getChildren()) {
        resolveFieldIndices(c);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // File handling
  // ---------------------------------------------------------------------------

  private boolean openNewFile() {
    data.writer = null;
    try {
      String physicalName =
          meta.getFileSupport().buildFilename(this, getCopy(), data.splitnr, true);
      String innerName = meta.getFileSupport().buildFilename(this, getCopy(), data.splitnr, false);
      FileObject file = HopVfs.getFileObject(physicalName, variables);
      data.currentFile = file;
      data.currentFileName = physicalName;

      OutputStream fos = HopVfs.getOutputStream(file, false);
      data.countingStream = new CountingOutputStream(fos);
      if (meta.getFileSupport().isZipped()) {
        data.zip = new ZipOutputStream(data.countingStream);
        ZipEntry entry = new ZipEntry(new File(innerName).getName());
        entry.setComment("Compressed by Apache Hop");
        data.zip.putNextEntry(entry);
        outputStream = data.zip;
      } else {
        outputStream = data.countingStream;
      }

      String enc = Utils.isEmpty(meta.getEncoding()) ? Const.UTF_8 : meta.getEncoding();
      data.writer = XML_OUT_FACTORY.createXMLStreamWriter(outputStream, enc);
      data.writer.writeStartDocument(enc, "1.0");
      if (!meta.isCompactFile() && meta.isBlankLineAfterXmlDeclaration()) {
        data.writer.writeCharacters(EOL);
      }

      writeOptionalDocType();
      writeOptionalXslPi();

      data.fileOpen = true;
      data.rowsWrittenToCurrentFile = false;
      data.currentGroupKey = null;
      openStack.clear();
      data.splitnr++;
      return true;
    } catch (Exception e) {
      logError("Error opening new file: " + e);
      return false;
    }
  }

  private void writeOptionalDocType() throws Exception {
    String root = variables.resolve(meta.getDoctypeRootElement());
    String sys = variables.resolve(meta.getDoctypeSystemId());
    String pub = variables.resolve(meta.getDoctypePublicId());
    if (Utils.isEmpty(root)) {
      return;
    }
    StringBuilder sb = new StringBuilder("<!DOCTYPE ").append(root);
    if (!Utils.isEmpty(pub)) {
      sb.append(" PUBLIC \"").append(pub).append("\"");
      if (!Utils.isEmpty(sys)) {
        sb.append(" \"").append(sys).append("\"");
      }
    } else if (!Utils.isEmpty(sys)) {
      sb.append(" SYSTEM \"").append(sys).append("\"");
    }
    sb.append(">");
    data.writer.writeDTD(sb.toString());
    if (!meta.isCompactFile()) {
      data.writer.writeCharacters(EOL);
    }
  }

  private void writeOptionalXslPi() throws Exception {
    String href = variables.resolve(meta.getXslStylesheetHref());
    if (Utils.isEmpty(href)) {
      return;
    }
    String type = variables.resolve(meta.getXslStylesheetType());
    if (Utils.isEmpty(type)) {
      type = "text/xsl";
    }
    data.writer.writeProcessingInstruction(
        "xml-stylesheet", "type=\"" + type + "\" href=\"" + href + "\"");
    if (!meta.isCompactFile()) {
      data.writer.writeCharacters(EOL);
    }
  }

  private void finalizeOpenPath() {
    if (data.writer == null || !data.fileOpen) {
      return;
    }
    try {
      // Close any open path elements (deepest first).
      while (!openStack.isEmpty()) {
        data.writer.writeEndElement();
        openStack.pop();
        if (!meta.isCompactFile()) {
          data.writer.writeCharacters(EOL);
        }
      }
    } catch (Exception e) {
      logError("Error finalizing XML path: " + e);
    }
  }

  private void closeFile() {
    if (!data.fileOpen) {
      return;
    }
    try {
      data.writer.writeEndDocument();
      data.writer.close();

      if (meta.getFileSupport().isZipped()) {
        data.zip.closeEntry();
        data.zip.finish();
      }
      if (data.countingStream != null) {
        dataVolumeOut =
            (dataVolumeOut != null ? dataVolumeOut : 0L) + data.countingStream.getCount();
        data.countingStream.close();
      }
    } catch (Exception e) {
      logError("Error closing XML output: " + e);
    } finally {
      data.fileOpen = false;
    }
  }

  /** Discards a file that was opened but received no rows. */
  private void discardEmptyFile() {
    try {
      if (data.writer != null) {
        try {
          data.writer.close();
        } catch (Exception ignore) {
          // best effort
        }
      }
      if (meta.getFileSupport().isZipped() && data.zip != null) {
        try {
          data.zip.close();
        } catch (Exception ignore) {
          // best effort
        }
      }
      if (data.countingStream != null) {
        try {
          data.countingStream.close();
        } catch (Exception ignore) {
          // best effort
        }
      }
      if (data.currentFile != null && data.currentFile.exists()) {
        // We never registered the file as a result (registration is deferred to the
        // first successful row write), so just delete it.
        data.currentFile.delete();
      }
    } catch (Exception e) {
      logError("Error discarding empty XML file: " + e);
    } finally {
      data.fileOpen = false;
    }
  }

  // ---------------------------------------------------------------------------
  // Row-level write
  // ---------------------------------------------------------------------------

  private void writeRowToTree(Object[] r) throws Exception {
    String[] newKey = computeGroupKey(r);

    if (data.currentGroupKey == null) {
      // First row: open the entire path from root to loop's parent.
      openPath(0, data.pathToLoopParent.size() - 1, r);
      data.currentGroupKey = newKey;
    } else {
      int changedLevel = newKey.length;
      for (int i = 0; i < newKey.length; i++) {
        if (!Objects.equals(data.currentGroupKey[i], newKey[i])) {
          changedLevel = i;
          break;
        }
      }
      if (changedLevel < newKey.length) {
        // Close from end-of-path back down through groupByPath[changedLevel] (inclusive).
        int closeDownToSize = pathIndexOf(data.groupByPath.get(changedLevel));
        while (openStack.size() > closeDownToSize) {
          data.writer.writeEndElement();
          openStack.pop();
          if (!meta.isCompactFile()) {
            data.writer.writeCharacters(EOL);
          }
        }
        openPath(closeDownToSize, data.pathToLoopParent.size() - 1, r);
        data.currentGroupKey = newKey;
      }
    }

    // Emit the loop subtree (one row → one occurrence of the loop element).
    writeSubtree(data.loopNode, r);
    if (!meta.isCompactFile()) {
      data.writer.writeCharacters(EOL);
    }

    incrementLinesOutput();
    if (!data.rowsWrittenToCurrentFile) {
      // Defer result-file registration until we actually write something.
      registerResultFile();
    }
    data.rowsWrittenToCurrentFile = true;
  }

  private void registerResultFile() {
    if (!meta.getFileSupport().isAddToResultFilenames()) {
      return;
    }
    if (data.currentFile == null) {
      return;
    }
    ResultFile rf =
        new ResultFile(
            ResultFile.FILE_TYPE_GENERAL,
            data.currentFile,
            getPipelineMeta().getName(),
            getTransformName());
    rf.setComment("File created by Advanced XML Output transform");
    addResultFile(rf);
  }

  /** Returns the index of {@code node} within {@link AdvancedXmlOutputData#pathToLoopParent}. */
  private int pathIndexOf(XmlNode node) throws HopException {
    for (int i = 0; i < data.pathToLoopParent.size(); i++) {
      if (data.pathToLoopParent.get(i) == node) {
        return i;
      }
    }
    throw new HopException("Internal error: node not on path: " + node);
  }

  /** Builds the current row's group-key tuple (one entry per group-by ancestor of the loop). */
  private String[] computeGroupKey(Object[] r) throws Exception {
    String[] key = new String[data.groupByPath.size()];
    for (int i = 0; i < data.groupByPath.size(); i++) {
      XmlNode g = data.groupByPath.get(i);
      Integer idx = data.fieldIndex.get(g);
      key[i] = idx == null ? "" : data.inputRowMeta.getValueMeta(idx).getString(r[idx]);
    }
    return key;
  }

  /**
   * Open path elements from index {@code from} (inclusive) up to {@code to} (inclusive). For each
   * opened element we also emit attribute children, the element's own mapped value (if any), and
   * any non-path static descendant subtrees, taking the values from the current row.
   */
  private void openPath(int from, int to, Object[] r) throws Exception {
    // Set of XmlNode identities that lie on the path from this node down to the loop. Children
    // matching one of these must NOT be emitted as static content (they are handled by the next
    // openPath/openLoop call).
    Set<XmlNode> onPath = onPathSet();

    for (int i = from; i <= to; i++) {
      XmlNode node = data.pathToLoopParent.get(i);
      writeStartElementWithNamespace(node);

      // Attributes first (StAX requires attributes before character/element content).
      writeAttributesOf(node, r);

      // Element's own mapped value, if any (text content before children).
      writeOwnTextValue(node, r);

      // Static (non-on-path) child subtrees.
      if (node.getChildren() != null) {
        for (XmlNode c : node.getChildren()) {
          if (c.getKind() == XmlNode.NodeKind.Attribute) {
            continue; // already written
          }
          if (onPath.contains(c)) {
            continue; // path will be opened next iteration / loop
          }
          writeSubtree(c, r);
        }
      }

      openStack.push(node);
      if (!meta.isCompactFile()) {
        data.writer.writeCharacters(EOL);
      }
    }
  }

  private Set<XmlNode> onPathSet() {
    Set<XmlNode> s = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    s.addAll(data.pathToLoopParent);
    s.add(data.loopNode);
    return s;
  }

  /** Recursively writes a complete element/attribute subtree using the given row. */
  private void writeSubtree(XmlNode node, Object[] r) throws Exception {
    switch (node.getKind()) {
      case Attribute -> {
        // Attribute writing is handled by the parent's writeAttributesOf().
      }
      case DocumentFragment -> writeDocumentFragment(node, r);
      case Element -> writeElement(node, r);
    }
  }

  private void writeElement(XmlNode node, Object[] r) throws Exception {
    Integer idx = data.fieldIndex.get(node);
    boolean hasMapping = idx != null;
    String value = null;
    if (hasMapping) {
      IValueMeta vm = data.inputRowMeta.getValueMeta(idx);
      Object data1 = r[idx];
      value = vm.isNull(data1) ? null : vm.getString(data1);
    } else if (!Utils.isEmpty(node.getDefaultValue())) {
      value = node.getDefaultValue();
    }

    boolean hasChildren = node.hasElementChildren();
    boolean hasAttributes = node.hasAttributeChildren();
    boolean shouldEmit =
        hasChildren
            || hasAttributes
            || value != null
            || node.isForceCreate()
            || meta.isCreateEmptyElement();

    if (!shouldEmit) {
      return;
    }

    writeStartElementWithNamespace(node);
    writeAttributesOf(node, r);
    if (value != null) {
      data.writer.writeCharacters(applyTrim(value));
    }
    if (node.getChildren() != null) {
      for (XmlNode c : node.getChildren()) {
        if (c.getKind() == XmlNode.NodeKind.Attribute) {
          continue;
        }
        writeSubtree(c, r);
      }
    }
    data.writer.writeEndElement();
  }

  private void writeStartElementWithNamespace(XmlNode node) throws Exception {
    if (!Utils.isEmpty(node.getNamespace())) {
      data.writer.writeStartElement(node.getNamespace(), node.getName());
      data.writer.writeDefaultNamespace(node.getNamespace());
    } else {
      data.writer.writeStartElement(node.getName());
    }
  }

  /** Writes any direct attribute children of {@code node} for the current row. */
  private void writeAttributesOf(XmlNode node, Object[] r) throws Exception {
    if (node.getChildren() == null) {
      return;
    }
    for (XmlNode c : node.getChildren()) {
      if (c.getKind() != XmlNode.NodeKind.Attribute) {
        continue;
      }
      Integer idx = data.fieldIndex.get(c);
      String value = null;
      if (idx != null) {
        IValueMeta vm = data.inputRowMeta.getValueMeta(idx);
        Object data1 = r[idx];
        value = vm.isNull(data1) ? null : vm.getString(data1);
      } else if (!Utils.isEmpty(c.getDefaultValue())) {
        value = c.getDefaultValue();
      }

      if (value == null) {
        if (meta.isCreateAttributeIfNull() || c.isForceCreate()) {
          data.writer.writeAttribute(
              c.getName(), Utils.isEmpty(c.getDefaultValue()) ? "" : c.getDefaultValue());
        }
        // else: skip silently
      } else {
        data.writer.writeAttribute(c.getName(), applyTrim(value));
      }
    }
    // Unmapped attributes that the user wants to emit anyway (rare; v1 covers via forceCreate).
    if (meta.isCreateAttributeIfUnmapped() && node.getChildren() != null) {
      for (XmlNode c : node.getChildren()) {
        if (c.getKind() != XmlNode.NodeKind.Attribute) {
          continue;
        }
        if (data.fieldIndex.containsKey(c)) {
          continue; // already handled
        }
        if (Utils.isEmpty(c.getMappedField()) && !c.isForceCreate()) {
          // forceCreate path is already covered above with default value
          data.writer.writeAttribute(
              c.getName(), Utils.isEmpty(c.getDefaultValue()) ? "" : c.getDefaultValue());
        }
      }
    }
  }

  /**
   * Writes the element's own text content (mapped field or default value), if applicable. For a
   * group-by node, the mapped field is interpreted purely as a group key and is NOT written as the
   * element's text content (use a child element / attribute to surface the value).
   */
  private void writeOwnTextValue(XmlNode node, Object[] r) throws Exception {
    if (node.isGroupBy()) {
      // Group-by node: mappedField is the group key only.
      if (!Utils.isEmpty(node.getDefaultValue())) {
        data.writer.writeCharacters(applyTrim(node.getDefaultValue()));
      }
      return;
    }
    Integer idx = data.fieldIndex.get(node);
    String value = null;
    if (idx != null) {
      IValueMeta vm = data.inputRowMeta.getValueMeta(idx);
      Object data1 = r[idx];
      value = vm.isNull(data1) ? null : vm.getString(data1);
    } else if (!Utils.isEmpty(node.getDefaultValue())) {
      value = node.getDefaultValue();
    }
    if (value != null) {
      data.writer.writeCharacters(applyTrim(value));
    }
  }

  /**
   * Inserts the value of the mapped field as parsed XML nodes (rather than escaped text). The field
   * value is expected to contain a well-formed XML fragment (or fragment with multiple top
   * elements). On parse failure we log a warning and emit nothing.
   */
  private void writeDocumentFragment(XmlNode node, Object[] r) throws Exception {
    Integer idx = data.fieldIndex.get(node);
    if (idx == null) {
      return;
    }
    IValueMeta vm = data.inputRowMeta.getValueMeta(idx);
    Object data1 = r[idx];
    if (vm.isNull(data1)) {
      return;
    }
    String fragment = vm.getString(data1);
    if (Utils.isEmpty(fragment)) {
      return;
    }
    // Wrap so we can have multiple top-level nodes.
    String wrapped = "<root>" + fragment + "</root>";
    try (Reader reader = new StringReader(wrapped)) {
      XMLStreamReader xr = XML_IN_FACTORY.createXMLStreamReader(reader);
      int depth = 0;
      while (xr.hasNext()) {
        int event = xr.next();
        switch (event) {
          case XMLStreamConstants.START_ELEMENT -> {
            depth++;
            if (depth == 1 && "root".equals(xr.getLocalName())) {
              break; // skip the wrapper itself
            }
            data.writer.writeStartElement(xr.getLocalName());
            for (int i = 0; i < xr.getAttributeCount(); i++) {
              data.writer.writeAttribute(xr.getAttributeLocalName(i), xr.getAttributeValue(i));
            }
          }
          case XMLStreamConstants.END_ELEMENT -> {
            if (depth == 1 && "root".equals(xr.getLocalName())) {
              depth--;
              break;
            }
            data.writer.writeEndElement();
            depth--;
          }
          case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> {
            if (depth >= 2 || (depth == 1 && !"root".equals(xr.getLocalName()))) {
              data.writer.writeCharacters(xr.getText());
            }
          }
          default -> {
            // ignore comments, PIs, whitespace
          }
        }
      }
      xr.close();
    } catch (Exception e) {
      logError(
          "Could not parse XML document fragment from field '"
              + node.getMappedField()
              + "': "
              + e.getMessage());
    }
  }

  private String applyTrim(String value) {
    return meta.isTrimValues() && value != null ? value.trim() : value;
  }

  private static XMLInputFactory createSecureInputFactory() {
    XMLInputFactory f = XMLInputFactory.newInstance();
    f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    f.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    return f;
  }

  // ---------------------------------------------------------------------------
  // Helpers exposed for tests
  // ---------------------------------------------------------------------------

  /** Test hook: returns the current data object's writer (so unit tests can inject a mock). */
  protected XMLStreamWriter getWriter() {
    return data == null ? null : data.writer;
  }
}
