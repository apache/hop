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
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Runs the XML Output (Advanced) transform: resolves the tree once, opens a StAX writer, then emits
 * prefix path, per-row loop body, and closing tags (with optional group-by).
 */
public class AdvancedXmlOutput extends BaseTransform<AdvancedXmlOutputMeta, AdvancedXmlOutputData> {

  private static final Class<?> PKG = AdvancedXmlOutputMeta.class;

  private static final String EOL = "\n";
  private static final XMLOutputFactory XML_OUT_FACTORY = XMLOutputFactory.newInstance();
  private static final XMLInputFactory XML_IN_FACTORY = createSecureInputFactory();

  /** Writes every byte to two underlying streams (e.g. file + in-memory capture). */
  private static final class TeeOutputStream extends OutputStream {
    private final OutputStream a;
    private final OutputStream b;

    TeeOutputStream(OutputStream a, OutputStream b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public void write(int c) throws IOException {
      a.write(c);
      b.write(c);
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
      a.write(buf, off, len);
      b.write(buf, off, len);
    }

    @Override
    public void flush() throws IOException {
      a.flush();
      b.flush();
    }

    @Override
    public void close() throws IOException {
      try {
        a.close();
      } finally {
        b.close();
      }
    }
  }

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
    data.writeToFile = meta.writesXmlFile();
    data.outputXmlField = meta.writesXmlField();
    if (data.outputXmlField && Utils.isEmpty(resolve(meta.getOutputXmlField()))) {
      logError(BaseMessages.getString(PKG, "AdvancedXmlOutput.Error.MissingOutputXmlField"));
      return false;
    }
    if (!data.writeToFile && meta.getFileSupport().isZipped()) {
      logError(BaseMessages.getString(PKG, "AdvancedXmlOutput.Error.ZipRequiresFile"));
      return false;
    }
    if (data.writeToFile && Utils.isEmpty(resolve(meta.getFileSupport().getFileName()))) {
      logError(BaseMessages.getString(PKG, "AdvancedXmlOutput.Error.MissingTargetFilename"));
      return false;
    }
    if (!data.writeToFile) {
      return true;
    }
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
      if (data.outputXmlField) {
        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
        data.inputRowMetaSize = getInputRowMeta().size();
      }
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

    // Lazy open if the user asked to defer file creation, or output-to-field-only mode.
    if (!data.fileOpen && (!data.writeToFile || meta.getFileSupport().isDoNotOpenNewFileInit())) {
      if (!openNewFile()) {
        logError(BaseMessages.getString(PKG, "AdvancedXmlOutput.Error.OpenOutputFailed"));
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

    boolean passThroughEachRow = data.writeToFile && !data.outputXmlField;
    if (passThroughEachRow) {
      putRow(getInputRowMeta(), r);
    } else if (data.outputXmlField) {
      try {
        data.lastRowForXmlOutput = getInputRowMeta().cloneRow(r);
      } catch (HopValueException e) {
        throw new HopException(e);
      }
    }

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
    if (data.outputXmlField) {
      data.xmlCaptureBuffer = new java.io.ByteArrayOutputStream();
    } else {
      data.xmlCaptureBuffer = null;
    }
    try {
      OutputStream payloadSink;
      FileObject file = null;
      String physicalName = null;

      if (data.writeToFile) {
        physicalName = meta.getFileSupport().buildFilename(this, getCopy(), data.splitnr, true);
        String innerName =
            meta.getFileSupport().buildFilename(this, getCopy(), data.splitnr, false);
        file = HopVfs.getFileObject(physicalName, variables);
        data.currentFile = file;
        data.currentFileName = physicalName;

        OutputStream fos = HopVfs.getOutputStream(file, false);
        data.countingStream = new CountingOutputStream(fos);
        if (meta.getFileSupport().isZipped()) {
          data.zip = new ZipOutputStream(data.countingStream);
          ZipEntry entry = new ZipEntry(new File(innerName).getName());
          entry.setComment("Compressed by Apache Hop");
          data.zip.putNextEntry(entry);
          payloadSink = data.zip;
        } else {
          data.zip = null;
          payloadSink = data.countingStream;
        }
      } else {
        data.currentFile = null;
        data.currentFileName = null;
        data.countingStream = null;
        data.zip = null;
        payloadSink =
            data.xmlCaptureBuffer != null
                ? data.xmlCaptureBuffer
                : new java.io.ByteArrayOutputStream();
      }

      if (data.xmlCaptureBuffer != null && data.writeToFile) {
        outputStream = new TeeOutputStream(data.xmlCaptureBuffer, payloadSink);
      } else {
        outputStream = payloadSink;
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
    boolean rowsWritten = data.rowsWrittenToCurrentFile;
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
    try {
      emitOutputXmlRowIfNeeded();
    } catch (HopException e) {
      logError(e.getMessage(), e);
    }
    if (rowsWritten && meta.isGenerateXsd() && data.writeToFile) {
      writeSiblingXsd();
    }
  }

  private void emitOutputXmlRowIfNeeded() throws HopException {
    if (!data.outputXmlField
        || !data.rowsWrittenToCurrentFile
        || data.xmlCaptureBuffer == null
        || data.lastRowForXmlOutput == null
        || data.outputRowMeta == null) {
      return;
    }
    String enc = Utils.isEmpty(meta.getEncoding()) ? Const.UTF_8 : meta.getEncoding();
    Charset cs;
    try {
      cs = Charset.forName(enc);
    } catch (Exception e) {
      cs = StandardCharsets.UTF_8;
    }
    String xml = data.xmlCaptureBuffer.toString(cs);
    Object[] out;
    if (meta.isIncludeInputFieldsInOutput()) {
      out = RowDataUtil.addValueData(data.lastRowForXmlOutput, data.inputRowMetaSize, xml);
    } else {
      out = new Object[] {xml};
    }
    putRow(data.outputRowMeta, out);
    data.xmlCaptureBuffer = null;
  }

  /**
   * Writes a sibling .xsd schema for the current data file. Errors are logged but don't fail the
   * pipeline (the data file is the contract; the schema is best-effort metadata).
   */
  private void writeSiblingXsd() {
    try {
      String xsdName = meta.getFileSupport().buildXsdFilename(this, getCopy(), data.splitnr - 1);
      AdvancedXmlOutputXsdWriter.write(
          xsdName, this, meta.getEncoding(), meta.getRootNode(), data.inputRowMeta);
      if (meta.getFileSupport().isAddToResultFilenames()) {
        registerXsdResultFile(xsdName);
      }
    } catch (Exception e) {
      logError("Error writing sibling XSD schema: " + e.getMessage(), e);
    }
  }

  private void registerXsdResultFile(String xsdName) {
    try {
      FileObject xsd = HopVfs.getFileObject(xsdName, this);
      ResultFile rf =
          new ResultFile(
              ResultFile.FILE_TYPE_GENERAL, xsd, getPipelineMeta().getName(), getTransformName());
      rf.setComment("XSD schema generated by XML Output (Advanced) transform");
      addResultFile(rf);
    } catch (Exception e) {
      logError("Could not register sibling XSD as result file: " + e.getMessage());
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
      if (data.xmlCaptureBuffer != null) {
        data.xmlCaptureBuffer.reset();
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
    if (!data.writeToFile || !meta.getFileSupport().isAddToResultFilenames()) {
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
    rf.setComment("File created by XML Output (Advanced) transform");
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
    }
    if (value == null && !Utils.isEmpty(node.getDefaultValue())) {
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

  /**
   * Writes the start tag for {@code node}, declaring its namespace (if any) as the default
   * namespace on the element. The URI must be bound before the {@code writeStartElement(uri, ...)}
   * call: woodstox (and other strict StAX impls) refuse to emit unbound namespace URIs. Children
   * with no explicit namespace then inherit the default namespace of their nearest ancestor.
   */
  private void writeStartElementWithNamespace(XmlNode node) throws Exception {
    String ns = node.getNamespace();
    if (Utils.isEmpty(ns)) {
      data.writer.writeStartElement(node.getName());
      return;
    }
    data.writer.setDefaultNamespace(ns);
    data.writer.writeStartElement(ns, node.getName());
    data.writer.writeDefaultNamespace(ns);
  }

  /**
   * Writes any direct attribute children of {@code node} for the current row.
   *
   * <p>Each attribute is emitted at most once. The decision tree is:
   *
   * <ul>
   *   <li>Unmapped attribute (no input field bound): emit when {@code force_create} is set OR
   *       {@code Create attribute if no field is mapped} is on, using the default value (or empty
   *       string).
   *   <li>Mapped attribute, non-null value: emit the value (trimmed if requested).
   *   <li>Mapped attribute, null value: emit when {@code force_create} is set OR {@code Create
   *       attribute when value is null} is on, using the default value (or empty string).
   * </ul>
   */
  private void writeAttributesOf(XmlNode node, Object[] r) throws Exception {
    if (node.getChildren() == null) {
      return;
    }
    for (XmlNode c : node.getChildren()) {
      if (c.getKind() != XmlNode.NodeKind.Attribute) {
        continue;
      }
      String defaultVal = Utils.isEmpty(c.getDefaultValue()) ? "" : c.getDefaultValue();

      if (!data.fieldIndex.containsKey(c)) {
        if (c.isForceCreate() || meta.isCreateAttributeIfUnmapped()) {
          data.writer.writeAttribute(c.getName(), defaultVal);
        }
        continue;
      }

      int idx = data.fieldIndex.get(c);
      IValueMeta vm = data.inputRowMeta.getValueMeta(idx);
      Object data1 = r[idx];
      String value = vm.isNull(data1) ? null : vm.getString(data1);

      if (value != null) {
        data.writer.writeAttribute(c.getName(), applyTrim(value));
      } else if (c.isForceCreate() || meta.isCreateAttributeIfNull()) {
        data.writer.writeAttribute(c.getName(), defaultVal);
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
    }
    if (value == null && !Utils.isEmpty(node.getDefaultValue())) {
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
    fragment = stripLeadingXmlDeclaration(fragment);
    if (Utils.isEmpty(fragment)) {
      return;
    }
    boolean stripOuter = node.isStripOuterFragmentElement();
    boolean skippedOuterWrapper = false;
    // Wrap so we can have multiple top-level nodes.
    String wrapped = "<root>" + fragment + "</root>";
    try (Reader reader = new StringReader(wrapped)) {
      XMLStreamReader xr = XML_IN_FACTORY.createXMLStreamReader(reader);
      int depth = 0;
      int stripWrapperDepth = -1;
      while (xr.hasNext()) {
        int event = xr.next();
        switch (event) {
          case XMLStreamConstants.START_ELEMENT -> {
            depth++;
            if (depth == 1 && "root".equals(xr.getLocalName())) {
              break;
            }
            if (stripOuter && !skippedOuterWrapper) {
              skippedOuterWrapper = true;
              stripWrapperDepth = depth;
              break;
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
            if (stripWrapperDepth == depth) {
              stripWrapperDepth = -1;
              depth--;
              break;
            }
            data.writer.writeEndElement();
            depth--;
          }
          case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> {
            if (depth > 1) {
              data.writer.writeCharacters(xr.getText());
            }
          }
          default -> {
            // ignore comments, PIs, whitespace-only at root
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

  /**
   * Removes an optional UTF-8 BOM and XML declaration so a value produced by another XML Output
   * (Advanced) transform (or any generator) can be wrapped in a synthetic root for parsing.
   */
  private static String stripLeadingXmlDeclaration(String fragment) {
    if (fragment == null || fragment.isEmpty()) {
      return fragment;
    }
    String s = fragment.stripLeading();
    if (!s.isEmpty() && s.charAt(0) == '\uFEFF') {
      s = s.substring(1).stripLeading();
    }
    if (s.startsWith("<?xml")) {
      int end = s.indexOf("?>");
      if (end >= 0) {
        s = s.substring(end + 2).stripLeading();
      }
    }
    return s;
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
