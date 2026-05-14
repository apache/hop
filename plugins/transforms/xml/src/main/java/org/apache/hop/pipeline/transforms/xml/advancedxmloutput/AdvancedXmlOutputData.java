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

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.zip.ZipOutputStream;
import javax.xml.stream.XMLStreamWriter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

@SuppressWarnings("java:S1104")
public class AdvancedXmlOutputData extends BaseTransformData implements ITransformData {

  /** Increments each time a new (split) file is opened. */
  public int splitnr;

  /** Whether {@code openNewFile} ran successfully and the file is now open. */
  public boolean fileOpen;

  /** Whether the currently open file has had at least one row written to it. */
  public boolean rowsWrittenToCurrentFile;

  /** The last opened {@link FileObject} (kept around so we can delete on empty-file). */
  public FileObject currentFile;

  /** The "inner" filename when the file is zipped; equals the on-disk name otherwise. */
  public String currentFileName;

  /** Optional zip wrapper around the underlying output stream. */
  public ZipOutputStream zip;

  /** Bytes-written counter for metrics. */
  public CountingOutputStream countingStream;

  /** The StAX writer wrapping the (possibly zipped) output stream. */
  public XMLStreamWriter writer;

  /** Row meta of the input stream, captured on first row. */
  public IRowMeta inputRowMeta;

  /** Resolved field indices for every node that references an input field (by node identity). */
  public java.util.Map<XmlNode, Integer> fieldIndex;

  /** Group-by ancestors (top→down) of the loop node, captured once at first-row time. */
  public List<XmlNode> groupByPath;

  /** The single loop node, captured once at first-row time. */
  public XmlNode loopNode;

  /** Path from root (inclusive) to the loop node's parent (inclusive). */
  public List<XmlNode> pathToLoopParent;

  /**
   * Last group-key tuple written. Same length as {@link #groupByPath}; null = no group is open yet.
   */
  public String[] currentGroupKey;

  /** Last input row snapshot for attaching the XML string field (output value / both modes). */
  public Object[] lastRowForXmlOutput;

  /** True: write XML to the configured file. */
  public boolean writeToFile;

  /** True when the XML document is also written to the meta's output string field. */
  public boolean outputXmlField;

  /** When {@link #outputXmlField}, uncompressed XML bytes for the current file / segment. */
  public ByteArrayOutputStream xmlCaptureBuffer;

  /** Output row layout including the optional XML field. */
  public IRowMeta outputRowMeta;

  public int inputRowMetaSize;

  public AdvancedXmlOutputData() {
    super();
    this.splitnr = 0;
    this.fileOpen = false;
    this.rowsWrittenToCurrentFile = false;
  }
}
