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

package org.apache.hop.pipeline.transforms.xml.xmloutput;

import java.io.File;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.xml.xmloutput.XmlField.ContentType;

/** Converts input rows to one or more XML files. */
public class XmlOutput extends BaseTransform<XmlOutputMeta, XmlOutputData> {
  private static final String EOL =
      "\n"; // force EOL char because woodstox library encodes CRLF incorrectly

  private static final XMLOutputFactory XML_OUT_FACTORY = XMLOutputFactory.newInstance();

  private OutputStream outputStream;

  public XmlOutput(
      TransformMeta transformMeta,
      XmlOutputMeta meta,
      XmlOutputData transformDataInterface,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline trans) {
    super(transformMeta, meta, transformDataInterface, copyNr, pipelineMeta, trans);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r;
    boolean result = true;

    r = getRow(); // This also waits for a row to be finished.

    if (first && meta.getFileDetails().isDoNotOpenNewFileInit()) {
      // no more input to be expected...
      // In this case, no file was opened.
      if (r == null) {
        setOutputDone();
        return false;
      }

      if (openNewFile()) {
        data.OpenedNewFile = true;
      } else {
        logError("Couldn't open file " + meta.getFileDetails().getFileName());
        setErrors(1L);
        return false;
      }
    }

    if ((r != null
        && getLinesOutput() > 0
        && meta.getFileDetails().getSplitEvery() > 0
        && (getLinesOutput() % meta.getFileDetails().getSplitEvery()) == 0)) {
      // Done with this part or with everything.
      closeFile();

      // Not finished: open another file...
      if (r != null && !openNewFile()) {
        logError("Unable to open new file (split #" + data.splitnr + "...");
        setErrors(1);
        return false;
      }
    }

    if (r == null) { // no more input to be expected...
      // Close the currently open output file here so the FILE_IO lineage event is emitted
      // before PipelineCompleted flushes the lineage hub. dispose() is only a safety net
      // and runs after the flush in the local pipeline engine's normal completion path.
      closeFile();
      setOutputDone();
      return false;
    }

    writeRowToFile(getInputRowMeta(), r);

    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    putRow(data.outputRowMeta, r); // in case we want it to go further...

    if (checkFeedback(getLinesOutput()) && isBasic()) {
      logBasic("linenr " + getLinesOutput());
    }

    return result;
  }

  private void writeRowToFile(IRowMeta rowMeta, Object[] r) throws HopException {
    try {
      if (first) {
        data.formatRowMeta = rowMeta.clone();

        first = false;

        data.fieldnrs = new int[meta.getOutputFields().size()];
        for (int i = 0; i < meta.getOutputFields().size(); i++) {
          XmlField xmlField = meta.getOutputFields().get(i);

          data.fieldnrs[i] = data.formatRowMeta.indexOfValue(xmlField.getFieldName());
          if (data.fieldnrs[i] < 0) {
            throw new HopException(
                "Field [" + xmlField.getFieldName() + "] couldn't be found in the input stream!");
          }

          // Apply the formatting settings to the valueMeta object...
          //
          IValueMeta valueMeta = data.formatRowMeta.getValueMeta(data.fieldnrs[i]);

          valueMeta.setConversionMask(xmlField.getFormat());
          valueMeta.setLength(xmlField.getLength(), xmlField.getPrecision());
          valueMeta.setDecimalSymbol(xmlField.getDecimalSymbol());
          valueMeta.setGroupingSymbol(xmlField.getGroupingSymbol());
          valueMeta.setCurrencySymbol(xmlField.getCurrencySymbol());
        }
      }

      if (meta.getOutputFields().isEmpty()) {
        /*
         * Write all values in stream to text file.
         */

        // OK, write a new row to the XML file:
        data.writer.writeStartElement(meta.getRepeatElement());

        for (int i = 0; i < data.formatRowMeta.size(); i++) {
          // Put a variables between the XML elements of the row
          //
          if (i > 0) {
            data.writer.writeCharacters(" ");
          }

          IValueMeta valueMeta = data.formatRowMeta.getValueMeta(i);
          Object valueData = r[i];

          writeField(valueMeta, valueData, valueMeta.getName());
        }
      } else {
        /*
         * Only write the fields specified!
         */
        // Write a new row to the XML file:
        data.writer.writeStartElement(meta.getRepeatElement());

        // First do the attributes and write them...
        writeRowAttributes(r);

        // Now write the elements
        //
        for (int i = 0; i < meta.getOutputFields().size(); i++) {
          XmlField outputField = meta.getOutputFields().get(i);
          if (outputField.getContentType() == ContentType.Element) {
            if (i > 0) {
              data.writer.writeCharacters(" "); // a variables between
              // elements
            }

            IValueMeta valueMeta = data.formatRowMeta.getValueMeta(data.fieldnrs[i]);
            Object valueData = r[data.fieldnrs[i]];

            String elementName = outputField.getElementName();
            if (Utils.isEmpty(elementName)) {
              elementName = outputField.getFieldName();
            }

            if (!(valueMeta.isNull(valueData) && meta.getFileDetails().isOmitNullValues())) {
              writeField(valueMeta, valueData, elementName);
            }
          }
        }
      }

      data.writer.writeEndElement();
      data.writer.writeCharacters(EOL);
    } catch (Exception e) {
      throw new HopException(
          "Error writing XML row :"
              + e.toString()
              + Const.CR
              + "Row: "
              + getInputRowMeta().getString(r),
          e);
    }

    incrementLinesOutput();
  }

  void writeRowAttributes(Object[] r) throws HopValueException, XMLStreamException {
    for (int i = 0; i < meta.getOutputFields().size(); i++) {
      XmlField xmlField = meta.getOutputFields().get(i);
      if (xmlField.getContentType() == ContentType.Attribute) {
        IValueMeta valueMeta = data.formatRowMeta.getValueMeta(data.fieldnrs[i]);
        Object valueData = r[data.fieldnrs[i]];

        String elementName = xmlField.getElementName();
        if (Utils.isEmpty(elementName)) {
          elementName = xmlField.getFieldName();
        }

        if (valueData != null) {
          data.writer.writeAttribute(elementName, valueMeta.getString(valueData));
        }
      }
    }
  }

  private void writeField(IValueMeta valueMeta, Object valueData, String element)
      throws HopTransformException {
    try {
      String value = valueMeta.getString(valueData);
      if (value != null) {
        data.writer.writeStartElement(element);
        data.writer.writeCharacters(value);
        data.writer.writeEndElement();
      } else {
        data.writer.writeEmptyElement(element);
      }
    } catch (Exception e) {
      throw new HopTransformException("Error writing line :", e);
    }
  }

  public String buildFilename(boolean ziparchive) {
    return meta.buildFilename(this, getCopy(), data.splitnr, ziparchive);
  }

  public boolean openNewFile() {
    boolean retval = false;
    data.writer = null;

    try {

      FileObject file = HopVfs.getFileObject(buildFilename(true), variables);
      data.outputVfsFile = file;

      if (meta.getFileDetails().isAddToResultFilenames()) {
        // Add this to the result file names...
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                file,
                getPipelineMeta().getName(),
                getTransformName());
        resultFile.setComment("This file was created with a xml output transform");
        addResultFile(resultFile);
      }

      if (meta.getFileDetails().isZipped()) {
        OutputStream fos = HopVfs.getOutputStream(file, false);
        data.countingStream = new CountingOutputStream(fos);
        data.zip = new ZipOutputStream(data.countingStream);
        File entry = new File(buildFilename(false));
        ZipEntry zipentry = new ZipEntry(entry.getName());
        zipentry.setComment("Compressed by Apache Hop");
        data.zip.putNextEntry(zipentry);
        outputStream = data.zip;
      } else {
        OutputStream fos = HopVfs.getOutputStream(file, false);
        data.countingStream = new CountingOutputStream(fos);
        outputStream = data.countingStream;
      }
      if (!Utils.isEmpty(meta.getEncoding())) {
        if (isBasic()) {
          logBasic("Opening output stream in encoding: " + meta.getEncoding());
        }
        data.writer = XML_OUT_FACTORY.createXMLStreamWriter(outputStream, meta.getEncoding());
        data.writer.writeStartDocument(meta.getEncoding(), "1.0");
      } else {
        if (isBasic()) {
          logBasic("Opening output stream in default encoding : " + Const.UTF_8);
        }
        data.writer = XML_OUT_FACTORY.createXMLStreamWriter(outputStream);
        data.writer.writeStartDocument(Const.UTF_8, "1.0");
      }
      data.writer.writeCharacters(EOL);

      // OK, write the header & the parent element:
      data.writer.writeStartElement(meta.getMainElement());
      // Add the name variables if defined
      if ((meta.getNameSpace() != null) && (!"".equals(meta.getNameSpace()))) {
        data.writer.writeDefaultNamespace(meta.getNameSpace());
      }
      data.writer.writeCharacters(EOL);

      retval = true;
    } catch (Exception e) {
      logError("Error opening new file : " + e.toString());
    }

    data.splitnr++;

    return retval;
  }

  void closeOutputStream(OutputStream stream) {
    try {
      if (stream != null) {
        stream.close();
      }
    } catch (Exception e) {
      logError("Error closing output stream : " + e.toString());
    }
  }

  private void emitXmlOutputLineage(long written) {
    if (data.isBeamContext() || written <= 0 || data.outputVfsFile == null) {
      return;
    }
    try {
      LineageFileIoEmitter.emitTransformFileIo(
          this, FileIoOperation.WRITE, null, data.outputVfsFile, written, true, null);
    } catch (Exception ignored) {
      // optional lineage
    }
  }

  private boolean closeFile() {
    boolean retval = false;
    if (data.OpenedNewFile) {
      try {
        // Close the parent element
        data.writer.writeEndElement();
        data.writer.writeCharacters(EOL);

        data.writer.writeEndDocument();
        data.writer.close();

        if (meta.getFileDetails().isZipped()) {
          data.zip.closeEntry();
          data.zip.finish();
          if (data.countingStream != null) {
            long written = data.countingStream.getCount();
            dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + written;
            emitXmlOutputLineage(written);
          }
          data.zip.close();
        } else {
          if (data.countingStream != null) {
            long written = data.countingStream.getCount();
            dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + written;
            emitXmlOutputLineage(written);
          }
        }
        closeOutputStream(outputStream);
        data.outputVfsFile = null;

        retval = true;
      } catch (Exception e) {
        // Ignore errors
      }
    }
    return retval;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      data.splitnr = 0;
      if (!meta.getFileDetails().isDoNotOpenNewFileInit()) {
        if (openNewFile()) {
          data.OpenedNewFile = true;
          return true;
        } else {
          logError("Couldn't open file " + meta.getFileDetails().getFileName());
          setErrors(1L);
          stopAll();
        }
      } else {
        return true;
      }
    }
    return false;
  }

  @Override
  public void dispose() {
    closeFile();

    super.dispose();
  }
}
