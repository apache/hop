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

package org.apache.hop.pipeline.transforms.tokenreplacement;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.io.CountingInputStream;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class TokenReplacement extends BaseTransform<TokenReplacementMeta, TokenReplacementData> {
  private static final Class<?> PKG = TokenReplacementMeta.class;
  public static final String CONST_NOT_FOUND_ON_INPUT_STREAM = " not found on input stream.";
  public static final String CONST_FIELD = "field";

  public TokenReplacement(
      TransformMeta transformMeta,
      TokenReplacementMeta meta,
      TokenReplacementData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public synchronized boolean processRow() throws HopException {

    boolean result = true;
    Object[] r = getRow(); // This also waits for a row to be finished.

    if (first && r != null) {
      first = false;

      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = getInputRowMeta().clone();

      if (meta.getOutputType().equalsIgnoreCase(CONST_FIELD)) {
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      }
      if (meta.getOutputType().equalsIgnoreCase("file") && !meta.isOutputFileNameInField()) {
        if (meta.getOutputFileName() != null) {
          String filename =
              meta.buildFilename(
                  meta.getOutputFileName(), this, getCopy(), getPartitionId(), data.splitnr);

          openNewOutputFile(filename);
        } else {
          throw new HopException("Output file name cannot be null.");
        }
      }
    }

    if (r == null) {
      // no more input to be expected...
      closeAllOutputFiles();
      setOutputDone();
      return false;
    }

    if (meta.getOutputType().equalsIgnoreCase("file")
        && !meta.isOutputFileNameInField()
        && meta.getSplitEvery() > 0
        && data.rowNumber % meta.getSplitEvery() == 0
        && data.rowNumber > 0) {
      closeAllOutputFiles();
      data.splitnr++;
      String filename =
          meta.buildFilename(
              meta.getOutputFileName(), this, getCopy(), getPartitionId(), data.splitnr);
      openNewOutputFile(filename);
    }

    String outputFilename = "";
    if (meta.getOutputType().equalsIgnoreCase("file") && !meta.isOutputFileNameInField()) {
      outputFilename =
          meta.buildFilename(
              meta.getOutputFileName(), this, getCopy(), getPartitionId(), data.splitnr);
    } else if (meta.getOutputType().equalsIgnoreCase("file") && meta.isOutputFileNameInField()) {
      String filenameValue =
          data.inputRowMeta.getString(r, resolve(meta.getOutputFileNameField()), "");
      if (!Utils.isEmpty(filenameValue)) {
        outputFilename = filenameValue;
      } else {
        throw new HopException("Filename cannot be empty.");
      }
    }

    // The value of every token field in this row, by its position in the transform's field list
    if (data.tokenValuePositions == null) {
      prepareTokens();
    }
    String[] tokenValues = getTokenValues(r);

    // Text and field input: the template, split into fixed text and tokens.
    // File input: streamed through a reader, so large files are never read into memory at once.
    TokenTemplate template = null;
    Reader reader = null;
    String inputFilename = "";

    if (meta.getInputType().equalsIgnoreCase("text")) {
      if (data.textTemplate == null) {
        data.textTemplate =
            TokenTemplate.parse(
                Const.NVL(meta.getInputText(), ""),
                data.tokenStartMarker,
                data.tokenEndMarker,
                data.tokenValuePositions);
      }
      template = data.textTemplate;

    } else if (meta.getInputType().equalsIgnoreCase(CONST_FIELD)) {
      if (data.inputFieldIndex < 0) {
        data.inputFieldIndex = data.inputRowMeta.indexOfValue(meta.getInputFieldName());
        if (data.inputFieldIndex < 0) {
          throw new HopValueException(
              "Input field " + meta.getInputFieldName() + CONST_NOT_FOUND_ON_INPUT_STREAM);
        }
      }
      String inputString = Const.NVL(data.inputRowMeta.getString(r, data.inputFieldIndex), "");
      // Rows often share the same template: split it again only when it changes
      if (data.fieldTemplate == null || !inputString.equals(data.fieldTemplateText)) {
        data.fieldTemplate =
            TokenTemplate.parse(
                inputString, data.tokenStartMarker, data.tokenEndMarker, data.tokenValuePositions);
        data.fieldTemplateText = inputString;
      }
      template = data.fieldTemplate;

    } else if (meta.getInputType().equalsIgnoreCase("file")) {
      if (meta.isInputFileNameInField()) {
        if (data.inputRowMeta.indexOfValue(resolve(meta.getInputFileNameField())) >= 0) {
          inputFilename = data.inputRowMeta.getString(r, resolve(meta.getInputFileNameField()), "");
        } else {
          throw new HopValueException(
              "Input filename field "
                  + resolve(meta.getInputFileNameField())
                  + CONST_NOT_FOUND_ON_INPUT_STREAM);
        }
      } else {
        inputFilename = resolve(meta.getInputFileName());
      }

      if (Utils.isEmpty(inputFilename)) {
        throw new HopValueException("Input filename cannot be empty");
      }
      if (!HopVfs.fileExists(inputFilename, variables)) {
        throw new HopException("Input file " + inputFilename + " does not exist.");
      }
      data.currentCountingInputStream =
          new CountingInputStream(HopVfs.getInputStream(inputFilename, variables));
      String inputEncoding = Const.NVL(resolve(meta.getOutputFileEncoding()), Const.UTF_8);
      reader =
          new TokenReplacingReader(
              getTokenResolver(tokenValues),
              new InputStreamReader(
                  data.currentCountingInputStream, Charset.forName(inputEncoding)),
              data.tokenStartMarker,
              data.tokenEndMarker);

      if (meta.isAddInputFileNameToResult()) {
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                HopVfs.getFileObject(inputFilename, variables),
                getTransformMeta().getName(),
                getTransformName());
        resultFile.setComment(BaseMessages.getString(PKG, "TokenReplacement.AddInputResultFile"));
        addResultFile(resultFile);
      }
    } else {
      throw new HopException("Unsupported input type " + meta.getInputType());
    }

    OutputStream bufferedWriter = null;

    if (!meta.getOutputType().equalsIgnoreCase(CONST_FIELD)) {
      if (meta.getOutputType().equalsIgnoreCase("file")) {

        if (inputFilename.equals(outputFilename)) {
          throw new HopException(
              "Input and output filenames must not be the same " + inputFilename);
        }

        int fileIndex = data.openFiles.indexOf(outputFilename);
        if (fileIndex < 0) {
          openNewOutputFile(outputFilename);
          fileIndex = data.openFiles.indexOf(outputFilename);
        }

        bufferedWriter = data.openBufferedWriters.get(fileIndex);

      } else {
        throw new HopException("Unsupported output type " + meta.getOutputType());
      }
    }

    String output = "";
    boolean toField = meta.getOutputType().equalsIgnoreCase(CONST_FIELD);
    StringBuilder text = data.outputText;
    text.setLength(0);

    try {
      if (template != null) {
        template.appendTo(tokenValues, text);
        if (!toField) {
          writeText(bufferedWriter, text);
        }
      } else {
        int length;
        while ((length = reader.read(data.readBuffer)) > 0) {
          if (toField) {
            text.append(data.readBuffer, 0, length);
          } else {
            writeText(bufferedWriter, CharBuffer.wrap(data.readBuffer, 0, length));
          }
        }
      }

      if (toField) {
        output = text.toString();
      } else {
        bufferedWriter.write(meta.getOutputFileFormatString().getBytes());
      }
    } catch (IOException ex) {
      throw new HopException(ex.getMessage(), ex);
    } finally {
      if (reader != null) {
        try {
          if (data.currentCountingInputStream != null) {
            long bytesIn = data.currentCountingInputStream.getCount();
            dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + bytesIn;
            if (!Utils.isEmpty(inputFilename) && bytesIn > 0) {
              try {
                FileObject inFile = HopVfs.getFileObject(inputFilename, variables);
                LineageFileIoEmitter.emitTransformFileIo(
                    this, FileIoOperation.READ, inFile, null, bytesIn, true, null);
              } catch (Exception ignored) {
                // optional lineage
              }
            }
          }
          data.currentCountingInputStream = null;
          reader.close();
        } catch (IOException ex) {
          throw new HopException(ex.getMessage(), ex);
        }
      }
    }

    if (meta.getOutputType().equalsIgnoreCase(CONST_FIELD)) {
      r = RowDataUtil.addValueData(r, data.outputRowMeta.size() - 1, output);
    } else if (meta.getOutputType().equalsIgnoreCase("file")) {
      incrementLinesWritten();
    }

    putRow(data.outputRowMeta, r); // in case we want it to go further...
    data.rowNumber++;
    if (checkFeedback(getLinesOutput()) && isBasic()) {
      logBasic("linenr " + getLinesOutput());
    }

    return result;
  }

  /**
   * Looks up, once, what every row needs: the position of each token field, the token markers and
   * whether an empty string differs from null.
   */
  private void prepareTokens() throws HopValueException {
    List<TokenReplacementField> fields = meta.getTokenReplacementFields();
    int[] indexes = new int[fields.size()];
    Map<String, Integer> positions = new HashMap<>();
    for (int i = 0; i < indexes.length; i++) {
      TokenReplacementField field = fields.get(i);
      indexes[i] = data.inputRowMeta.indexOfValue(field.getName());
      if (indexes[i] < 0) {
        throw new HopValueException("Field " + field.getName() + CONST_NOT_FOUND_ON_INPUT_STREAM);
      }
      // When two fields share a token name, the last one wins, as before
      positions.put(field.getTokenName(), i);
    }
    data.tokenStartMarker = resolve(meta.getTokenStartString());
    data.tokenEndMarker = resolve(meta.getTokenEndString());
    data.emptyStringDiffersFromNull =
        BooleanUtils.toBoolean(
            Const.getEnvironmentVariable("KETTLE_EMPTY_STRING_DIFFERS_FROM_NULL", "N"));
    data.tokenFieldIndexes = indexes;
    data.tokenValuePositions = positions;
  }

  /**
   * The value of every token field in a row. Variables in a value are resolved; a null value
   * becomes an empty string unless KETTLE_EMPTY_STRING_DIFFERS_FROM_NULL is set, and then keeps its
   * token as written.
   */
  private String[] getTokenValues(Object[] row) throws HopValueException {
    String[] values = new String[data.tokenFieldIndexes.length];
    for (int i = 0; i < values.length; i++) {
      String value = resolveValue(data.inputRowMeta.getString(row, data.tokenFieldIndexes[i]));
      if (value == null && !data.emptyStringDiffersFromNull) {
        value = "";
      }
      values[i] = value;
    }
    return values;
  }

  /**
   * Resolves the variables in a field value. A value without a variable expression comes out of
   * {@link #resolve(String)} unchanged, so that (synchronized) call is skipped for it.
   */
  private String resolveValue(String value) {
    if (Utils.isEmpty(value) || !mayContainVariables(value)) {
      return value;
    }
    return resolve(value);
  }

  /** True when a text holds one of the expressions that {@link #resolve(String)} replaces. */
  static boolean mayContainVariables(String text) {
    return text.contains(StringUtil.UNIX_OPEN)
        || text.contains(StringUtil.WINDOWS_OPEN)
        || text.contains(StringUtil.HEX_OPEN)
        || text.contains(StringUtil.RESOLVER_OPEN);
  }

  /** The token values of a row by token name, for the reader of an input file. */
  private TokenResolver getTokenResolver(String[] tokenValues) {
    TokenResolver resolver = new TokenResolver();
    List<TokenReplacementField> fields = meta.getTokenReplacementFields();
    for (int i = 0; i < tokenValues.length; i++) {
      resolver.addToken(fields.get(i).getTokenName(), tokenValues[i]);
    }
    return resolver;
  }

  /** Writes text to an output file in the output file encoding. */
  private void writeText(OutputStream outputStream, CharSequence text) throws IOException {
    if (data.outputCharset == null) {
      data.outputCharset = Charset.forName(meta.getOutputFileEncoding());
    }
    ByteBuffer bytes = data.outputCharset.encode(CharBuffer.wrap(text));
    outputStream.write(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining());
  }

  @Override
  public boolean init() {

    return super.init();
  }

  @Override
  public void dispose() {

    try {
      closeAllOutputFiles();
    } catch (HopException ignored) {
    }

    super.dispose();
  }

  public void openNewOutputFile(String filename) throws HopException {
    if (data.openFiles.contains(filename)) {
      if (isDetailed()) {
        logDetailed("File " + filename + " is already open.");
      }
      return;
    }
    if (meta.isCreateParentFolder()) {
      try {
        createParentFolder(filename);
      } catch (Exception ex) {
        throw new HopException(ex.getMessage(), ex);
      }
    }

    boolean fileExists = HopVfs.fileExists(filename, variables);

    OutputStream writer =
        HopVfs.getOutputStream(filename, meta.isAppendOutputFileName(), variables);
    CountingOutputStream countingStream = new CountingOutputStream(writer);
    OutputStream bufferedWriter = new BufferedOutputStream(countingStream, 5000);

    try {
      if (fileExists && meta.isAppendOutputFileName()) {
        bufferedWriter.write(meta.getOutputFileFormatString().getBytes());
      }
    } catch (IOException ex) {
      throw new HopException(ex.getMessage(), ex);
    }

    data.openFiles.add(filename);
    data.openWriters.add(countingStream);
    data.openBufferedWriters.add(bufferedWriter);
  }

  public void closeAllOutputFiles() throws HopException {
    try {
      ArrayList<String> outputNames = new ArrayList<>(data.openFiles);
      Iterator<OutputStream> itWriter = data.openWriters.iterator();
      Iterator<OutputStream> itBufferedWriter = data.openBufferedWriters.iterator();
      if (meta.isAddOutputFileNameToResult()) {
        for (String fn : outputNames) {
          ResultFile resultFile =
              new ResultFile(
                  ResultFile.FILE_TYPE_GENERAL,
                  HopVfs.getFileObject(fn, variables),
                  getTransformMeta().getName(),
                  getTransformName());
          resultFile.setComment(
              BaseMessages.getString(PKG, "TokenReplacement.AddOutputResultFile"));
          addResultFile(resultFile);
        }
      }

      while (itBufferedWriter.hasNext()) {
        OutputStream bufferedWriter = itBufferedWriter.next();
        if (bufferedWriter != null) {
          bufferedWriter.flush();
          bufferedWriter.close();
        }
      }

      int outIndex = 0;
      while (itWriter.hasNext()) {
        OutputStream writer = itWriter.next();
        if (writer != null) {
          if (writer instanceof CountingOutputStream cos) {
            long written = cos.getCount();
            dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + written;
            if (!data.isBeamContext() && written > 0 && outIndex < outputNames.size()) {
              try {
                FileObject outFile = HopVfs.getFileObject(outputNames.get(outIndex), variables);
                LineageFileIoEmitter.emitTransformFileIo(
                    this, FileIoOperation.WRITE, null, outFile, written, true, null);
              } catch (Exception ignored) {
                // optional lineage
              }
            }
          }
          writer.close();
        }
        outIndex++;
      }

      data.openBufferedWriters.clear();
      data.openWriters.clear();
      data.openFiles.clear();
    } catch (IOException ex) {
      throw new HopException(ex.getMessage(), ex);
    }
  }

  private void createParentFolder(String filename) throws Exception {
    // Check for parent folder
    FileObject parentfolder = null;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject(filename, variables).getParent();
      if (parentfolder.exists()) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "TokenReplacement.Log.ParentFolderExist", parentfolder.getName()));
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "TokenReplacement.Log.ParentFolderNotExist", parentfolder.getName()));
        }
        if (meta.isCreateParentFolder()) {
          parentfolder.createFolder();
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "TokenReplacement.Log.ParentFolderCreated", parentfolder.getName()));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "TokenReplacement.Log.ParentFolderNotExistCreateIt",
                  parentfolder.getName(),
                  filename));
        }
      }

    } finally {
      if (parentfolder != null) {
        try {
          parentfolder.close();
        } catch (Exception ex) {
          // Ignore
        }
      }
    }
  }
}
