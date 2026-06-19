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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class JsonEOutput extends BaseTransform<JsonEOutputMeta, JsonEOutputData> {
  private static final Class<?> PKG =
      JsonEOutput.class; // for i18n purposes, needed by Translator2!!

  public Object[] prevRow;
  private JsonNodeFactory nc;
  private ObjectMapper mapper;
  private ObjectNode currentNode;

  public JsonEOutput(
      TransformMeta transformMeta,
      JsonEOutputMeta meta,
      JsonEOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    if (super.init()) {
      // Output Value field is always required to output JSON resulting by looping over group keys
      if (Utils.isEmpty(resolve(meta.getOutputValue()))) {
        logError(BaseMessages.getString(PKG, "JsonOutput.Error.MissingOutputFieldName"));
        stopAll();
        setErrors(1);
        return false;
      }
      if (meta.getOperationType() == JsonEOutputMeta.OperationType.WRITE_TO_FILE
          || meta.getOperationType() == JsonEOutputMeta.OperationType.BOTH) {
        // Init global json items array only if output to file is needed
        data.jsonItems = new ArrayList<>();
        data.isWriteToFile = true;
        if (!meta.getFileSettings().isDoNotOpenNewFileInit()
            && data.isWriteToFile
            && !openNewFile()) {
          logError(BaseMessages.getString(PKG, "JsonOutput.Error.OpenNewFile", buildFilename()));
          stopAll();
          setErrors(1);
          return false;
        }
      }

      data.realBlocName = Const.NVL(resolve(meta.getJsonBloc()), "");
      return true;
    }

    return false;
  }

  @Override
  public boolean processRow() throws HopException {
    // This also waits for a row to be finished.
    Object[] r = getRow();
    if (r == null) {
      // only attempt writing to file when the first row is not empty
      if (data.isWriteToFile && !first && meta.getFileSettings().getSplitOutputAfter() == 0) {
        // no more input to be expected...
        // Let's output the remaining unsafe data
        outputRow(prevRow);
        writeJsonFile();
        setOutputDone();
        return false;
      }

      // Process the leftover data only when a split file size is defined
      // and there are still items pending.
      if (meta.getFileSettings().getSplitOutputAfter() > 0 && !data.jsonItems.isEmpty()) {
        serializeJson(data.jsonItems);
        writeJsonFile();
        setOutputDone();
        return false;
      }

      outputRow(prevRow);
      setOutputDone();
      return false;
    }

    if (first && onFirstRecord(r)) {
      return false;
    }

    data.rowsAreSafe = false;
    manageRowItems(r);
    return true;
  }

  public void manageRowItems(Object[] row) throws HopException {
    ObjectNode itemNode;

    boolean sameGroup = sameGroup(prevRow, row);

    if (meta.isUseSingleItemPerGroup()) {
      /*
       * If grouped rows are forced to produce a single item, reuse the same itemNode as long as the
       * row belongs to the previous group. Feature #3287
       */
      if (!sameGroup || currentNode == null) {
        currentNode = new ObjectNode(nc);
      }

      itemNode = currentNode;

    } else {
      // Create a new object with specified fields
      itemNode = new ObjectNode(nc);
    }

    if (!sameGroup && !data.jsonKeyGroupItems.isEmpty()) {
      // Output the new row
      if (isDebug()) {
        logDebug("Record Num: " + data.nrRow + " - Generating JSON chunk");
      }
      outputRow(prevRow);
      data.jsonKeyGroupItems = new ArrayList<>();
    }

    for (int i = 0; i < data.nrFields; i++) {
      JsonEOutputField outputField = meta.getOutputFields().get(i);

      String jsonAttributeName = getJsonAttributeName(outputField);
      boolean putBlank = !outputField.isRemoveIfBlank();

      /*
       * Prepare the array node to collect all values of a field inside a group into an array. Skip
       * fields appearing in the grouped fields since they are always unique per group.
       */
      ArrayNode arNode = null;
      if (meta.isUseSingleItemPerGroup() && !data.keyFields.contains(i)) {
        if (!itemNode.has(jsonAttributeName)) {
          arNode = itemNode.putArray(jsonAttributeName);
        } else {
          arNode = (ArrayNode) itemNode.get(jsonAttributeName);
        }
        // In case whe have an array to store data, the flag to remove blanks is effectivly
        // deactivated.
        putBlank = false;
      }

      IValueMeta v = data.inputRowMeta.getValueMeta(data.fieldIndexes[i]);
      switch (v.getType()) {
        case IValueMeta.TYPE_BOOLEAN:
          Boolean boolValue = data.inputRowMeta.getBoolean(row, data.fieldIndexes[i]);

          if (putBlank) {
            itemNode.put(jsonAttributeName, boolValue);
          } else if (boolValue != null) {
            if (arNode == null) {
              itemNode.put(jsonAttributeName, boolValue);
            } else {
              arNode.add(boolValue);
            }
          }
          break;

        case IValueMeta.TYPE_INTEGER:
          Long integerValue = data.inputRowMeta.getInteger(row, data.fieldIndexes[i]);

          if (putBlank) {
            itemNode.put(jsonAttributeName, integerValue);
          } else if (integerValue != null) {
            if (arNode == null) {
              itemNode.put(jsonAttributeName, integerValue);
            } else {
              arNode.add(integerValue);
            }
          }
          break;
        case IValueMeta.TYPE_NUMBER:
          Double numberValue = data.inputRowMeta.getNumber(row, data.fieldIndexes[i]);

          if (putBlank) {
            itemNode.put(jsonAttributeName, numberValue);
          } else if (numberValue != null) {
            if (arNode == null) {
              itemNode.put(jsonAttributeName, numberValue);
            } else {
              arNode.add(numberValue);
            }
          }
          break;
        case IValueMeta.TYPE_BIGNUMBER:
          BigDecimal bignumberValue = data.inputRowMeta.getBigNumber(row, data.fieldIndexes[i]);

          if (putBlank) {
            itemNode.put(jsonAttributeName, bignumberValue);
          } else if (bignumberValue != null) {
            if (arNode == null) {
              itemNode.put(jsonAttributeName, bignumberValue);
            } else {
              arNode.add(bignumberValue);
            }
          }
          break;
        default:
          String value = data.inputRowMeta.getString(row, data.fieldIndexes[i]);
          if (putBlank && !outputField.isJsonFragment()) {
            itemNode.put(jsonAttributeName, value);
          } else if (value != null) {
            if (outputField.isJsonFragment()) {
              try {
                JsonNode jsonNode = mapper.readTree(value);
                if (outputField.isWithoutEnclosing()) {
                  itemNode.setAll((ObjectNode) jsonNode);
                } else {
                  if (arNode == null) {
                    itemNode.set(jsonAttributeName, jsonNode);
                  } else {
                    arNode.add(jsonNode);
                  }
                }
              } catch (IOException e) {
                throw new HopTransformException(
                    BaseMessages.getString(PKG, "JsonOutput.Error.Casting"), e);
              }
            } else {
              if (arNode == null) {
                itemNode.put(jsonAttributeName, value);
              } else {
                arNode.add(value);
              }
            }
          }

          break;
      }
    }
    if (meta.getFileSettings().getSplitOutputAfter() > 0) {
      data.jsonItems.add(itemNode);
    }

    /*
     * Only add a new item node if each row should produce a single JSON object or in case of a
     * single JSON object for a group of rows, if no item node was added yet. This happens for the
     * first new row of a group only.
     */
    if (!meta.isUseSingleItemPerGroup() || data.jsonKeyGroupItems.isEmpty()) {
      data.jsonKeyGroupItems.add(itemNode);
    }

    prevRow = data.inputRowMeta.cloneRow(row); // copy the row to previous
    data.nrRow++;

    if (meta.getFileSettings().getSplitOutputAfter() > 0
        && (data.nrRow) % meta.getFileSettings().getSplitOutputAfter() == 0) {
      // Output the new row
      if (isDebug()) {
        logDebug("Record Num: " + data.nrRow + " - Generating JSON chunk");
      }
      serializeJson(data.jsonItems);
      writeJsonFile();
      data.jsonItems = new ArrayList<>();
    }
  }

  private String getJsonAttributeName(JsonEOutputField field) {
    String elementName = variables.resolve(field.getElementName());
    return Const.NVL(elementName, field.getFieldName());
  }

  private String getKeyJsonAttributeName(JsonEOutputKeyField field) {
    String elementName = variables.resolve(field.getElementName());
    return Const.NVL(elementName, field.getFieldName());
  }

  private void outputRow(Object[] rowData) throws HopException {
    // We can now output an object
    ObjectNode globalItemNode = null;

    if (Utils.isEmpty(data.jsonKeyGroupItems)) return;

    if (!data.jsonKeyGroupItems.isEmpty()) {
      serializeJson(data.jsonKeyGroupItems);
    }

    data.jsonLength = data.jsonSerialized.length();

    if (data.outputRowMeta != null) {

      Object[] keyRow = new Object[meta.getKeyFields().size()];

      // Create a new object with specified fields
      if (data.isWriteToFile) {
        globalItemNode = new ObjectNode(nc);
      }

      for (int i = 0; i < meta.getKeyFields().size(); i++) {
        JsonEOutputKeyField keyField = meta.getKeyFields().get(i);
        try {
          IValueMeta vmi = data.inputRowMeta.getValueMeta(data.keysGroupIndexes[i]);
          switch (vmi.getType()) {
            case IValueMeta.TYPE_BOOLEAN:
              keyRow[i] = data.inputRowMeta.getBoolean(rowData, data.keysGroupIndexes[i]);
              if (data.isWriteToFile) {
                globalItemNode.put(getKeyJsonAttributeName(keyField), (Boolean) keyRow[i]);
              }
              break;
            case IValueMeta.TYPE_INTEGER:
              keyRow[i] = data.inputRowMeta.getInteger(rowData, data.keysGroupIndexes[i]);
              if (data.isWriteToFile) {
                globalItemNode.put(getKeyJsonAttributeName(keyField), (Long) keyRow[i]);
              }
              break;
            case IValueMeta.TYPE_NUMBER:
              keyRow[i] = data.inputRowMeta.getNumber(rowData, data.keysGroupIndexes[i]);
              if (data.isWriteToFile) {
                globalItemNode.put(getKeyJsonAttributeName(keyField), (Double) keyRow[i]);
              }
              break;
            case IValueMeta.TYPE_BIGNUMBER:
              keyRow[i] = data.inputRowMeta.getBigNumber(rowData, data.keysGroupIndexes[i]);
              if (data.isWriteToFile) {
                globalItemNode.put(getKeyJsonAttributeName(keyField), (BigDecimal) keyRow[i]);
              }
              break;
            default:
              keyRow[i] = data.inputRowMeta.getString(rowData, data.keysGroupIndexes[i]);
              if (data.isWriteToFile) {
                globalItemNode.put(getKeyJsonAttributeName(keyField), (String) keyRow[i]);
              }
              break;
          }
        } catch (HopValueException e) {
          throw new HopException(
              "Error getting json values for key field: " + keyField.getFieldName(), e);
        }
      }

      if (data.isWriteToFile) {
        try {
          // JSON serialization here...
          JsonNode jsonNode = mapper.readTree(data.jsonSerialized);
          if (meta.getOutputValue() != null) {
            globalItemNode.set(meta.getOutputValue(), jsonNode);
          }
        } catch (IOException e) {
          throw new HopException("Error serializing JSON values", e);
        }
        data.jsonItems.add(globalItemNode);
      }

      Object[] additionalRowFields = new Object[2];

      additionalRowFields[0] = data.jsonSerialized;

      // Fill accessory fields
      if (!Utils.isEmpty(meta.getJsonSizeFieldName())) {
        additionalRowFields[1] = data.jsonLength;
      }

      Object[] outputRowData = RowDataUtil.addRowData(keyRow, keyRow.length, additionalRowFields);
      incrementLinesOutput();

      putRow(data.outputRowMeta, outputRowData);
    }

    // Data are safe
    data.rowsAreSafe = true;
  }

  private void writeJsonFile() throws HopTransformException {
    // Open a file
    if (data.isWriteToFile && !openNewFile())
      throw new HopTransformException(
          BaseMessages.getString(PKG, "JsonOutput.Error.OpenNewFile", buildFilename()));
    // Write data to file
    try {
      data.writer.write(data.jsonSerialized);
    } catch (Exception e) {
      throw new HopTransformException(BaseMessages.getString(PKG, "JsonOutput.Error.Writing"), e);
    }
    // Close file
    closeFile();
  }

  private void serializeJson(List<ObjectNode> jsonItemsList) throws HopException {
    ObjectNode theNode = new ObjectNode(nc);
    Object listValue = meta.isUseArrayWithSingleInstance() ? jsonItemsList : jsonItemsList.get(0);
    try {
      if (!Utils.isEmpty(meta.getJsonBloc())) {
        // TBD Try to understand if this can have a performance impact and do it better...
        theNode.set(
            meta.getJsonBloc(),
            mapper.readTree(
                mapper.writeValueAsString(jsonItemsList.size() > 1 ? jsonItemsList : listValue)));
        if (meta.isJsonPrettified()) {
          data.jsonSerialized = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(theNode);
        } else {
          data.jsonSerialized = mapper.writeValueAsString(theNode);
        }
      } else if (meta.isJsonPrettified()) {
        data.jsonSerialized =
            mapper
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString((jsonItemsList.size() > 1 ? jsonItemsList : listValue));
      } else {
        data.jsonSerialized =
            mapper.writeValueAsString((jsonItemsList.size() > 1 ? jsonItemsList : listValue));
      }
    } catch (IOException e) {
      throw new HopException("Error serializing JSON", e);
    }
  }

  // Is the row r of the same group as previous?
  private boolean sameGroup(Object[] previous, Object[] r) throws HopValueException {
    return data.inputRowMeta.compare(previous, r, data.keysGroupIndexes) == 0;
  }

  private boolean onFirstRecord(Object[] r) throws HopException {

    nc = HopJson.newMapper().getNodeFactory();
    mapper = HopJson.newMapper();

    first = false;
    data.inputRowMeta = getInputRowMeta();
    data.inputRowMetaSize = data.inputRowMeta.size();

    // Init previous row copy to this first row
    prevRow = data.inputRowMeta.cloneRow(r); // copy the row to previous

    // Create new structure for output fields
    data.outputRowMeta = new RowMeta();
    List<JsonEOutputKeyField> keyFields = meta.getKeyFields();
    for (int i = 0; i < meta.getKeyFields().size(); i++) {
      IValueMeta vmi =
          data.inputRowMeta.getValueMeta(
              data.inputRowMeta.indexOfValue(keyFields.get(i).getFieldName()));
      data.outputRowMeta.addValueMeta(i, vmi);
    }

    // This is JSON block's column
    data.outputRowMeta.addValueMeta(
        meta.getKeyFields().size(), new ValueMetaString(meta.getOutputValue()));

    int fieldLength = meta.getKeyFields().size() + 1;
    if (!Utils.isEmpty(meta.getJsonSizeFieldName())) {
      data.outputRowMeta.addValueMeta(
          fieldLength, new ValueMetaInteger(meta.getJsonSizeFieldName()));
    }

    initDataFieldsPositionsArray();

    if (initKeyFieldsPositionArray(r)) return true;
    return false;
  }

  private void initDataFieldsPositionsArray() throws HopException {
    // Cache the field name indexes
    //
    data.nrFields = meta.getOutputFields().size();
    data.fieldIndexes = new int[data.nrFields];
    data.keyFields = new HashSet<>();
    for (int i = 0; i < data.nrFields; i++) {
      data.fieldIndexes[i] =
          data.inputRowMeta.indexOfValue(meta.getOutputFields().get(i).getFieldName());
      if (data.fieldIndexes[i] < 0)
        throw new HopException(BaseMessages.getString(PKG, "JsonOutput.Exception.FieldNotFound"));
      JsonEOutputField field = meta.getOutputFields().get(i);
      field.setElementName(variables.resolve(field.getElementName()));

      /*
       * Mark all output fields that are part of the group key fields. This way we can avoid
       * collecting unique values of each group inside an array. Feature #3287
       */
      for (JsonEOutputKeyField jsonEOutputKeyField : meta.getKeyFields()) {
        if (jsonEOutputKeyField.getFieldName().equals(field.getFieldName())) {
          data.keyFields.add(i);
          break;
        }
      }
    }
  }

  private boolean initKeyFieldsPositionArray(Object[] r) {
    data.keysGroupIndexes = new int[meta.getKeyFields().size()];

    for (int i = 0; i < meta.getKeyFields().size(); i++) {
      data.keysGroupIndexes[i] =
          data.inputRowMeta.indexOfValue(meta.getKeyFields().get(i).getFieldName());
      if ((r != null) && (data.keysGroupIndexes[i] < 0)) {
        setErrors(1);
        stopAll();
        return true;
      }
    }
    return false;
  }

  @Override
  public void dispose() {

    if (data.jsonKeyGroupItems != null) {
      data.jsonKeyGroupItems = null;
    }

    closeFile();
    super.dispose();
  }

  private void createParentFolder(String filename) throws HopTransformException {
    if (!meta.getFileSettings().isCreateParentFolder()) return;
    // Check for parent folder
    FileObject parentfolder = null;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject(filename).getParent();
      if (!parentfolder.exists()) {
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG, "JsonOutput.Error.ParentFolderNotExist", parentfolder.getName()));
        }
        parentfolder.createFolder();
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "JsonOutput.Log.ParentFolderCreated"));
        }
      }
    } catch (Exception e) {
      throw new HopTransformException(
          BaseMessages.getString(
              PKG, "JsonOutput.Error.ErrorCreatingParentFolder", parentfolder.getName()));
    } finally {
      if (parentfolder != null) {
        try {
          parentfolder.close();
        } catch (Exception ex) {
          /* Ignore */
        }
      }
    }
  }

  public boolean openNewFile() {

    if (data.writer != null) return true;
    boolean retval = false;
    try {

      String filename = buildFilename();
      createParentFolder(filename);
      if (meta.isAddingToResult()) {
        // Add this to the result file names...
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                HopVfs.getFileObject(filename),
                getPipelineMeta().getName(),
                getTransformName());
        resultFile.setComment(BaseMessages.getString(PKG, "JsonOutput.ResultFilenames.Comment"));
        addResultFile(resultFile);
      }

      OutputStream outputStream;
      OutputStream fos = HopVfs.getOutputStream(filename, meta.getFileSettings().isFileAppended());
      data.countingStream = new CountingOutputStream(fos);
      outputStream = data.countingStream;

      if (!Utils.isEmpty(meta.getEncoding())) {
        data.writer =
            new OutputStreamWriter(
                new BufferedOutputStream(outputStream, 5000), resolve(meta.getEncoding()));
      } else {
        data.writer = new OutputStreamWriter(new BufferedOutputStream(outputStream, 5000));
      }

      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "JsonOutput.FileOpened", filename));
      }

      data.openedFilename = filename;
      data.splitnr++;

      retval = true;

    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "JsonOutput.Error.OpeningFile", e.toString()));
    }

    return retval;
  }

  public String buildFilename() {
    return meta.getFileSettings()
        .buildFilename(variables, getCopy() + "", getPartitionId(), data.splitnr + "", false);
  }

  private boolean closeFile() {
    if (data.writer == null) return true;
    boolean retval = false;

    try {
      data.writer.flush();
      if (data.countingStream != null) {
        long written = data.countingStream.getCount();
        dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + written;
        if (!data.isBeamContext() && written > 0 && data.openedFilename != null) {
          try {
            FileObject outFile = HopVfs.getFileObject(data.openedFilename, this);
            LineageFileIoEmitter.emitTransformFileIo(
                this, FileIoOperation.WRITE, null, outFile, written, true, null);
          } catch (Exception ignored) {
            // optional lineage
          }
        }
      }
      data.openedFilename = null;
      data.writer.close();
      data.writer = null;
      data.countingStream = null;
      retval = true;
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "JsonOutput.Error.ClosingFile", e.toString()));
      setErrors(1);
      retval = false;
    }

    return retval;
  }
}
