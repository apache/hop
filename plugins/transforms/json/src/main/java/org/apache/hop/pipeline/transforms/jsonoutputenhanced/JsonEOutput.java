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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
  private ObjectMapper fileMapper;
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
      data.isOutputValue = meta.getOperationType() != JsonEOutputMeta.OperationType.WRITE_TO_FILE;
      data.isWriteToFile =
          meta.getOperationType() == JsonEOutputMeta.OperationType.WRITE_TO_FILE
              || meta.getOperationType() == JsonEOutputMeta.OperationType.BOTH;
      // Without group keys every row is an item in the file, unless all rows are merged into a
      // single item. Group keys make every group an item.
      data.streamFileRows =
          data.isWriteToFile && meta.getKeyFields().isEmpty() && !meta.isUseSingleItemPerGroup();
      data.collectGroupItems = data.isOutputValue || (data.isWriteToFile && !data.streamFileRows);

      if (data.isWriteToFile) {
        if (!meta.getFileSettings().isDoNotOpenNewFileInit() && !openNewFile()) {
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
      // no more input to be expected: finish the last group and the file
      if (!first) {
        finishGroup(prevRow);
      }
      if (data.isWriteToFile) {
        finishFile();
      }
      setOutputDone();
      return false;
    }

    if (first && onFirstRecord(r)) {
      return false;
    }

    data.rowsAreSafe = false;
    manageRowItems(r);

    if (!data.isOutputValue) {
      // The JSON only goes to the file: pass the rows on as they are
      putRow(data.inputRowMeta, r);
    }
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

    if (!sameGroup) {
      finishGroup(prevRow);
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
    /*
     * Only add a new item node if each row should produce a single JSON object or in case of a
     * single JSON object for a group of rows, if no item node was added yet. This happens for the
     * first new row of a group only.
     */
    if (data.collectGroupItems
        && (!meta.isUseSingleItemPerGroup() || data.jsonKeyGroupItems.isEmpty())) {
      data.jsonKeyGroupItems.add(itemNode);
    }

    if (data.streamFileRows) {
      writeFileItem(itemNode);
    }

    prevRow = data.inputRowMeta.cloneRow(row); // copy the row to previous
    data.nrRow++;
  }

  private String getJsonAttributeName(JsonEOutputField field) {
    String elementName = variables.resolve(field.getElementName());
    return Const.NVL(elementName, field.getFieldName());
  }

  private String getKeyJsonAttributeName(JsonEOutputKeyField field) {
    String elementName = variables.resolve(field.getElementName());
    return Const.NVL(elementName, field.getFieldName());
  }

  /**
   * A group is complete: send its row to the output field and, when group keys are used, write its
   * item to the file.
   */
  private void finishGroup(Object[] groupRow) throws HopException {
    if (Utils.isEmpty(data.jsonKeyGroupItems)) {
      return;
    }
    if (isDebug()) {
      logDebug("Record Num: " + data.nrRow + " - Generating JSON chunk");
    }
    if (data.isOutputValue) {
      outputRow(groupRow);
    }
    if (data.isWriteToFile && !data.streamFileRows) {
      if (meta.getKeyFields().isEmpty()) {
        // All rows are merged into a single item
        for (ObjectNode item : data.jsonKeyGroupItems) {
          writeFileItem(item);
        }
      } else {
        writeFileItem(buildGroupFileItem(groupRow));
      }
    }
    data.jsonKeyGroupItems = new ArrayList<>();
  }

  private void outputRow(Object[] rowData) throws HopException {
    serializeJson(data.jsonKeyGroupItems);
    data.jsonLength = data.jsonSerialized.length();

    Object[] keyRow = getKeyValues(rowData);

    Object[] additionalRowFields = new Object[2];

    additionalRowFields[0] = data.jsonSerialized;

    // Fill accessory fields
    if (!Utils.isEmpty(meta.getJsonSizeFieldName())) {
      additionalRowFields[1] = data.jsonLength;
    }

    Object[] outputRowData = RowDataUtil.addRowData(keyRow, keyRow.length, additionalRowFields);
    incrementLinesOutput();

    putRow(data.outputRowMeta, outputRowData);

    // Data are safe
    data.rowsAreSafe = true;
  }

  private Object[] getKeyValues(Object[] rowData) throws HopException {
    Object[] keyRow = new Object[meta.getKeyFields().size()];
    for (int i = 0; i < meta.getKeyFields().size(); i++) {
      JsonEOutputKeyField keyField = meta.getKeyFields().get(i);
      try {
        IValueMeta vmi = data.inputRowMeta.getValueMeta(data.keysGroupIndexes[i]);
        keyRow[i] =
            switch (vmi.getType()) {
              case IValueMeta.TYPE_BOOLEAN ->
                  data.inputRowMeta.getBoolean(rowData, data.keysGroupIndexes[i]);
              case IValueMeta.TYPE_INTEGER ->
                  data.inputRowMeta.getInteger(rowData, data.keysGroupIndexes[i]);
              case IValueMeta.TYPE_NUMBER ->
                  data.inputRowMeta.getNumber(rowData, data.keysGroupIndexes[i]);
              case IValueMeta.TYPE_BIGNUMBER ->
                  data.inputRowMeta.getBigNumber(rowData, data.keysGroupIndexes[i]);
              default -> data.inputRowMeta.getString(rowData, data.keysGroupIndexes[i]);
            };
      } catch (HopValueException e) {
        throw new HopException(
            "Error getting json values for key field: " + keyField.getFieldName(), e);
      }
    }
    return keyRow;
  }

  /**
   * The file item of a group: the key fields plus the group's items under the output value name.
   * The JSON block name only wraps the file, not every group.
   */
  private ObjectNode buildGroupFileItem(Object[] groupRow) throws HopException {
    ObjectNode groupItem = new ObjectNode(nc);
    Object[] keyRow = getKeyValues(groupRow);
    for (int i = 0; i < keyRow.length; i++) {
      String name = getKeyJsonAttributeName(meta.getKeyFields().get(i));
      switch (keyRow[i]) {
        case null -> groupItem.putNull(name);
        case Boolean b -> groupItem.put(name, b);
        case Long l -> groupItem.put(name, l);
        case Double d -> groupItem.put(name, d);
        case BigDecimal bd -> groupItem.put(name, bd);
        default -> groupItem.put(name, keyRow[i].toString());
      }
    }
    groupItem.set(meta.getOutputValue(), buildGroupValue(data.jsonKeyGroupItems));
    return groupItem;
  }

  /** The items of a group: an array, or the single item unless arrays are forced. */
  private JsonNode buildGroupValue(List<ObjectNode> items) {
    if (items.size() > 1 || meta.isUseArrayWithSingleInstance()) {
      return new ArrayNode(nc).addAll(items);
    }
    return items.get(0);
  }

  /**
   * Write an item to the file straight away, so the file never has to fit in memory. The first item
   * is held back: a file with a single item holds that item, not an array, unless arrays are
   * forced.
   */
  private void writeFileItem(JsonNode item) throws HopException {
    try {
      if (data.fileItemCount == 0) {
        data.pendingFileItem = item;
      } else {
        if (data.fileItemCount == 1) {
          startFileDocument(true);
          data.fileGenerator.writeTree(data.pendingFileItem);
          data.pendingFileItem = null;
        }
        data.fileGenerator.writeTree(item);
      }
    } catch (IOException e) {
      throw new HopTransformException(BaseMessages.getString(PKG, "JsonOutput.Error.Writing"), e);
    }
    data.fileItemCount++;
    if (!data.isOutputValue) {
      incrementLinesOutput();
    }

    int splitOutputAfter = meta.getFileSettings().getSplitOutputAfter();
    if (splitOutputAfter > 0 && data.fileItemCount >= splitOutputAfter) {
      finishFile();
    }
  }

  private void startFileDocument(boolean array) throws IOException, HopTransformException {
    if (!openNewFile()) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "JsonOutput.Error.OpenNewFile", buildFilename()));
    }
    data.fileGenerator = fileMapper.getFactory().createGenerator(data.writer);
    // The file is closed separately, with its lineage
    data.fileGenerator.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    if (meta.isJsonPrettified()) {
      data.fileGenerator.setPrettyPrinter(new DefaultPrettyPrinter());
    }
    if (!Utils.isEmpty(meta.getJsonBloc())) {
      data.fileGenerator.writeStartObject();
      data.fileGenerator.writeFieldName(meta.getJsonBloc());
    }
    if (array) {
      data.fileGenerator.writeStartArray();
    }
  }

  /** Close the JSON document and the file, if any item was written to it. */
  private void finishFile() throws HopTransformException {
    if (data.fileItemCount == 0) {
      return;
    }
    try {
      if (data.fileItemCount == 1) {
        startFileDocument(meta.isUseArrayWithSingleInstance());
        data.fileGenerator.writeTree(data.pendingFileItem);
      }
      if (data.fileItemCount > 1 || meta.isUseArrayWithSingleInstance()) {
        data.fileGenerator.writeEndArray();
      }
      if (!Utils.isEmpty(meta.getJsonBloc())) {
        data.fileGenerator.writeEndObject();
      }
      data.fileGenerator.close();
    } catch (IOException e) {
      throw new HopTransformException(BaseMessages.getString(PKG, "JsonOutput.Error.Writing"), e);
    }
    data.fileGenerator = null;
    data.pendingFileItem = null;
    data.fileItemCount = 0;
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
    // Items are written one by one: leave flushing to the buffers
    fileMapper = HopJson.newMapper().disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);

    first = false;
    data.inputRowMeta = getInputRowMeta();
    data.inputRowMetaSize = data.inputRowMeta.size();
    data.keysGroupIndexes = meta.resolveKeyFieldIndexes(data.inputRowMeta);

    // Init previous row copy to this first row
    prevRow = data.inputRowMeta.cloneRow(r); // copy the row to previous

    // Create new structure for output fields
    data.outputRowMeta = new RowMeta();
    for (int i = 0; i < meta.getKeyFields().size(); i++) {
      data.outputRowMeta.addValueMeta(
          data.inputRowMeta.getValueMeta(data.keysGroupIndexes[i]).clone());
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

  @Override
  public void dispose() {

    if (data.jsonKeyGroupItems != null) {
      data.jsonKeyGroupItems = null;
    }

    closeFile();
    super.dispose();
  }

  private void createParentFolder(String filename) throws HopTransformException {
    if (!meta.getFileSettings().isCreateParentFolder()) {
      return;
    }
    // Check for parent folder
    FileObject parentfolder = null;
    try {
      // Get parent folder
      parentfolder = HopVfs.getFileObject(filename, variables).getParent();
      if (parentfolder == null) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "JsonEOutput.Error.ErrorCreatingParentFolder", filename));
      }
      if (!parentfolder.exists()) {
        String parentUri = HopVfs.getFriendlyURI(parentfolder);
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(PKG, "JsonEOutput.Error.ParentFolderNotExist", parentUri));
        }
        try {
          parentfolder.createFolder();
        } catch (Exception createEx) {
          // Another concurrent writer (e.g. Beam parallel outputs) may have created it
          parentfolder = HopVfs.getFileObject(filename, variables).getParent();
          if (parentfolder == null || !parentfolder.exists()) {
            throw createEx;
          }
        }
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "JsonEOutput.Log.ParentFolderCreated"));
        }
      }
    } catch (Exception e) {
      String parentDesc =
          parentfolder != null ? HopVfs.getFriendlyURI(parentfolder) : String.valueOf(filename);
      throw new HopTransformException(
          BaseMessages.getString(PKG, "JsonEOutput.Error.ErrorCreatingParentFolder", parentDesc),
          e);
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
