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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class JsonOutput extends BaseTransform<JsonOutputMeta, JsonOutputData> implements ITransform<JsonOutputMeta, JsonOutputData> {
    private static Class<?> PKG = JsonOutput.class; // for i18n purposes, needed by Translator2!!

    private JsonOutputMeta meta;
    private JsonOutputData data;
    public  Object[] prevRow;

    private ObjectNode itemNode;
    private JsonNodeFactory nc;
    private List<ObjectNode> jsonItems;
    private ObjectMapper mapper;
    private int startPagePos;

    public JsonOutput(TransformMeta transformMeta, JsonOutputMeta meta, JsonOutputData data, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
        super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    public void manageRowItems(Object[] row) throws HopException {

        if (data.isGenLoopOverKey() && !sameGroup( prevRow, row ) && jsonItems.size()>0) {
            // Output the new row
            logDebug("Record Num: " + data.nrRow + " - Generating JSON chunk");
            outPutRow(prevRow);
            jsonItems = new ArrayList<>();
        }

        // Create a new object with specified fields
        itemNode = new ObjectNode(nc);

        for (int i = 0; i < data.nrFields; i++) {
            JsonOutputField outputField = meta.getOutputFields()[i];

            IValueMeta v = data.inputRowMeta.getValueMeta(data.fieldIndexes[i]);

            switch (v.getType()) {
                case IValueMeta.TYPE_BOOLEAN:
                    Boolean boolValue = data.inputRowMeta.getBoolean(row, data.fieldIndexes[i]);

                    if (boolValue != null)
                        itemNode.put(outputField.getElementName(), boolValue);
                    else {
                        if (!outputField.isRemoveIfBlank())
                            itemNode.put(outputField.getElementName(), boolValue);
                    }
                    break;

                case IValueMeta.TYPE_INTEGER:
                    Long integerValue = data.inputRowMeta.getInteger(row, data.fieldIndexes[i]);

                    if (integerValue != null)
                        itemNode.put(outputField.getElementName(), integerValue);
                    else
                        if (!outputField.isRemoveIfBlank())
                            itemNode.put(outputField.getElementName(), integerValue);
                    break;
                case IValueMeta.TYPE_NUMBER:
                    Double numberValue = data.inputRowMeta.getNumber(row, data.fieldIndexes[i]);

                    if (numberValue != null)
                        itemNode.put(outputField.getElementName(), numberValue);
                    else
                        if (!outputField.isRemoveIfBlank())
                            itemNode.put(outputField.getElementName(), numberValue);
                    break;
                case IValueMeta.TYPE_BIGNUMBER:
                    BigDecimal bignumberValue = data.inputRowMeta.getBigNumber(row, data.fieldIndexes[i]);

                    if(bignumberValue != null)
                        itemNode.put(outputField.getElementName(), bignumberValue);
                    else
                        if (!outputField.isRemoveIfBlank())
                            itemNode.put(outputField.getElementName(), bignumberValue);
                    break;
                default:
                    String value = data.inputRowMeta.getString(row, data.fieldIndexes[i]);
                    if (value != null) {
                        if (outputField.isJSONFragment()) {
                            try {
                                JsonNode jsonNode = mapper.readTree(value);
                                itemNode.put(outputField.getElementName(), jsonNode);
                            } catch (IOException e) {
                                // TBD Exception must be properly managed
                                e.printStackTrace();
                            }
                        } else {
                            itemNode.put(outputField.getElementName(), value);

                        }
                    } else {
                        if (!outputField.isRemoveIfBlank())
                            itemNode.put(outputField.getElementName(), value);
                    }

                    break;
            }
        }

        jsonItems.add(itemNode);
        prevRow = data.inputRowMeta.cloneRow( row ); // copy the row to previous
        data.nrRow++;

        if (meta.getSplitOutputAfter() > 0 && (data.nrRow) % meta.getSplitOutputAfter() == 0) {
            // Output the new row
            logDebug("Record Num: " + data.nrRow + " - Generating JSON chunk");
            outPutRow(prevRow);
            jsonItems = new ArrayList<>();
        }

    }

    // Is the row r of the same group as previous?
    private boolean sameGroup( Object[] previous, Object[] r ) throws HopValueException {
        return data.inputRowMeta.compare( previous, r, data.keysGroupIndexes ) == 0;
    }

    public boolean processRow() throws HopException {

        Object[] r = getRow(); // This also waits for a row to be finished.
        if (r == null) {
            // no more input to be expected...
            if ( !data.rowsAreSafe ) {
                // Let's output the remaining unsafe data
                outPutRow( r );
            }
            setOutputDone();
            return false;
        }

        if (first) {

            if (onFirstRecord(r)) return false;

        }

        manageRowItems(r);

        if (data.isWriteToFile() && !data.isOutputValue()) {
            putRow(data.inputRowMeta, r); // in case we want it go further...
            incrementLinesOutput();
        }
        return true;
    }

    private boolean onFirstRecord(Object[] r) throws HopException {

        nc = new ObjectMapper().getNodeFactory();
        mapper = new ObjectMapper();
        jsonItems = new ArrayList<>();
        startPagePos = 1;

        first = false;
        data.inputRowMeta = getInputRowMeta();
        data.inputRowMetaSize = data.inputRowMeta.size();

        // Init previous row copy to this first row
        prevRow = data.inputRowMeta.cloneRow( r ); // copy the row to previous

        if (data.isOutputValue()) {
            // Create new structure for output fields
            data.outputRowMeta = new RowMeta();
            JsonOutputKeyField[] keyFields = meta.getKeyFields();
            for (int i=0; i<meta.getKeyFields().length; i++) {
                IValueMeta vmi = data.inputRowMeta.getValueMeta(data.inputRowMeta.indexOfValue(keyFields[i].getFieldName()));
                data.outputRowMeta.addValueMeta(i, vmi);
            }

            // This is JSON block's column
            data.outputRowMeta.addValueMeta(meta.getKeyFields().length, new ValueMetaString(meta.getOutputValue()));
        }


        int fieldLength = meta.getKeyFields().length + 1;
        if (meta.getJsonSizeFieldname() != null && meta.getJsonSizeFieldname().length()>0) {
            data.outputRowMeta.addValueMeta(fieldLength, new ValueMetaInteger(meta.getJsonSizeFieldname()));
            fieldLength++;
        }
        if (meta.getJsonPageStartsAtFieldname() != null && meta.getJsonPageStartsAtFieldname().length()>0) {
            data.outputRowMeta.addValueMeta(fieldLength, new ValueMetaInteger(meta.getJsonPageStartsAtFieldname()));
            fieldLength++;
        }
        if (meta.getJsonPageEndsAtFieldname() != null && meta.getJsonPageEndsAtFieldname().length()>0) {
            data.outputRowMeta.addValueMeta(fieldLength, new ValueMetaInteger(meta.getJsonPageEndsAtFieldname()));
            fieldLength++;
        }

        initDataFieldsPositionsArray();


        if (initKeyFieldsPositionArray(r)) return true;
        return false;
    }

    private void initDataFieldsPositionsArray() throws HopException {
        // Cache the field name indexes
        //
        data.nrFields = meta.getOutputFields().length;
        data.fieldIndexes = new int[data.nrFields];
        for (int i = 0; i < data.nrFields; i++) {
            data.fieldIndexes[i] = data.inputRowMeta.indexOfValue(meta.getOutputFields()[i].getFieldName());
            if (data.fieldIndexes[i] < 0) {
                throw new HopException(BaseMessages.getString(PKG, "JsonOutput.Exception.FieldNotFound"));
            }
            JsonOutputField field = meta.getOutputFields()[i];
            field.setElementName(variables.resolve(field.getElementName()));
        }
    }

    private boolean initKeyFieldsPositionArray(Object[] r) {
        data.keysGroupIndexes = new int[ meta.getKeyFields().length ];

        for ( int i = 0; i < meta.getKeyFields().length; i++ ) {
            data.keysGroupIndexes[ i ] = data.inputRowMeta.indexOfValue( meta.getKeyFields()[ i ].getFieldName() );
            if ( ( r != null ) && ( data.keysGroupIndexes[ i ] < 0 ) ) {
                /* logError( BaseMessages.getString( PKG, "GroupBy.Log.GroupFieldCouldNotFound", meta.getGroupField()[ i ] ) );*/
                setErrors( 1 );
                stopAll();
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private void outPutRow(Object[] rowData) throws HopException {
        // We can now output an object
        String value = null;

        if (jsonItems == null || jsonItems.size()==0)
            return;

        try {
            if (jsonItems != null && jsonItems.size()>0) {
                if (meta.getJsonBloc() != null && meta.getJsonBloc().length()>0) {
                    ObjectNode theNode = new ObjectNode(nc);
                    // TBD Try to understand if this can have a performance impact and do it better...

                    theNode.put(meta.getJsonBloc(), mapper.readTree(mapper.writeValueAsString(jsonItems.size() > 1
                            ? jsonItems : (!meta.isUseArrayWithSingleInstance() ? jsonItems.get(0) : jsonItems))));
                    if (meta.isJsonPrittified())
                        value = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(theNode);
                    else
                        value = mapper.writeValueAsString(theNode);
                } else {
                    if (meta.isJsonPrittified())
                        value = mapper.writerWithDefaultPrettyPrinter().writeValueAsString((jsonItems.size() > 1
                                ? jsonItems : (!meta.isUseArrayWithSingleInstance() ? jsonItems.get(0) : jsonItems)));
                    else
                        value = mapper.writeValueAsString((jsonItems.size() > 1
                            ? jsonItems : (!meta.isUseArrayWithSingleInstance() ? jsonItems.get(0) : jsonItems)));
                }
            }

        } catch (IOException e) {
            // TBD Exception must be properly managed
            e.printStackTrace();
        }

        int jsonLength = value.length();

        if (data.isOutputValue() && data.outputRowMeta != null) {

            Object[] keyRow = new Object[meta.getKeyFields().length];

            for (int i=0; i<meta.getKeyFields().length; i++) {
                try {
                    IValueMeta vmi = data.inputRowMeta.getValueMeta(data.keysGroupIndexes[ i ]);
                    switch (vmi.getType()) {
                        case IValueMeta.TYPE_BOOLEAN:
                            keyRow[i] = data.inputRowMeta.getBoolean(rowData, data.keysGroupIndexes[ i ]);
                            break;
                        case IValueMeta.TYPE_INTEGER:
                            keyRow[i] = data.inputRowMeta.getInteger(rowData, data.keysGroupIndexes[ i ]);
                            break;
                        case IValueMeta.TYPE_NUMBER:
                            keyRow[i] = data.inputRowMeta.getNumber(rowData, data.keysGroupIndexes[ i ]);
                            break;
                        case IValueMeta.TYPE_BIGNUMBER:
                            keyRow[i] = data.inputRowMeta.getBigNumber(rowData, data.keysGroupIndexes[ i ]);
                            break;
                        default:
                            keyRow[i] = data.inputRowMeta.getString(rowData, data.keysGroupIndexes[ i ]);
                            break;
                    }
                } catch (HopValueException e) {
                    // TODO - Properly handle the exception
                    // e.printStackTrace();
                }
            }

            Object[] fieldsQueueArray = new Object[4];

            fieldsQueueArray[0] = value;
            int nextFieldPos = 1;


            if (meta.getJsonSizeFieldname() != null && meta.getJsonSizeFieldname().length()>0) {
                fieldsQueueArray[nextFieldPos] = new Long(jsonLength);
                nextFieldPos++;
            }
            if (meta.getJsonPageStartsAtFieldname() != null && meta.getJsonPageStartsAtFieldname().length()>0) {
                fieldsQueueArray[nextFieldPos] = new Long(startPagePos);
                startPagePos = data.nrRow + 1;
                nextFieldPos++;
            }
            if (meta.getJsonPageEndsAtFieldname() != null && meta.getJsonPageEndsAtFieldname().length()>0) {
                fieldsQueueArray[nextFieldPos] = new Long(data.nrRow);
                nextFieldPos++;
            }

            Object[] outputRowData = RowDataUtil.addRowData(keyRow, keyRow.length, fieldsQueueArray);
            incrementLinesOutput();

            putRow(data.outputRowMeta, outputRowData);
        }

        if (data.isWriteToFile()) {
            // Open a file
            if (!openNewFile()) {
                throw new HopTransformException(BaseMessages.getString(
                        PKG, "JsonOutput.Error.OpenNewFile", buildFilename()));
            }
            // Write data to file
            try {
                data.writer.write(value);
            } catch (Exception e) {
                throw new HopTransformException(BaseMessages.getString(PKG, "JsonOutput.Error.Writing"), e);
            }
            // Close file
            closeFile();
        }

        // Data are safe
        data.rowsAreSafe = true;
    }

    public boolean init() {

        if (super.init()) {
            data.setWriteToFile((meta.getOperationType() == JsonOutputMeta.OPERATION_TYPE_WRITE_TO_FILE));
            data.setOutputValue((meta.getOperationType() == JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE));
            data.setGenFlat((meta.getGenerationType() == JsonOutputMeta.GENERATON_TYPE_FLAT));
            data.setGenLoopOverKey((meta.getGenerationType() == JsonOutputMeta.GENERATON_TYPE_LOOP_OVER_KEY));

            if (data.isOutputValue()) {
                // We need to have output field name
                if (Utils.isEmpty(resolve(meta.getOutputValue()))) {
                    logError(BaseMessages.getString(PKG, "JsonOutput.Error.MissingOutputFieldName"));
                    stopAll();
                    setErrors(1);
                    return false;
                }
            }
            if (data.isWriteToFile()) {
                if (!meta.isDoNotOpenNewFileInit()) {
                    if (!openNewFile()) {
                        logError(BaseMessages.getString(PKG, "JsonOutput.Error.OpenNewFile", buildFilename()));
                        stopAll();
                        setErrors(1);
                        return false;
                    }
                }

            }
            data.realBlocName = Const.NVL( resolve( meta.getJsonBloc() ), "" );
            return true;
        }

        return false;
    }

    public void dispose() {

        if (jsonItems != null) {
            jsonItems = null;
        }

        closeFile();
        super.dispose();

    }

    private void createParentFolder(String filename) throws HopTransformException {
        if (!meta.isCreateParentFolder()) {
            return;
        }
        // Check for parent folder
        FileObject parentfolder = null;
        try {
            // Get parent folder
            parentfolder = HopVfs.getFileObject(filename).getParent();
            if (!parentfolder.exists()) {
                if (log.isDebug()) {
                    logDebug(BaseMessages.getString(PKG, "JsonOutput.Error.ParentFolderNotExist", parentfolder.getName()));
                }
                parentfolder.createFolder();
                if (log.isDebug()) {
                    logDebug(BaseMessages.getString(PKG, "JsonOutput.Log.ParentFolderCreated"));
                }
            }
        } catch (Exception e) {
            throw new HopTransformException(BaseMessages.getString(
                    PKG, "JsonOutput.Error.ErrorCreatingParentFolder", parentfolder.getName()));
        } finally {
            if (parentfolder != null) {
                try {
                    parentfolder.close();
                } catch (Exception ex) { /* Ignore */
                }
            }
        }
    }

    public boolean openNewFile() {
        if (data.writer != null) {
            return true;
        }
        boolean retval = false;
        try {


            String filename = buildFilename();
            createParentFolder( filename );
            if ( meta.AddToResult() ) {
                // Add this to the result file names...
                ResultFile resultFile =
                        new ResultFile(
                                ResultFile.FILE_TYPE_GENERAL, HopVfs.getFileObject( filename ),
                                getPipelineMeta().getName(), getTransformName() );
                resultFile.setComment( BaseMessages.getString( PKG, "JsonOutput.ResultFilenames.Comment" ) );
                addResultFile( resultFile );
            }

            OutputStream outputStream;
            OutputStream fos = HopVfs.getOutputStream( filename, meta.isFileAppended() );
            outputStream = fos;

            if ( !Utils.isEmpty( meta.getEncoding() ) ) {
                data.writer =
                        new OutputStreamWriter( new BufferedOutputStream( outputStream, 5000 ), resolve( meta
                                .getEncoding() ) );
            } else {
                data.writer = new OutputStreamWriter( new BufferedOutputStream( outputStream, 5000 ) );
            }

            if ( log.isDetailed() ) {
                logDetailed( BaseMessages.getString( PKG, "JsonOutput.FileOpened", filename ) );
            }

            data.splitnr++;


            retval = true;

        } catch ( Exception e ) {
            logError( BaseMessages.getString( PKG, "JsonOutput.Error.OpeningFile", e.toString() ) );
        }

        return retval;
    }

    public String buildFilename() {
        return meta.buildFilename(resolve(meta.getFileName()), getCopy(), data.splitnr);
    }

    private boolean closeFile() {
        if (data.writer == null) {
            return true;
        }
        boolean retval = false;

        try {
            data.writer.close();
            data.writer = null;
            retval = true;
        } catch (Exception e) {
            logError(BaseMessages.getString(PKG, "JsonOutput.Error.ClosingFile", e.toString()));
            setErrors(1);
            retval = false;
        }

        return retval;
    }
}
