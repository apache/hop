/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hop.pipeline.transforms.excelwriter;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class ExcelWriterFileField {

    private static final Class<?> PKG = ExcelWriterFileField.class; // For Translator

    /** The base name of the output file */
    @HopMetadataProperty(key = "name",
            injectionKeyDescription = "ExcelWriterMeta.Injection.FileName.Field")
    private String fileName;

    /** The file extension in case of a generated filename */
    @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.Extension.Field")
    private String extension;

    /** The password to protect the sheet */
    @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.Password.Field")
    private String password;

    @HopMetadataProperty(key = "protected_by",
            injectionKeyDescription = "ExcelWriterMeta.Injection.ProtectedBy.Field")
    private String protectedBy;

    /** Flag: protect the sheet */
    @HopMetadataProperty(key = "protect_sheet",
            injectionKeyDescription = "ExcelWriterMeta.Injection.ProtectSheet.Field")
    private boolean protectsheet;

    /** Flag: add the time in the filename */
    @HopMetadataProperty(key = "add_time",
            injectionKeyDescription = "ExcelWriterMeta.Injection.TimeInFilename.Field")
    private boolean timeInFilename;

    /** the excel sheet name */
    @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.SheetName.Field")
    private String sheetname;

    /** Flag : Do not open new file when pipeline start */
    @HopMetadataProperty(key = "do_not_open_newfile_init",
            injectionKeyDescription = "ExcelWriterMeta.Injection.DoNotOpenNewFileInit.Field")
    private boolean doNotOpenNewFileInit;

    @HopMetadataProperty(key = "SpecifyFormat", injectionKeyDescription = "ExcelWriterMeta.Injection.SpecifyFormat.Field")
    private boolean specifyFormat;

    @HopMetadataProperty(key = "date_time_format",
            injectionKeyDescription = "ExcelWriterMeta.Injection.DateTimeFormat.Field")
    private String dateTimeFormat;

    /** Flag : auto size columns? */
    @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.AutoSizeColums.Field")
    private boolean autosizecolums;

    /** Do we need to stream data to handle very large files? */
    @HopMetadataProperty(key = "stream_data",
            injectionKeyDescription = "ExcelWriterMeta.Injection.StreamingData.Field")
    private boolean streamingData;

    /**
     * if this value is larger then 0, the text file is split up into parts of this number of lines
     */
    @HopMetadataProperty(key = "splitevery",
            injectionKeyDescription = "ExcelWriterMeta.Injection.SplitEvery.Field")
    private int splitEvery;

    /** Flag: add the transformnr in the filename */
    @HopMetadataProperty(key = "split",
            injectionKeyDescription = "ExcelWriterMeta.Injection.TransformNrInFilename.Field")
    private boolean transformNrInFilename;

    /** what to do if file exists */
    @HopMetadataProperty(key = "if_file_exists",
            injectionKeyDescription = "ExcelWriterMeta.Injection.IfFileExists.Field")
    private String ifFileExists;

    @HopMetadataProperty(key = "if_sheet_exists",
            injectionKeyDescription = "ExcelWriterMeta.Injection.IfSheetExists.Field")
    private String ifSheetExists;

    /** Flag: add the date in the filename */
    @HopMetadataProperty(key = "add_date",
            injectionKeyDescription = "ExcelWriterMeta.Injection.DateInFilename.Field")
    private boolean dateInFilename;

    public String getIfFileExists() {
        return ifFileExists;
    }

    public void setIfFileExists(String ifFileExists) {
        this.ifFileExists = ifFileExists;
    }

    public String getIfSheetExists() {
        return ifSheetExists;
    }

    public void setIfSheetExists(String ifSheetExists) {
        this.ifSheetExists = ifSheetExists;
    }

    /** @param transformNrInFilename The transformNrInFilename to set. */
    public void setTransformNrInFilename(boolean transformNrInFilename) {
        this.transformNrInFilename = transformNrInFilename;
    }


    /** @return the streamingData */
    public boolean isStreamingData() {
        return streamingData;
    }

    /** @param streamingData the streamingData to set */
    public void setStreamingData(boolean streamingData) {
        this.streamingData = streamingData;
    }

    /** @return Returns the "do not open new file at init" flag. */
    public boolean isDoNotOpenNewFileInit() {
        return doNotOpenNewFileInit;
    }

    /** @param doNotOpenNewFileInit The "do not open new file at init" flag to set. */
    public void setDoNotOpenNewFileInit(boolean doNotOpenNewFileInit) {
        this.doNotOpenNewFileInit = doNotOpenNewFileInit;
    }

    /** @return Returns the splitEvery. */
    public int getSplitEvery() {
        return splitEvery;
    }

    /** @param splitEvery The splitEvery to set. */
    public void setSplitEvery(int splitEvery) {
        this.splitEvery = splitEvery >= 0 ? splitEvery : 0;
    }

    /** @return Returns the transformNrInFilename. */
    public boolean isTransformNrInFilename() {
        return transformNrInFilename;
    }

    /** @return Returns the password. */
    public String getPassword() {
        return password;
    }

    /** @param password teh passwoed to set. */
    public void setPassword(String password) {
        this.password = password;
    }

    /** @return Returns the sheet name. */
    public String getSheetname() {
        return sheetname;
    }

    /** @param sheetname The sheet name. */
    public void setSheetname(String sheetname) {
        this.sheetname = sheetname;
    }

    public String getProtectedBy() {
        return protectedBy;
    }

    public void setProtectedBy(String protectedBy) {
        this.protectedBy = protectedBy;
    }

    /** @return Returns the extension. */
    public String getExtension() {
        return extension;
    }

    /** @param extension The extension to set. */
    public void setExtension(String extension) {
        this.extension = extension;
    }

    /** @return Returns the dateInFilename. */
    public boolean isDateInFilename() {
        return dateInFilename;
    }

    /** @param dateInFilename The dateInFilename to set. */
    public void setDateInFilename(boolean dateInFilename) {
        this.dateInFilename = dateInFilename;
    }

    /** @return Returns the timeInFilename. */
    public boolean isTimeInFilename() {
        return timeInFilename;
    }

    /** @return Returns the protectsheet. */
    public boolean isProtectsheet() {
        return protectsheet;
    }

    /** @param timeInFilename The timeInFilename to set. */
    public void setTimeInFilename(boolean timeInFilename) {
        this.timeInFilename = timeInFilename;
    }

    /** @param protectsheet the value to set. */
    public void setProtectsheet(boolean protectsheet) {
        this.protectsheet = protectsheet;
    }

    /** @return Returns the autosizecolums. */
    public boolean isAutosizecolums() {
        return autosizecolums;
    }

    /** @param autosizecolums The autosizecolums to set. */
    public void setAutosizecolums(boolean autosizecolums) {
        this.autosizecolums = autosizecolums;
    }

    public boolean isSpecifyFormat() {
        return specifyFormat;
    }

    public void setSpecifyFormat(boolean specifyFormat) {
        this.specifyFormat = specifyFormat;
    }

    public String getDateTimeFormat() {
        return dateTimeFormat;
    }

    public void setDateTimeFormat(String dateTimeFormat) {
        this.dateTimeFormat = dateTimeFormat;
    }

    /** @return Returns the fileName. */
    public String getFileName() {
        return fileName;
    }

    /** @param fileName The fileName to set. */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public ExcelWriterFileField() {
        setDefault();
    }

    public ExcelWriterFileField(String fileName, String extension, String sheetname) {
        this.fileName = fileName;
        this.extension = extension;
        this.sheetname = sheetname;

        setDefault();
    }

    public void setDefault() {
        fileName = "file";
        ifFileExists = ExcelWriterTransformMeta.IF_FILE_EXISTS_CREATE_NEW;
        ifSheetExists = ExcelWriterTransformMeta.IF_SHEET_EXISTS_CREATE_NEW;
        autosizecolums = false;
        streamingData = false;
        extension = "xls";
        doNotOpenNewFileInit = false;
        transformNrInFilename = false;
        dateInFilename = false;
        timeInFilename = false;
        dateTimeFormat = null;
        specifyFormat = false;
        protectsheet = false;
        splitEvery = 0;
        sheetname = BaseMessages.getString(PKG, "ExcelWriterMeta.Tab.Sheetname.Text");
    }
}
