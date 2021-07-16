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

import org.apache.hop.metadata.api.HopMetadataProperty;

public class ExcelWriterTemplateField {

    /** Flag: use a template */
    @HopMetadataProperty(key = "enabled",
            injectionKeyDescription = "ExcelWriterMeta.Injection.TemplateEnabled.Field")
    private boolean templateEnabled;

    @HopMetadataProperty(key = "sheet_enabled",
            injectionKeyDescription = "ExcelWriterMeta.Injection.TemplateSheetEnabled.Field")
    private boolean templateSheetEnabled;

    @HopMetadataProperty(key = "hidden",
            injectionKeyDescription = "ExcelWriterMeta.Injection.TemplateSheetHidden.Field")
    private boolean templateSheetHidden;

    /** the excel template */
    @HopMetadataProperty(key = "filename",
            injectionKeyDescription = "ExcelWriterMeta.Injection.TemplateFileName.Field")
    private String templateFileName;

    @HopMetadataProperty(key = "sheetname",
            injectionKeyDescription = "ExcelWriterMeta.Injection.TemplateSheetName.Field")
    private String templateSheetName;

    public ExcelWriterTemplateField() {
    }

    public ExcelWriterTemplateField(boolean templateEnabled, boolean templateSheetEnabled, boolean templateSheetHidden, String templateFileName, String templateSheetName) {
        this.templateEnabled = templateEnabled;
        this.templateSheetEnabled = templateSheetEnabled;
        this.templateSheetHidden = templateSheetHidden;
        this.templateFileName = templateFileName;
        this.templateSheetName = templateSheetName;
    }

    /** @return Returns the template. */
    public boolean isTemplateEnabled() {
        return templateEnabled;
    }

    /** @param template The template to set. */
    public void setTemplateEnabled(boolean template) {
        this.templateEnabled = template;
    }

    public boolean isTemplateSheetEnabled() {
        return templateSheetEnabled;
    }

    public void setTemplateSheetEnabled(boolean templateSheetEnabled) {
        this.templateSheetEnabled = templateSheetEnabled;
    }

    public boolean isTemplateSheetHidden() {
        return templateSheetHidden;
    }

    public void setTemplateSheetHidden(boolean hide) {
        this.templateSheetHidden = hide;
    }

    /** @return Returns the templateFileName. */
    public String getTemplateFileName() {
        return templateFileName;
    }

    /** @param templateFileName The templateFileName to set. */
    public void setTemplateFileName(String templateFileName) {
        this.templateFileName = templateFileName;
    }

    public String getTemplateSheetName() {
        return templateSheetName;
    }

    public void setTemplateSheetName(String templateSheetName) {
        this.templateSheetName = templateSheetName;
    }

    public void setDefault() {
        templateEnabled = false;
        templateFileName = "template.xls";
        templateSheetHidden = false;
    }

}
