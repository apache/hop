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

package org.apache.hop.pipeline.transforms.excelwriter;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "TypeExitExcelWriterTransform",
    image = "excelwriter.svg",
    name = "i18n::TypeExitExcelWriterTransform.Name",
    description = "i18n::TypeExitExcelWriterTransform.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::ExcelWriterTransformMeta.keyword",
    documentationUrl = "/pipeline/transforms/excelwriter.html")
public class ExcelWriterTransformMeta
    extends BaseTransformMeta<ExcelWriterTransform, ExcelWriterTransformData> {
  private static final Class<?> PKG = ExcelWriterTransformMeta.class;

  public static final String IF_FILE_EXISTS_REUSE = "reuse";
  public static final String IF_FILE_EXISTS_CREATE_NEW = "new";

  public static final String IF_SHEET_EXISTS_REUSE = "reuse";
  public static final String IF_SHEET_EXISTS_CREATE_NEW = "new";

  public static final String ROW_WRITE_OVERWRITE = "overwrite";
  public static final String ROW_WRITE_PUSH_DOWN = "push";

  @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.MakeSheetActive.Field")
  private boolean makeSheetActive;

  @HopMetadataProperty(
      injectionKeyDescription = "ExcelWriterMeta.Injection.ForceFormulaRecalculation.Field")
  private boolean forceFormulaRecalculation = false;

  @HopMetadataProperty(
      injectionKeyDescription = "ExcelWriterMeta.Injection.LeaveExistingStylesUnchanged.Field")
  private boolean leaveExistingStylesUnchanged = false;

  /** advanced line append options */
  @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.AppendOffset.Field")
  private int appendOffset = 0;

  @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.AppendEmpty.Field")
  private int appendEmpty = 0;

  @HopMetadataProperty(
      injectionKeyDescription = "ExcelWriterMeta.Injection.SchemaDefinition.Field",
      hopMetadataPropertyType = HopMetadataPropertyType.STATIC_SCHEMA_DEFINITION)
  private String schemaDefinition;

  @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.AppendOmitHeader.Field")
  private boolean appendOmitHeader = false;

  /** how to write rows */
  @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.RowWritingMethod.Field")
  private String rowWritingMethod;

  /** where to start writing */
  @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.StartingCell.Field")
  private String startingCell;

  /** Add a header at the top of the file? */
  @HopMetadataProperty(
      key = "header",
      injectionKeyDescription = "ExcelWriterMeta.Injection.HeaderEnabled.Field")
  private boolean headerEnabled;

  /** Add a footer at the bottom of the file? */
  @HopMetadataProperty(
      key = "footer",
      injectionKeyDescription = "ExcelWriterMeta.Injection.FooterEnabled.Field")
  private boolean footerEnabled;

  /** Flag: add the filenames to result filenames */
  @HopMetadataProperty(
      key = "add_to_result_filenames",
      injectionKeyDescription = "ExcelWriterMeta.Injection.AddToResultFilenames.Field")
  private boolean addToResultFilenames;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionKey = "FIELD",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "ExcelWriterMeta.Injection.Fields",
      injectionKeyDescription = "ExcelWriterMeta.Injection.Field")
  private List<ExcelWriterOutputField> outputFields;

  /** Flag : appendLines lines? */
  @HopMetadataProperty(injectionKeyDescription = "ExcelWriterMeta.Injection.AppendLines.Field")
  private boolean appendLines;

  @HopMetadataProperty private ExcelWriterFileField file;

  @HopMetadataProperty private ExcelWriterTemplateField template;

  public ExcelWriterTransformMeta() {
    super();

    file = new ExcelWriterFileField();
    template = new ExcelWriterTemplateField();
    outputFields = new ArrayList<>();
  }

  public String getSchemaDefinition() {
    return schemaDefinition;
  }

  public void setSchemaDefinition(String schemaDefinition) {
    this.schemaDefinition = schemaDefinition;
  }

  public ExcelWriterFileField getFile() {
    return file;
  }

  public void setFile(ExcelWriterFileField file) {
    this.file = file;
  }

  public ExcelWriterTemplateField getTemplate() {
    return template;
  }

  public void setTemplate(ExcelWriterTemplateField template) {
    this.template = template;
  }

  public int getAppendOffset() {
    return appendOffset;
  }

  public void setAppendOffset(int appendOffset) {
    this.appendOffset = appendOffset;
  }

  public int getAppendEmpty() {
    return appendEmpty;
  }

  public void setAppendEmpty(int appendEmpty) {
    this.appendEmpty = appendEmpty >= 0 ? appendEmpty : 0;
  }

  public boolean isAppendOmitHeader() {
    return appendOmitHeader;
  }

  public void setAppendOmitHeader(boolean appendOmitHeader) {
    this.appendOmitHeader = appendOmitHeader;
  }

  public String getStartingCell() {
    return startingCell;
  }

  public void setStartingCell(String startingCell) {
    this.startingCell = startingCell;
  }

  public String getRowWritingMethod() {
    return rowWritingMethod;
  }

  public void setRowWritingMethod(String rowWritingMethod) {
    this.rowWritingMethod = rowWritingMethod;
  }

  /**
   * @return Returns the footer.
   */
  public boolean isFooterEnabled() {
    return footerEnabled;
  }

  /**
   * @param footer The footer to set.
   */
  public void setFooterEnabled(boolean footer) {
    this.footerEnabled = footer;
  }

  /**
   * @return Returns the header.
   */
  public boolean isHeaderEnabled() {
    return headerEnabled;
  }

  /**
   * @param header The header to set.
   */
  public void setHeaderEnabled(boolean header) {
    this.headerEnabled = header;
  }

  /**
   * @return Returns the add to result filesname.
   */
  public boolean isAddToResultFilenames() {
    return addToResultFilenames;
  }

  /**
   * @param addtoresultfilenames The addtoresultfilenames to set.
   */
  public void setAddToResultFilenames(boolean addtoresultfilenames) {
    this.addToResultFilenames = addtoresultfilenames;
  }

  /**
   * @return Returns the outputFields.
   */
  public List<ExcelWriterOutputField> getOutputFields() {
    return outputFields;
  }

  /**
   * @param outputFields The outputFields to set.
   */
  public void setOutputFields(List<ExcelWriterOutputField> outputFields) {
    this.outputFields = outputFields;
  }

  /**
   * @return Returns the appendLines.
   */
  public boolean isAppendLines() {
    return appendLines;
  }

  /**
   * @param append The appendLines to set.
   */
  public void setAppendLines(boolean append) {
    this.appendLines = append;
  }

  public void setMakeSheetActive(boolean makeSheetActive) {
    this.makeSheetActive = makeSheetActive;
  }

  public boolean isMakeSheetActive() {
    return makeSheetActive;
  }

  public boolean isForceFormulaRecalculation() {
    return forceFormulaRecalculation;
  }

  public void setForceFormulaRecalculation(boolean forceFormulaRecalculation) {
    this.forceFormulaRecalculation = forceFormulaRecalculation;
  }

  public boolean isLeaveExistingStylesUnchanged() {
    return leaveExistingStylesUnchanged;
  }

  public void setLeaveExistingStylesUnchanged(boolean leaveExistingStylesUnchanged) {
    this.leaveExistingStylesUnchanged = leaveExistingStylesUnchanged;
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  @Override
  public void setDefault() {

    headerEnabled = true;
    footerEnabled = false;
    addToResultFilenames = true;
    appendLines = false;
    startingCell = "A1";
    rowWritingMethod = ROW_WRITE_OVERWRITE;
    appendEmpty = 0;
    appendOffset = 0;
    appendOmitHeader = false;
    makeSheetActive = true;
    forceFormulaRecalculation = false;
    file.setDefault();
    template.setDefault();
  }

  public String[] getFiles(IVariables variables) {
    int copies = 1;
    int splits = 1;

    if (file.isTransformNrInFilename()) {
      copies = 3;
    }

    if (file.getSplitEvery() != 0) {
      splits = 4;
    }

    int nr = copies * splits;
    if (nr > 1) {
      nr++;
    }

    String[] retval = new String[nr];

    int i = 0;
    for (int copy = 0; copy < copies; copy++) {
      for (int split = 0; split < splits; split++) {
        retval[i] = buildFilename(variables, copy, split, false, "", 1);
        i++;
      }
    }
    if (i < nr) {
      retval[i] = "...";
    }

    return retval;
  }

  public String buildFilename(
      IVariables variables,
      int transformNr,
      int splitNr,
      boolean beamContext,
      String transformId,
      int bundleNr) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String retval = variables.resolve(file.getFileName());
    String realextension = variables.resolve(file.getExtension());

    Date now = new Date();

    if (file.isSpecifyFormat() && !Utils.isEmpty(file.getDateTimeFormat())) {
      daf.applyPattern(file.getDateTimeFormat());
      String dt = daf.format(now);
      retval += dt;
    } else {
      if (file.isDateInFilename()) {
        daf.applyPattern("yyyMMdd");
        String d = daf.format(now);
        retval += "_" + d;
      }
      if (file.isTimeInFilename()) {
        daf.applyPattern("HHmmss");
        String t = daf.format(now);
        retval += "_" + t;
      }
    }
    if (file.isTransformNrInFilename()) {
      retval += "_" + transformNr;
    }
    if (file.getSplitEvery() > 0) {
      retval += "_" + splitNr;
    }
    if (beamContext) {
      retval += "_" + transformId + "_" + bundleNr;
    }

    if (realextension != null && realextension.length() != 0) {
      retval += "." + realextension;
    }

    return retval;
  }

  public String buildFilename(IRowMeta rowMeta, Object[] row, IVariables variables) {
    int filenameFieldIdx = rowMeta.indexOfValue(variables.resolve(getFile().getFileNameField()));
    String retval = variables.resolve((String) row[filenameFieldIdx]);
    String realextension = variables.resolve(file.getExtension());

    if (realextension != null && realextension.length() != 0) {
      retval += "." + realextension;
    }

    return retval;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    // No values are added to the row in this type of transform
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    // Check output fields
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ExcelWriterTransformMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);

      String errorMessage = "";
      boolean errorFound = false;

      // Check fieldname fields is present
      if (getFile().isFileNameInField()) {
        int idx = prev.indexOfValue(getFile().getFileNameField());
        if (idx < 0) {
          errorMessage =
              BaseMessages.getString(
                  PKG, "ExcelWriterTransformMeta.CheckResult.FilenameFieldNotFound", errorMessage);
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        }
      }

      // Starting from selected fields in ...
      for (int i = 0; i < outputFields.size(); i++) {
        ExcelWriterOutputField field = outputFields.get(i);
        int idx = prev.indexOfValue(field.getName());
        if (idx < 0) {
          errorMessage += "\t\t" + field.getName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(
                PKG, "ExcelWriterTransformMeta.CheckResult.FieldsNotFound", errorMessage);
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExcelWriterTransformMeta.CheckResult.AllFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExcelWriterTransformMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "ExcelWriterTransformMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "ExcelWriterTransformMeta.CheckResult.FilesNotChecked"),
            transformMeta);
    remarks.add(cr);
  }

  /**
   * @param variables the variable variables to use
   * @param definitions
   * @param iResourceNaming
   * @param metadataProvider the metadataProvider in which non-hop metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming iResourceNaming,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      //
      if (!Utils.isEmpty(file.getFileName())) {
        FileObject fileObject =
            HopVfs.getFileObject(variables.resolve(file.getFileName()), variables);
        file.setFileName(iResourceNaming.nameResource(fileObject, variables, true));
      }
      if (!Utils.isEmpty(template.getTemplateFileName())) {
        FileObject fileObject =
            HopVfs.getFileObject(variables.resolve(template.getTemplateFileName()), variables);
        template.setTemplateFileName(iResourceNaming.nameResource(fileObject, variables, true));
      }

      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
