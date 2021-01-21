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

package org.apache.hop.pipeline.transforms.propertyoutput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Transform(
    id = "PropertyOutput",
    image = "propertyoutput.svg",
    name = "i18n::BaseTransform.TypeTooltipDesc.PropertyOutput",
    description = "i18n::BaseTransform.TypeLongDesc.PropertyOutput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/propertyoutput.html")
public class PropertyOutputMeta extends BaseTransformMeta
    implements ITransformMeta<PropertyOutput, PropertyOutputData> {
  private static final Class<?> PKG = PropertyOutputMeta.class; // For Translator

  private String keyfield;
  private String valuefield;

  private boolean addToResult;

  /** The base name of the output file */
  private String fileName;

  /* Specification if file name is in field */

  private boolean fileNameInField;

  private String fileNameField;

  /** The file extention in case of a generated filename */
  private String extension;

  /** Flag: add the transformnr in the filename */
  private boolean transformNrInFilename;

  /** Flag: add the partition number in the filename */
  private boolean partNrInFilename;

  /** Flag: add the date in the filename */
  private boolean dateInFilename;

  /** Flag: add the time in the filename */
  private boolean timeInFilename;

  /** Flag: create parent folder if needed */
  private boolean createparentfolder;

  /** Comment to add in file */
  private String comment;

  /** Flag append in file */
  private boolean append;

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public Object clone() {

    PropertyOutputMeta retval = (PropertyOutputMeta) super.clone();
    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      PropertyOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new PropertyOutput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  /** @return Returns the extension. */
  public String getExtension() {
    return extension;
  }

  /** @param extension The extension to set. */
  public void setExtension(String extension) {
    this.extension = extension;
  }

  /** @return Returns the fileName. */
  public String getFileName() {
    return fileName;
  }

  /** @return Is the file name coded in a field? */
  public boolean isFileNameInField() {
    return fileNameInField;
  }

  /** @param fileNameInField Is the file name coded in a field? */
  public void setFileNameInField(boolean fileNameInField) {
    this.fileNameInField = fileNameInField;
  }

  /** @return The field name that contains the output file name. */
  public String getFileNameField() {
    return fileNameField;
  }

  /** @param fileNameField Name of the field that contains the file name */
  public void setFileNameField(String fileNameField) {
    this.fileNameField = fileNameField;
  }

  /** @return Returns the transformNrInFilename. */
  public boolean isTransformNrInFilename() {
    return transformNrInFilename;
  }

  /** @param transformNrInFilename The transformNrInFilename to set. */
  public void setTransformNrInFilename(boolean transformNrInFilename) {
    this.transformNrInFilename = transformNrInFilename;
  }

  /** @return Returns the timeInFilename. */
  public boolean isTimeInFilename() {
    return timeInFilename;
  }

  /** @return Returns the dateInFilename. */
  public boolean isDateInFilename() {
    return dateInFilename;
  }

  /** @param dateInFilename The dateInFilename to set. */
  public void setDateInFilename(boolean dateInFilename) {
    this.dateInFilename = dateInFilename;
  }

  /** @param timeInFilename The timeInFilename to set. */
  public void setTimeInFilename(boolean timeInFilename) {
    this.timeInFilename = timeInFilename;
  }

  /** @param fileName The fileName to set. */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  /**
   * @return Returns the Add to result filesname flag.
   * @deprecated use {@link #isAddToResult()}
   */
  @Deprecated
  public boolean addToResult() {
    return isAddToResult();
  }

  public boolean isAddToResult() {
    return addToResult;
  }

  /** @param addToResult The Add file to result to set. */
  public void setAddToResult(boolean addToResult) {
    this.addToResult = addToResult;
  }

  /** @return Returns the create parent folder flag. */
  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  /** @param createparentfolder The create parent folder flag to set. */
  public void setCreateParentFolder(boolean createparentfolder) {
    this.createparentfolder = createparentfolder;
  }

  /** @return Returns the append flag. */
  public boolean isAppend() {
    return append;
  }

  /** @param append The append to set. */
  public void setAppend(boolean append) {
    this.append = append;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String commentin) {
    this.comment = commentin;
  }

  public String[] getFiles(IVariables variables) {
    int copies = 1;
    int parts = 1;

    if (transformNrInFilename) {
      copies = 3;
    }

    if (partNrInFilename) {
      parts = 3;
    }

    int nr = copies * parts;
    if (nr > 1) {
      nr++;
    }

    String[] retval = new String[nr];

    int i = 0;
    for (int copy = 0; copy < copies; copy++) {
      for (int part = 0; part < parts; part++) {

        retval[i] = buildFilename(variables, copy);
        i++;
      }
    }
    if (i < nr) {
      retval[i] = "...";
    }

    return retval;
  }

  public String buildFilename(IVariables variables, int transformnr) {

    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String retval = variables.resolve(fileName);

    Date now = new Date();

    if (dateInFilename) {
      daf.applyPattern("yyyMMdd");
      String d = daf.format(now);
      retval += "_" + d;
    }
    if (timeInFilename) {
      daf.applyPattern("HHmmss");
      String t = daf.format(now);
      retval += "_" + t;
    }
    if (transformNrInFilename) {
      retval += "_" + transformnr;
    }

    if (extension != null && extension.length() != 0) {
      retval += "." + extension;
    }

    return retval;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {

      keyfield = XmlHandler.getTagValue(transformNode, "keyfield");
      valuefield = XmlHandler.getTagValue(transformNode, "valuefield");
      comment = XmlHandler.getTagValue(transformNode, "comment");

      fileName = XmlHandler.getTagValue(transformNode, "file", "name");

      createparentfolder =
          "Y"
              .equalsIgnoreCase(
                  XmlHandler.getTagValue(transformNode, "file", "create_parent_folder"));
      extension = XmlHandler.getTagValue(transformNode, "file", "extention");
      transformNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "split"));
      partNrInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "haspartno"));
      dateInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_date"));
      timeInFilename =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "add_time"));
      addToResult =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "AddToResult"));
      append = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "file", "append"));
      fileName = XmlHandler.getTagValue(transformNode, "file", "name");
      fileNameInField =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "fileNameInField"));
      fileNameField = XmlHandler.getTagValue(transformNode, "fileNameField");

    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  @Override
  public void setDefault() {
    append = false;
    createparentfolder = false;
    // Items ...
    keyfield = null;
    valuefield = null;
    comment = null;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    // Items ...

    retval.append("    " + XmlHandler.addTagValue("keyfield", keyfield));
    retval.append("    " + XmlHandler.addTagValue("valuefield", valuefield));
    retval.append("    " + XmlHandler.addTagValue("comment", comment));

    retval.append("    " + XmlHandler.addTagValue("fileNameInField", fileNameInField));
    retval.append("    " + XmlHandler.addTagValue("fileNameField", fileNameField));
    retval.append("    <file>" + Const.CR);

    retval.append("      " + XmlHandler.addTagValue("name", fileName));
    retval.append("      " + XmlHandler.addTagValue("extention", extension));
    retval.append("      " + XmlHandler.addTagValue("split", transformNrInFilename));
    retval.append("      " + XmlHandler.addTagValue("haspartno", partNrInFilename));
    retval.append("      " + XmlHandler.addTagValue("add_date", dateInFilename));
    retval.append("      " + XmlHandler.addTagValue("add_time", timeInFilename));

    retval.append("      " + XmlHandler.addTagValue("create_parent_folder", createparentfolder));
    retval.append("    " + XmlHandler.addTagValue("addtoresult", addToResult));
    retval.append("    " + XmlHandler.addTagValue("append", append));
    retval.append("      </file>" + Const.CR);

    return retval.toString();
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
    // Now see what we can find as previous transform...
    if (prev != null && prev.size() > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "PropertyOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.NoFields"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    // Check if filename is given
    if (!Utils.isEmpty(fileName)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.FilenameOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.FilenameError"),
              transformMeta);
      remarks.add(cr);
    }

    // Check for Key field

    IValueMeta v = prev.searchValueMeta(keyfield);
    if (v == null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.KeyFieldMissing"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.KeyFieldOk"),
              transformMeta);
      remarks.add(cr);
    }

    // Check for Value field

    v = prev.searchValueMeta(valuefield);
    if (v == null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.ValueFieldMissing"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.ValueFieldOk"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public PropertyOutputData getTransformData() {
    return new PropertyOutputData();
  }

  /** @return the keyfield */
  public String getKeyField() {
    return keyfield;
  }

  /** @return the valuefield */
  public String getValueField() {
    return valuefield;
  }

  /** @param KeyField the keyfield to set */
  public void setKeyField(String KeyField) {
    this.keyfield = KeyField;
  }

  /** @param valuefield the valuefield to set */
  public void setValueField(String valuefield) {
    this.valuefield = valuefield;
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files
   * relatively. So what this does is turn the name of files into absolute paths OR it simply
   * includes the resource in the ZIP file. For now, we'll simply turn it into an absolute path and
   * pray that the file is on a shared drive or something like that.
   *
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
      // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.data
      // To : /home/matt/test/files/foo/bar.data
      //
      // In case the name of the file comes from previous transforms, forget about this!
      if (!fileNameInField) {
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(fileName));

        // If the file doesn't exist, forget about this effort too!
        //
        if (fileObject.exists()) {
          // Convert to an absolute path...
          //
          fileName = iResourceNaming.nameResource(fileObject, variables, true);
          return fileName;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
