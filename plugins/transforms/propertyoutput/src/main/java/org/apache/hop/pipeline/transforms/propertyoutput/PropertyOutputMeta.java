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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

@Transform(
    id = "PropertyOutput",
    image = "propertyoutput.svg",
    name = "i18n::PropertyOutput.Name",
    description = "i18n::PropertyOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::PropertyOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/propertyoutput.html")
@Getter
@Setter
public class PropertyOutputMeta extends BaseTransformMeta<PropertyOutput, PropertyOutputData> {
  private static final Class<?> PKG = PropertyOutputMeta.class;
  public static final String CONST_SPACES_LONG = "      ";
  public static final String CONST_SPACES = "    ";

  @Getter
  @Setter
  public static class FileDetails {
    @HopMetadataProperty(key = "name")
    private String fileName;

    /** The file extension in case of a generated filename */
    @HopMetadataProperty(key = "extension")
    private String extension;

    /** Flag: add the transform copy number in the filename */
    @HopMetadataProperty(key = "split")
    private boolean transformNrInFilename;

    /** Flag: add the partition ID in the filename */
    @HopMetadataProperty(key = "haspartno")
    private boolean partitionIdInFilename;

    /** Flag: add the date in the filename */
    @HopMetadataProperty(key = "add_date")
    private boolean dateInFilename;

    /** Flag: add the time in the filename */
    @HopMetadataProperty(key = "add_time")
    private boolean timeInFilename;

    /** Flag: create parent folder if needed */
    @HopMetadataProperty(key = "create_parent_folder")
    private boolean createParentFolder;

    /** Flag append in file */
    @HopMetadataProperty(key = "append")
    private boolean appending;

    @HopMetadataProperty(key = "addtoresult")
    private boolean addToResult;

    public FileDetails() {}

    public FileDetails(FileDetails f) {
      this();
      this.appending = f.appending;
      this.createParentFolder = f.createParentFolder;
      this.dateInFilename = f.dateInFilename;
      this.extension = f.extension;
      this.fileName = f.fileName;
      this.partitionIdInFilename = f.partitionIdInFilename;
      this.timeInFilename = f.timeInFilename;
      this.transformNrInFilename = f.transformNrInFilename;
      this.addToResult = f.addToResult;
    }
  }

  @HopMetadataProperty(key = "keyfield")
  private String keyField;

  @HopMetadataProperty(key = "valuefield")
  private String valueField;

  /** Comment to add in file */
  @HopMetadataProperty(key = "comment")
  private String comment;

  /** Specification if file name is in field */
  @HopMetadataProperty(key = "fileNameInField")
  private boolean fileNameInField;

  @HopMetadataProperty(key = "fileNameField")
  private String fileNameField;

  @HopMetadataProperty(key = "file")
  private FileDetails fileDetails;

  public PropertyOutputMeta() {
    this.fileDetails = new FileDetails();
  }

  public PropertyOutputMeta(PropertyOutputMeta m) {
    this();
    this.comment = m.comment;
    this.keyField = m.keyField;
    this.valueField = m.valueField;
    this.fileNameInField = m.fileNameInField;
    this.fileNameField = m.fileNameField;
    this.fileDetails = new FileDetails(m.fileDetails);
  }

  @Override
  public Object clone() {
    return new PropertyOutputMeta(this);
  }

  public String[] getFiles(IVariables variables) {
    int copies = 1;
    int parts = 1;

    if (fileDetails.transformNrInFilename) {
      copies = 3;
    }

    if (fileDetails.partitionIdInFilename) {
      parts = 3;
    }

    int nr = copies * parts;
    if (nr > 1) {
      nr++;
    }

    String[] fileNames = new String[nr];

    int i = 0;
    for (int copy = 0; copy < copies; copy++) {
      for (int part = 0; part < parts; part++) {

        fileNames[i] = buildFilename(variables, copy);
        i++;
      }
    }
    if (i < nr) {
      fileNames[i] = "...";
    }

    return fileNames;
  }

  public String buildFilename(IVariables variables, int copyNr) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String retval = variables.resolve(fileDetails.fileName);

    Date now = new Date();

    if (fileDetails.dateInFilename) {
      daf.applyPattern("yyyMMdd");
      String d = daf.format(now);
      retval += "_" + d;
    }
    if (fileDetails.timeInFilename) {
      daf.applyPattern("HHmmss");
      String t = daf.format(now);
      retval += "_" + t;
    }
    if (fileDetails.transformNrInFilename) {
      retval += "_" + copyNr;
    }

    if (!Utils.isEmpty(fileDetails.extension)) {
      retval += "." + fileDetails.extension;
    }

    return retval;
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
    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "PropertyOutputMeta.CheckResult.FieldsReceived", "" + prev.size()),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.NoFields"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.ExpectedInputOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.ExpectedInputError"),
              transformMeta);
      remarks.add(cr);
    }

    // Check if filename is given
    if (!Utils.isEmpty(fileDetails.fileName)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.FilenameOk"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "PropertyOutputMeta.CheckResult.FilenameError"),
              transformMeta);
      remarks.add(cr);
    }

    // Check for Key field

    IValueMeta v = prev.searchValueMeta(keyField);
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

    v = prev.searchValueMeta(valueField);
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
   * @param definitions The definitions to use.
   * @param iResourceNaming The way to rename resources.
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
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(fileDetails.fileName));

        // If the file doesn't exist, forget about this effort too!
        //
        if (fileObject.exists()) {
          // Convert to an absolute path...
          //
          fileDetails.fileName = iResourceNaming.nameResource(fileObject, variables, true);
          return fileDetails.fileName;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * This is used to provide backward compatibility with older XML files.
   *
   * @param node The node containing the transform properties
   */
  @Override
  public void convertLegacyXml(Node node) {
    fileDetails.extension =
        Const.NVL(
            XmlHandler.getTagValue(node, "file", "extention"),
            XmlHandler.getTagValue(node, "file", "extension"));
  }
}
