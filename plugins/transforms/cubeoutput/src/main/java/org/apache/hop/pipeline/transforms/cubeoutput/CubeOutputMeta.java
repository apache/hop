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

package org.apache.hop.pipeline.transforms.cubeoutput;

import java.util.List;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataWrapper;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "CubeOutput",
    image = "cubeoutput.svg",
    name = "i18n::CubeOutput.Name",
    description = "i18n::CubeOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::CubeOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/serialize-to-file.html")
@HopMetadataWrapper(tag = "file")
public class CubeOutputMeta extends BaseTransformMeta<CubeOutput, CubeOutputData> {
  private static final Class<?> PKG = CubeOutputMeta.class;

  @HopMetadataProperty(key = "name")
  private String filename;

  @HopMetadataProperty(key = "filename_create_parent_folders")
  private boolean filenameCreatingParentFolders;

  /** Flag: add the filenames to result filenames */
  @HopMetadataProperty(key = "add_to_result_filenames")
  private boolean addToResultFilenames;

  /** Flag : Do not open new file when pipeline start */
  @HopMetadataProperty(key = "do_not_open_newfile_init")
  private boolean doNotOpenNewFileInit;

  public CubeOutputMeta() {
    super(); // allocate BaseTransformMeta
  }

  public CubeOutputMeta(CubeOutputMeta m) {
    this.filename = m.filename;
    this.filenameCreatingParentFolders = m.filenameCreatingParentFolders;
    this.addToResultFilenames = m.addToResultFilenames;
    this.doNotOpenNewFileInit = m.doNotOpenNewFileInit;
  }

  @Override
  public CubeOutputMeta clone() {
    return new CubeOutputMeta(this);
  }

  @Override
  public void setDefault() {
    addToResultFilenames = false;
    doNotOpenNewFileInit = false;
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
    if (!Utils.isEmpty(prev)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "CubeOutputMeta.CheckResult.ReceivingFields", String.valueOf(prev.size())),
              transformMeta);
      remarks.add(cr);
    }

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "CubeOutputMeta.CheckResult.FileSpecificationsNotChecked"),
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
      // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.data
      // To : /home/matt/test/files/foo/bar.data
      //
      FileObject fileObject = HopVfs.getFileObject(variables.resolve(filename));

      // If the file doesn't exist, forget about this effort too!
      //
      if (fileObject.exists()) {
        // Convert to an absolute path...
        //
        filename = iResourceNaming.nameResource(fileObject, variables, true);

        return filename;
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * Sets filename
   *
   * @param filename value of filename
   */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /** Gets filename creating parent folders */
  public boolean isFilenameCreatingParentFolders() {
    return filenameCreatingParentFolders;
  }

  /**
   * @param filenameCreatingParentFolders The filenameCreatingParentFolders to set
   */
  public void setFilenameCreatingParentFolders(boolean filenameCreatingParentFolders) {
    this.filenameCreatingParentFolders = filenameCreatingParentFolders;
  }

  /**
   * Gets addToResultFilenames
   *
   * @return value of addToResultFilenames
   */
  public boolean isAddToResultFilenames() {
    return addToResultFilenames;
  }

  /**
   * Sets addToResultFilenames
   *
   * @param addToResultFilenames value of addToResultFilenames
   */
  public void setAddToResultFilenames(boolean addToResultFilenames) {
    this.addToResultFilenames = addToResultFilenames;
  }

  /**
   * Gets doNotOpenNewFileInit
   *
   * @return value of doNotOpenNewFileInit
   */
  public boolean isDoNotOpenNewFileInit() {
    return doNotOpenNewFileInit;
  }

  /**
   * Sets doNotOpenNewFileInit
   *
   * @param doNotOpenNewFileInit value of doNotOpenNewFileInit
   */
  public void setDoNotOpenNewFileInit(boolean doNotOpenNewFileInit) {
    this.doNotOpenNewFileInit = doNotOpenNewFileInit;
  }
}
