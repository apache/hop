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

package org.apache.hop.pipeline.transforms.cubeinput;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "CubeInput",
    image = "cubeinput.svg",
    name = "i18n::CubeInput.Name",
    description = "i18n::CubeInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::CubeInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/serialize-de-from-file.html")
public class CubeInputMeta extends BaseTransformMeta<CubeInput, CubeInputData> {
  private static final Class<?> PKG = CubeInputMeta.class;

  @HopMetadataProperty(key = "file")
  private CubeFile file;

  @HopMetadataProperty(key = "limit")
  private String rowLimit;

  @HopMetadataProperty(key = "addfilenameresult")
  private boolean addFilenameResult;

  public CubeInputMeta() {
    super();
    file = new CubeFile();
  }

  public CubeInputMeta(CubeInputMeta m) {
    this();
    this.file = new CubeFile(m.file);
    this.rowLimit = m.rowLimit;
    this.addFilenameResult = m.addFilenameResult;
  }

  @Override
  public CubeInputMeta clone() {
    return new CubeInputMeta(this);
  }

  @Override
  public void setDefault() {
    this.file = new CubeFile();
    this.rowLimit = "0";
    this.addFilenameResult = false;
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    GZIPInputStream fis = null;
    DataInputStream dis = null;
    try {
      InputStream is =
          HopVfs.getInputStream(
              variables.resolve(file.getName().replace("${Internal.Transform.CopyNr}", "0")));
      fis = new GZIPInputStream(is);
      dis = new DataInputStream(fis);

      IRowMeta add = new RowMeta(dis);
      for (int i = 0; i < add.size(); i++) {
        add.getValueMeta(i).setOrigin(name);
      }
      r.mergeRowMeta(add);
    } catch (HopFileException kfe) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "CubeInputMeta.Exception.UnableToReadMetaData"), kfe);
    } catch (IOException e) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "CubeInputMeta.Exception.ErrorOpeningOrReadingCubeFile"), e);
    } finally {
      try {
        if (fis != null) {
          fis.close();
        }
        if (dis != null) {
          dis.close();
        }
      } catch (IOException ioe) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "CubeInputMeta.Exception.UnableToCloseCubeFile"), ioe);
      }
    }
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

    cr =
        new CheckResult(
            ICheckResult.TYPE_RESULT_COMMENT,
            BaseMessages.getString(PKG, "CubeInputMeta.CheckResult.FileSpecificationsNotChecked"),
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
      FileObject fileObject = HopVfs.getFileObject(variables.resolve(file.getName()));

      // If the file doesn't exist, forget about this effort too!
      //
      if (fileObject.exists()) {
        // Convert to an absolute path...
        //
        file.name = iResourceNaming.nameResource(fileObject, variables, true);
        return file.name;
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  public static class CubeFile {
    @HopMetadataProperty private String name;

    public CubeFile() {}

    public CubeFile(String name) {
      this.name = name;
    }

    public CubeFile(CubeFile f) {
      this.name = f.name;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }
  }

  /**
   * Gets file
   *
   * @return value of file
   */
  public CubeFile getFile() {
    return file;
  }

  /**
   * Sets file
   *
   * @param file value of file
   */
  public void setFile(CubeFile file) {
    this.file = file;
  }

  /**
   * Gets rowLimit
   *
   * @return value of rowLimit
   */
  public String getRowLimit() {
    return rowLimit;
  }

  /**
   * Sets rowLimit
   *
   * @param rowLimit value of rowLimit
   */
  public void setRowLimit(String rowLimit) {
    this.rowLimit = rowLimit;
  }

  /**
   * Gets addFilenameResult
   *
   * @return value of addFilenameResult
   */
  public boolean isAddFilenameResult() {
    return addFilenameResult;
  }

  /**
   * Sets addFilenameResult
   *
   * @param addFilenameResult value of addFilenameResult
   */
  public void setAddFilenameResult(boolean addFilenameResult) {
    this.addFilenameResult = addFilenameResult;
  }
}
