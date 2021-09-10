/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.blockingtransform;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.File;
import java.util.List;

@Transform(
    id = "BlockingTransform",
    image = "blockingtransform.svg",
    name = "i18n::BlockingTransform.Name",
    description = "i18n::BlockingTransform.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/blockingtransform.html")
public class BlockingTransformMeta extends BaseTransformMeta<BlockingTransform, BlockingTransformData> {

  private static final Class<?> PKG = BlockingTransformMeta.class; // For Translator

  /** Directory to store the temp files */
  @HopMetadataProperty private String directory;

  /** Temp files prefix... */
  @HopMetadataProperty private String prefix;

  /** The cache size: number of rows to keep in memory */
  @HopMetadataProperty(key = "cache_size")
  private int cacheSize;

  /**
   * Compress files: if set to true, temporary files are compressed, thus reducing I/O at the cost
   * of slightly higher CPU usage
   */
  @HopMetadataProperty(key = "compress")
  private boolean compressFiles;

  /** Pass all rows, or only the last one. Only the last row was the original behaviour. */
  @HopMetadataProperty(key = "pass_all_rows")
  private boolean passAllRows;

  /** Cache size: how many rows do we keep in memory */
  public static final int CACHE_SIZE = 5000;

  public BlockingTransformMeta() {
    passAllRows = false;
    directory = "${java.io.tmpdir}";
    prefix = "block";
    cacheSize = CACHE_SIZE;
    compressFiles = true;
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

    if (prev != null && prev.size() > 0) {
      // Check the sort directory
      String realDirectory = variables.resolve(directory);

      File f = new File(realDirectory);
      if (f.exists()) {
        if (f.isDirectory()) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "BlockingTransformMeta.CheckResult.DirectoryExists", realDirectory),
                  transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG, "BlockingTransformMeta.CheckResult.ExistsButNoDirectory", realDirectory),
                  transformMeta);
          remarks.add(cr);
        }
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "BlockingTransformMeta.CheckResult.DirectoryNotExists", realDirectory),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "BlockingTransformMeta.CheckResult.NoFields"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "BlockingTransformMeta.CheckResult.TransformExpectingRowsFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "BlockingTransformMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // Default: no values are added to the row in the transform
  }

  /** @return Returns the cacheSize. */
  public int getCacheSize() {
    return cacheSize;
  }

  /** @param cacheSize The cacheSize to set. */
  public void setCacheSize(int cacheSize) {
    this.cacheSize = cacheSize;
  }

  /** @return Returns the prefix. */
  public String getPrefix() {
    return prefix;
  }

  /** @param prefix The prefix to set. */
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  /** @return Returns whether temporary files should be compressed */
  public boolean isCompressFiles() {
    return compressFiles;
  }

  /** @param compressFiles Whether to compress temporary files created during sorting */
  public void setCompressFiles(boolean compressFiles) {
    this.compressFiles = compressFiles;
  }

  /** @return true when all rows are passed and false when only the last one is passed. */
  public boolean isPassAllRows() {
    return passAllRows;
  }

  /**
   * @param passAllRows set to true if all rows should be passed and false if only the last one
   *     should be passed
   */
  public void setPassAllRows(boolean passAllRows) {
    this.passAllRows = passAllRows;
  }

  /** @return The directory to store the temporary files in. */
  public String getDirectory() {
    return directory;
  }

  /** Set the directory to store the temp files in. */
  public void setDirectory(String directory) {
    this.directory = directory;
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal,
    };
  }
}
