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
package org.apache.hop.pipeline.transforms.sftpput;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Uploads files, or the content of a field, to an SFTP server. Where that server is and how to
 * authenticate with it lives in an {@code SFTP connection} metadata object, so the same connection
 * can be used by this transform and by every other transform which accepts a file name.
 */
@Getter
@Setter
@Transform(
    id = "SFTPPut",
    image = "SFTPPut.svg",
    name = "i18n::SftpPut.Name",
    description = "i18n::SftpPut.Description",
    categoryDescription =
        "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Experimental",
    keywords = "i18n::SftpPutMeta.keyword",
    documentationUrl = "/pipeline/transforms/sftpput.html",
    classLoaderGroup = "sftp")
public class SftpPutMeta extends BaseTransformMeta<SftpPut, SftpPutData> {

  private static final Class<?> PKG = SftpPutMeta.class;

  /** The name of the SFTP connection in the metadata. */
  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "SftpPut.Injection.Connection",
      hopMetadataPropertyType = HopMetadataPropertyType.VFS_SFTP_CONNECTION)
  private String connection;

  /** The field holding the name of the file to upload, or its content. */
  @HopMetadataProperty(
      key = "sourceFileFieldName",
      injectionKeyDescription = "SftpPut.Injection.SourceFileFieldName")
  private String sourceFileFieldName;

  /** When enabled the source field holds the data to upload instead of a file name. */
  @HopMetadataProperty(
      key = "inputIsStream",
      injectionKeyDescription = "SftpPut.Injection.InputIsStream")
  private boolean inputIsStream;

  /** The field holding the folder on the server to upload to. */
  @HopMetadataProperty(
      key = "remoteDirectoryFieldName",
      injectionKeyDescription = "SftpPut.Injection.RemoteDirectoryFieldName")
  private String remoteDirectoryFieldName;

  /**
   * The field holding the name of the file on the server. Optional when uploading a file: the name
   * of the source file is used then. Mandatory when uploading the content of a field.
   */
  @HopMetadataProperty(
      key = "remoteFilenameFieldName",
      injectionKeyDescription = "SftpPut.Injection.RemoteFilenameFieldName")
  private String remoteFilenameFieldName;

  @HopMetadataProperty(
      key = "createRemoteFolder",
      injectionKeyDescription = "SftpPut.Injection.CreateRemoteFolder")
  private boolean createRemoteFolder;

  /** What to do with the source file once it's uploaded. */
  @HopMetadataProperty(
      key = "aftersftpput",
      storeWithCode = true,
      injectionKeyDescription = "SftpPut.Injection.AfterSftpPut")
  private AfterSftpPut afterSftpPut;

  /** The field holding the folder to move the source file to, with {@link AfterSftpPut#MOVE}. */
  @HopMetadataProperty(
      key = "destinationfolderFieldName",
      injectionKeyDescription = "SftpPut.Injection.DestinationFolderFieldName")
  private String destinationFolderFieldName;

  @HopMetadataProperty(
      key = "createdestinationfolder",
      injectionKeyDescription = "SftpPut.Injection.CreateDestinationFolder")
  private boolean createDestinationFolder;

  @HopMetadataProperty(
      key = "addFilenameToResult",
      injectionKeyDescription = "SftpPut.Injection.AddFilenameToResult")
  private boolean addFilenameToResult;

  public SftpPutMeta() {
    super();
    afterSftpPut = AfterSftpPut.NOTHING;
  }

  /**
   * An empty {@code <aftersftpput/>} element deserializes to null: that's the same as doing
   * nothing.
   */
  public AfterSftpPut getAfterSftpPut() {
    return afterSftpPut == null ? AfterSftpPut.NOTHING : afterSftpPut;
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

    if (prev == null || prev.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "SftpPutMeta.CheckResult.NotReceivingFields"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SftpPutMeta.CheckResult.TransformReceivingData", prev.size() + ""),
              transformMeta));
    }

    if (input.length > 0) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SftpPutMeta.CheckResult.TransformReceivingData2"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "SftpPutMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta));
    }

    if (Utils.isEmpty(connection)) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SftpPutMeta.CheckResult.NoConnection"),
              transformMeta));
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /** What to do with the source file after it was uploaded. */
  @Getter
  public enum AfterSftpPut implements IEnumHasCodeAndDescription {
    NOTHING("nothing", BaseMessages.getString(PKG, "SftpPut.AfterSftpPut.DoNothing.Label")),
    DELETE("delete", BaseMessages.getString(PKG, "SftpPut.AfterSftpPut.Delete.Label")),
    MOVE("move", BaseMessages.getString(PKG, "SftpPut.AfterSftpPut.Move.Label")),
    ;

    private final String code;
    private final String description;

    AfterSftpPut(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(AfterSftpPut.class);
    }

    public static AfterSftpPut lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(AfterSftpPut.class, description, NOTHING);
    }
  }
}
