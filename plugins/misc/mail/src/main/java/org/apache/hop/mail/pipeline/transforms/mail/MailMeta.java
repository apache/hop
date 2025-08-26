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

package org.apache.hop.mail.pipeline.transforms.mail;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mail.workflow.actions.mail.MailEmbeddedImageField;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
/** Send mail transform. based on Mail action */
@Transform(
    id = "Mail",
    image = "mail.svg",
    name = "i18n::Mail.Name",
    description = "i18n::Mail.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::MailMeta.keyword",
    documentationUrl = "/pipeline/transforms/mail.html")
public class MailMeta extends BaseTransformMeta<Mail, MailData> {
  private static final Class<?> PKG = MailMeta.class;
  private static final String CONST_SPACE = "      ";
  private static final String CONST_SPACE_SHORT = "    ";

  @HopMetadataProperty private String server;

  @HopMetadataProperty private String destination;

  @HopMetadataProperty private String destinationCc;

  @HopMetadataProperty private String destinationBCc;

  /** Caution : this is not the reply to addresses but the mail sender name */
  @HopMetadataProperty(key = "replyto")
  private String replyAddress;

  /** Caution : this is not the reply to addresses but the mail sender */
  @HopMetadataProperty(key = "replytoname")
  private String replyName;

  @HopMetadataProperty private String subject;

  @HopMetadataProperty(key = "include_date")
  private boolean includeDate;

  @HopMetadataProperty(key = "include_subfolders")
  private boolean includeSubFolders;

  @HopMetadataProperty private boolean zipFilenameDynamic;

  @HopMetadataProperty(key = "isFilenameDynamic")
  private boolean filenameDynamic;

  @HopMetadataProperty private String dynamicFieldname;

  @HopMetadataProperty private String dynamicWildcard;

  @HopMetadataProperty private String dynamicZipFilename;

  @HopMetadataProperty private String sourcefilefoldername;

  @HopMetadataProperty private String sourcewildcard;

  @HopMetadataProperty private String connectionName;

  @HopMetadataProperty(key = "contact_person")
  private String contactPerson;

  @HopMetadataProperty(key = "contact_phone")
  private String contactPhone;

  @HopMetadataProperty private String comment;

  @HopMetadataProperty(key = "include_files")
  private boolean includingFiles;

  @HopMetadataProperty(key = "zip_files")
  private boolean zipFiles;

  @HopMetadataProperty(key = "zip_name")
  private String zipFilename;

  @HopMetadataProperty(key = "zip_limit_size")
  private String ziplimitsize;

  @HopMetadataProperty(key = "use_auth")
  private boolean usingAuthentication;

  @HopMetadataProperty(key = "usexoauth2")
  private boolean usexoauth2;

  @HopMetadataProperty(key = "auth_user")
  private String authenticationUser;

  @HopMetadataProperty(key = "auth_password", password = true)
  private String authenticationPassword;

  @HopMetadataProperty(key = "only_comment")
  private boolean onlySendComment;

  @HopMetadataProperty(key = "use_HTML")
  private boolean useHTML;

  @HopMetadataProperty(key = "use_secure_auth")
  private boolean usingSecureAuthentication;

  @HopMetadataProperty(key = "use_Priority")
  private boolean usePriority;

  @HopMetadataProperty private String port;

  @HopMetadataProperty private String priority;

  @HopMetadataProperty private String importance;

  @HopMetadataProperty private String sensitivity;

  @HopMetadataProperty(key = "secureconnectiontype")
  private String secureConnectionType;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty private String encoding;

  /** The reply to addresses */
  @HopMetadataProperty private String replyToAddresses;

  @HopMetadataProperty(key = "embeddedimage", groupKey = "embeddedimages")
  public List<MailEmbeddedImageField> embeddedImages;

  /** Flag : attach file from content defined in a field */
  @HopMetadataProperty private boolean attachContentFromField;

  /** file content field name */
  @HopMetadataProperty private String attachContentField;

  /** filename content field */
  @HopMetadataProperty private String attachContentFileNameField;

  @HopMetadataProperty(key = "include_message_in_output")
  private boolean addMessageToOutput;

  @HopMetadataProperty(key = "message_output_field")
  private String messageOutputField;

  @HopMetadataProperty(key = "check_server_identity")
  private boolean checkServerIdentity;

  @HopMetadataProperty(key = "trusted_hosts")
  private String trustedHosts;

  public MailMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (isAddMessageToOutput()) {
      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(messageOutputField, IValueMeta.TYPE_STRING);
        v.setOrigin(origin);
        row.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(e);
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
    if (prev == null || prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.NotReceivingFields"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "MailMeta.CheckResult.TransformRecevingData", prev.size() + ""),
              transformMeta);
    }
    remarks.add(cr);

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.TransformRecevingData2"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "MailMeta.CheckResult.NoInputReceivedFromOtherTransforms"),
              transformMeta);
    }
    remarks.add(cr);

    // Servername
    if (Utils.isEmpty(server)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.ServerEmpty"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.ServerOk"),
              transformMeta);
      remarks.add(cr);
      // is the field exists?
      if (prev.indexOfValue(variables.resolve(server)) < 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_WARNING,
                BaseMessages.getString(PKG, "MailMeta.CheckResult.ServerFieldNotFound", server),
                transformMeta);
      }
      remarks.add(cr);
    }

    // port number
    if (Utils.isEmpty(port)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.PortEmpty"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.PortOk"),
              transformMeta);
    }
    remarks.add(cr);

    // reply address
    if (Utils.isEmpty(replyAddress)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.ReplayAddressEmpty"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.ReplayAddressOk"),
              transformMeta);
    }
    remarks.add(cr);

    // Destination
    if (Utils.isEmpty(destination)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.DestinationEmpty"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.DestinationOk"),
              transformMeta);
    }
    remarks.add(cr);

    // Subject
    if (Utils.isEmpty(subject)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.SubjectEmpty"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.SubjectOk"),
              transformMeta);
    }
    remarks.add(cr);

    // Comment
    if (Utils.isEmpty(comment)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_WARNING,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.CommentEmpty"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "MailMeta.CheckResult.CommentEmpty"),
              transformMeta);
    }
    remarks.add(cr);

    if (filenameDynamic) {
      // Dynamic Filename field
      if (Utils.isEmpty(dynamicFieldname)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "MailMeta.CheckResult.DynamicFilenameFieldEmpty"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "MailMeta.CheckResult.DynamicFilenameFieldOk"),
                transformMeta);
      }
      remarks.add(cr);

    } else {
      // static filename
      if (Utils.isEmpty(sourcefilefoldername)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "MailMeta.CheckResult.SourceFilenameEmpty"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "MailMeta.CheckResult.SourceFilenameOk"),
                transformMeta);
      }
      remarks.add(cr);
    }

    if (isZipFiles()) {
      if (filenameDynamic) {
        // dynamic zipfilename
        if (Utils.isEmpty(getDynamicZipFilename())) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "MailMeta.CheckResult.DynamicZipfilenameEmpty"),
                  transformMeta);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "MailMeta.CheckResult.DynamicZipfilenameOK"),
                  transformMeta);
        }
        remarks.add(cr);

      } else {
        // static zipfilename
        if (Utils.isEmpty(zipFilename)) {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(PKG, "MailMeta.CheckResult.ZipfilenameEmpty"),
                  transformMeta);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(PKG, "MailMeta.CheckResult.ZipfilenameOk"),
                  transformMeta);
        }
        remarks.add(cr);
      }
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
