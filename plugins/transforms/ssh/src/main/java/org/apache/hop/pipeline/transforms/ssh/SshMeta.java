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

package org.apache.hop.pipeline.transforms.ssh;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "SSH",
    image = "ssh.svg",
    name = "i18n::SSH.Name",
    description = "i18n::SSH.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::SSHMeta.keyword",
    documentationUrl = "/pipeline/transforms/runssh.html")
public class SshMeta extends BaseTransformMeta<Ssh, SshData> {
  private static final Class<?> PKG = SshMeta.class;
  private static final int DEFAULT_PORT = 22;

  @HopMetadataProperty private String command;
  @HopMetadataProperty private boolean dynamicCommandField;

  @HopMetadataProperty(key = "commandfieldname")
  private String commandFieldName;

  @HopMetadataProperty private String serverName;
  @HopMetadataProperty private String port;
  @HopMetadataProperty private String userName;

  @HopMetadataProperty(password = true)
  private String password;

  @HopMetadataProperty private boolean usePrivateKey;
  @HopMetadataProperty private String keyFileName;

  @HopMetadataProperty(password = true)
  private String passPhrase;

  @HopMetadataProperty private String stdOutFieldName;
  @HopMetadataProperty private String stdErrFieldName;
  @HopMetadataProperty private String timeOut;
  @HopMetadataProperty private String proxyHost;
  @HopMetadataProperty private String proxyPort;
  @HopMetadataProperty private String proxyUsername;

  @HopMetadataProperty(password = true)
  private String proxyPassword;

  public SshMeta() {
    dynamicCommandField = false;
    command = null;
    commandFieldName = null;
    port = String.valueOf(DEFAULT_PORT);
    serverName = null;
    userName = null;
    password = null;
    usePrivateKey = true;
    keyFileName = null;
    stdOutFieldName = "stdOut";
    stdErrFieldName = "stdErr";
    timeOut = "0";
    proxyHost = null;
    proxyPort = null;
    proxyUsername = null;
    proxyPassword = null;
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
    String errorMessage = "";

    // Target hostname
    if (Utils.isEmpty(getServerName())) {
      errorMessage = BaseMessages.getString(PKG, "SSHMeta.CheckResult.TargetHostMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "SSHMeta.CheckResult.TargetHostOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (isUsePrivateKey()) {
      String keyfilename = variables.resolve(getKeyFileName());
      if (Utils.isEmpty(keyfilename)) {
        errorMessage = BaseMessages.getString(PKG, "SSHMeta.CheckResult.PrivateKeyFileNameMissing");
        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        errorMessage = BaseMessages.getString(PKG, "SSHMeta.CheckResult.PrivateKeyFileNameOK");
        cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
        remarks.add(cr);
        boolean keyFileExists = false;
        try {
          keyFileExists = HopVfs.fileExists(keyfilename, variables);
        } catch (Exception e) {
          /* Ignore */
        }
        if (!keyFileExists) {
          errorMessage =
              BaseMessages.getString(
                  PKG, "SSHMeta.CheckResult.PrivateKeyFileNotExist", keyfilename);
          cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
          remarks.add(cr);
        } else {
          errorMessage =
              BaseMessages.getString(PKG, "SSHMeta.CheckResult.PrivateKeyFileExists", keyfilename);
          cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
          remarks.add(cr);
        }
      }
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SSHMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SSHMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (!isDynamicCommandField()) {
      row.clear();
    }
    IValueMeta v = new ValueMetaString(variables.resolve(getStdOutFieldName()));
    v.setOrigin(name);
    row.addValueMeta(v);

    String stderrfield = variables.resolve(getStdErrFieldName());
    if (!Utils.isEmpty(stderrfield)) {
      v = new ValueMetaBoolean(stderrfield);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
