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

package org.apache.hop.pipeline.transforms.salesforce;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
public abstract class SalesforceTransformMeta<
        Main extends SalesforceTransform, Data extends SalesforceTransformData>
    extends BaseTransformMeta<Main, Data> {

  private static final Class<?> PKG = SalesforceTransformMeta.class;

  /** The Salesforce Target URL */
  @HopMetadataProperty(
      key = "targetUrl",
      injectionKey = "SALESFORCE_URL",
      injectionKeyDescription = "SalesforceInputMeta.Injection.SALESFORCE_URL")
  private String targetUrl;

  /** The userName */
  @HopMetadataProperty(
      key = "username",
      injectionKey = "SALESFORCE_USERNAME",
      injectionKeyDescription = "SalesforceInputMeta.Injection.SALESFORCE_USERNAME")
  private String username;

  /** The password */
  @HopMetadataProperty(
      key = "password",
      injectionKey = "SALESFORCE_PASSWOR",
      injectionKeyDescription = "SalesforceInputMeta.Injection.SALESFORCE_PASSWOR",
      password = true)
  private String password;

  /** The time out */
  @HopMetadataProperty(
      key = "timeout",
      injectionKey = "TIME_OUT",
      injectionKeyDescription = "SalesforceInputMeta.Injection.TIME_OUT")
  private String timeout;

  /** The connection compression */
  @HopMetadataProperty(
      key = "useCompression",
      injectionKey = "USE_COMPRESSION",
      injectionKeyDescription = "SalesforceInputMeta.Injection.USE_COMPRESSION")
  private boolean compression;

  /** The Salesforce module */
  @HopMetadataProperty(
      key = "module",
      injectionKey = "MODULE",
      injectionKeyDescription = "SalesforceInputMeta.Injection.MODULE")
  private String module;

  /** Salesforce Connection metadata name */
  @HopMetadataProperty(
      key = "salesforce_connection",
      injectionKey = "SALESFORCE_CONNECTION",
      injectionKeyDescription = "SalesforceInputMeta.SALESFORCE_CONNECTION")
  private String salesforceConnection;

  @Override
  public Object clone() {
    SalesforceTransformMeta retval = (SalesforceTransformMeta) super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
    setTargetUrl(SalesforceConnectionUtils.TARGET_DEFAULT_URL);
    setUsername("");
    setPassword("");
    setTimeout("60000");
    setCompression(false);
    setModule("Account");
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

    // check URL
    if (Utils.isEmpty(getTargetUrl())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.NoURL"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.URLOk"),
              transformMeta);
    }
    remarks.add(cr);

    // check user name
    if (Utils.isEmpty(getUsername())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.NoUsername"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.UsernameOk"),
              transformMeta);
    }
    remarks.add(cr);

    // check module
    if (Utils.isEmpty(getModule())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.NoModule"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "SalesforceTransformMeta.CheckResult.ModuleOk"),
              transformMeta);
    }
    remarks.add(cr);
  }
}
