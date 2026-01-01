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

package org.apache.hop.workflow.actions.sendnagiospassivecheck;

import com.googlecode.jsendnsca.Level;
import com.googlecode.jsendnsca.MessagePayload;
import com.googlecode.jsendnsca.NagiosPassiveCheckSender;
import com.googlecode.jsendnsca.NagiosSettings;
import com.googlecode.jsendnsca.builders.MessagePayloadBuilder;
import com.googlecode.jsendnsca.builders.NagiosSettingsBuilder;
import com.googlecode.jsendnsca.encryption.Encryption;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/** This defines an SendNagiosPassiveCheck action. */
@Action(
    id = "SEND_NAGIOS_PASSIVE_CHECK",
    name = "i18n::ActionSendNagiosPassiveCheck.Name",
    description = "i18n::ActionSendNagiosPassiveCheck.Description",
    image = "SendNagiosPassiveCheck.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionSendNagiosPassiveCheck.keyword",
    documentationUrl = "/workflow/actions/sendnagiospassivecheck.html")
@Getter
@Setter
public class ActionSendNagiosPassiveCheck extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSendNagiosPassiveCheck.class;
  public static final String CONST_SPACES = "      ";

  @HopMetadataProperty(key = "servername")
  private String serverName;

  @HopMetadataProperty(key = "port")
  private String port;

  @HopMetadataProperty(key = "responseTimeOut")
  private String responseTimeOut;

  @HopMetadataProperty(key = "connectionTimeOut")
  private String connectionTimeOut;

  @HopMetadataProperty(key = "message")
  private String message;

  @HopMetadataProperty(key = "senderServerName")
  private String senderServerName;

  @HopMetadataProperty(key = "senderServiceName")
  private String senderServiceName;

  @HopMetadataProperty(key = "encryptionMode", storeWithCode = true)
  private EncryptionModeEnum encryptionMode;

  @HopMetadataProperty(key = "level")
  private int level;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  /** Default responseTimeOut to 1000 milliseconds */
  private static final int DEFAULT_RESPONSE_TIME_OUT = 10000; // ms

  /** Default connection responseTimeOut to 5000 milliseconds */
  public static final int DEFAULT_CONNECTION_TIME_OUT = 5000; // ms

  /** Default port */
  public static final int DEFAULT_PORT = 5667;

  public static final String[] encryptionModeDesc =
      new String[] {
        BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.EncryptionMode.None"),
        BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.EncryptionMode.TripleDES"),
        BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.EncryptionMode.XOR")
      };

  public static final String[] levelTypeDesc =
      new String[] {
        BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.LevelType.Unknown"),
        BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.EncryptionMode.OK"),
        BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.EncryptionMode.Warning"),
        BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.EncryptionMode.Critical")
      };

  public static final int LEVEL_TYPE_UNKNOWN = 0;
  public static final int LEVEL_TYPE_OK = 1;
  public static final int LEVEL_TYPE_WARNING = 2;
  public static final int LEVEL_TYPE_CRITICAL = 3;

  public ActionSendNagiosPassiveCheck(String n) {
    super(n, "");
    port = "" + DEFAULT_PORT;
    serverName = null;
    connectionTimeOut = String.valueOf(DEFAULT_CONNECTION_TIME_OUT);
    responseTimeOut = String.valueOf(DEFAULT_RESPONSE_TIME_OUT);
    message = null;
    senderServerName = null;
    senderServiceName = null;
    encryptionMode = EncryptionModeEnum.NONE;
    level = LEVEL_TYPE_UNKNOWN;
    password = null;
  }

  public ActionSendNagiosPassiveCheck() {
    this("");
  }

  @Override
  public Object clone() {
    ActionSendNagiosPassiveCheck je = (ActionSendNagiosPassiveCheck) super.clone();
    return je;
  }

  public static String getEncryptionModeDesc(int i) {
    if (i < 0 || i >= encryptionModeDesc.length) {
      return encryptionModeDesc[0];
    }
    return encryptionModeDesc[i];
  }

  public static String getLevelDesc(int i) {
    if (i < 0 || i >= levelTypeDesc.length) {
      return levelTypeDesc[0];
    }
    return levelTypeDesc[i];
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    if (isBasic()) {
      logBasic(BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.Started", serverName));
    }

    Result result = previousResult;
    result.setNrErrors(1);
    result.setResult(false);

    // Target
    String realServername = resolve(serverName);
    String realPassword = Utils.resolvePassword(getVariables(), password);
    int realPort = Const.toInt(resolve(port), DEFAULT_PORT);
    int realResponseTimeOut = Const.toInt(resolve(responseTimeOut), DEFAULT_RESPONSE_TIME_OUT);
    int realConnectionTimeOut =
        Const.toInt(resolve(connectionTimeOut), DEFAULT_CONNECTION_TIME_OUT);

    // Sender
    String realSenderServerName = resolve(senderServerName);
    String realSenderServiceName = resolve(senderServiceName);

    try {
      if (Utils.isEmpty(realServername)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.Error.TargetServerMissing"));
      }

      String realMessageString = resolve(message);

      if (Utils.isEmpty(realMessageString)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.Error.MessageMissing"));
      }

      Level level = Level.UNKNOWN;
      switch (getLevel()) {
        case LEVEL_TYPE_OK:
          level = Level.OK;
          break;
        case LEVEL_TYPE_CRITICAL:
          level = Level.CRITICAL;
          break;
        case LEVEL_TYPE_WARNING:
          level = Level.WARNING;
          break;
        default:
          break;
      }
      Encryption encr = Encryption.NONE;
      switch (getEncryptionMode()) {
        case TRIPLEDES:
          encr = Encryption.TRIPLE_DES;
          break;
        case XOR:
          encr = Encryption.XOR;
          break;
        default:
          break;
      }

      // settings
      NagiosSettingsBuilder ns = new NagiosSettingsBuilder();
      ns.withNagiosHost(realServername);
      ns.withPort(realPort);
      ns.withConnectionTimeout(realConnectionTimeOut);
      ns.withResponseTimeout(realResponseTimeOut);
      ns.withEncryption(encr);
      if (!Utils.isEmpty(realPassword)) {
        ns.withPassword(realPassword);
      } else {
        ns.withNoPassword();
      }

      // target nagios host
      NagiosSettings settings = ns.create();

      // sender
      MessagePayloadBuilder pb = new MessagePayloadBuilder();
      if (!Utils.isEmpty(realSenderServerName)) {
        pb.withHostname(realSenderServerName);
      }
      pb.withLevel(level);
      if (!Utils.isEmpty(realSenderServiceName)) {
        pb.withServiceName(realSenderServiceName);
      }
      pb.withMessage(realMessageString);
      MessagePayload payload = pb.create();

      NagiosPassiveCheckSender sender = new NagiosPassiveCheckSender(settings);

      sender.send(payload);

      result.setNrErrors(0);
      result.setResult(true);

    } catch (Exception e) {
      logError(
          BaseMessages.getString(PKG, "ActionSendNagiosPassiveCheck.ErrorGetting", e.toString()));
    }

    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (!Utils.isEmpty(serverName)) {
      String realServername = resolve(serverName);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realServername, ResourceType.SERVER));
      references.add(reference);
    }
    return references;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "serverName",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}
