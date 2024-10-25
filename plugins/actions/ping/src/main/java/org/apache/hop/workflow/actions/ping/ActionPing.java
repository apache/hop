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

package org.apache.hop.workflow.actions.ping;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
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

/** This defines a ping action. */
@Action(
    id = "PING",
    name = "i18n::ActionPing.Name",
    description = "i18n::ActionPing.Description",
    image = "Ping.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionPing.keyword",
    documentationUrl = "/workflow/actions/ping.html")
@SuppressWarnings("java:S1104")
@Getter
@Setter
public class ActionPing extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionPing.class;
  public static final String CONST_ACTION_PING_OK_LABEL = "ActionPing.OK.Label";
  public static final String CONST_ACTION_PING_NOK_LABEL = "ActionPing.NOK.Label";
  public static final String CONST_HOSTNAME = "hostname";

  @HopMetadataProperty(key = "hostname")
  private String hostname;

  @HopMetadataProperty(key = "timeout")
  private String timeout;

  public String defaultTimeout = "3000";

  @HopMetadataProperty(key = "nbrPackets")
  private String nbrPackets;

  // Here for backwards compatibility with old writing method
  @HopMetadataProperty(key = "nbr_packets")
  private String oldNbrPackets;

  private static final String WINDOWS_CHAR = "-n";
  private static final String NIX_CHAR = "-c";
  public String classicPing = "classicPing";
  public int iclassicPing = 0;
  public String systemPing = "systemPing";
  public int isystemPing = 1;
  public String bothPings = "bothPings";
  public int ibothPings = 2;

  @HopMetadataProperty(key = "pingtype")
  public String pingtype;

  public int ipingtype;

  public ActionPing(String n) {
    super(n, "");
    pingtype = classicPing;
    hostname = null;
    nbrPackets = "2";
    timeout = defaultTimeout;
  }

  public ActionPing() {
    this("");
  }

  @Override
  public Object clone() {
    ActionPing je = (ActionPing) super.clone();
    return je;
  }

  public String getRealNbrPackets() {
    return resolve(getNbrPackets());
  }

  public String getRealHostname() {
    return resolve(getHostname());
  }

  public String getRealTimeout() {
    return resolve(getTimeout());
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;

    result.setNrErrors(1);
    result.setResult(false);

    String hostname = getRealHostname();
    int timeoutInt = Const.toInt(getRealTimeout(), 300);
    int packets = Const.toInt(getRealNbrPackets(), 2);
    boolean status = false;

    if (Utils.isEmpty(hostname)) {
      // No Host was specified
      logError(BaseMessages.getString(PKG, "ActionPing.SpecifyHost.Label"));
      return result;
    }

    try {
      if (ipingtype == isystemPing || ipingtype == ibothPings) {
        // Perform a system (Java) ping ...
        status = systemPing(hostname, timeoutInt);
        if (status) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionPing.SystemPing"),
                BaseMessages.getString(PKG, CONST_ACTION_PING_OK_LABEL, hostname));
          }
        } else {
          logError(
              BaseMessages.getString(PKG, "ActionPing.SystemPing"),
              BaseMessages.getString(PKG, CONST_ACTION_PING_NOK_LABEL, hostname));
        }
      }
      if ((ipingtype == iclassicPing) || (ipingtype == ibothPings && !status)) {
        // Perform a classic ping ..
        status = classicPing(hostname, packets);
        if (status) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionPing.ClassicPing"),
                BaseMessages.getString(PKG, CONST_ACTION_PING_OK_LABEL, hostname));
          }
        } else {
          logError(
              BaseMessages.getString(PKG, "ActionPing.ClassicPing"),
              BaseMessages.getString(PKG, CONST_ACTION_PING_NOK_LABEL, hostname));
        }
      }
    } catch (Exception ex) {
      logError(BaseMessages.getString(PKG, "ActionPing.Error.Label") + ex.getMessage());
    }
    if (status) {
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, CONST_ACTION_PING_OK_LABEL, hostname));
      }
      result.setNrErrors(0);
      result.setResult(true);
    } else {
      logError(BaseMessages.getString(PKG, CONST_ACTION_PING_NOK_LABEL, hostname));
    }
    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  private boolean systemPing(String hostname, int timeout) {
    boolean retval = false;

    InetAddress address = null;
    try {
      address = InetAddress.getByName(hostname);
      if (address == null) {
        logError(BaseMessages.getString(PKG, "ActionPing.CanNotGetAddress", hostname));
        return retval;
      }

      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionPing.HostName", address.getHostName()));
        logDetailed(
            BaseMessages.getString(PKG, "ActionPing.HostAddress", address.getHostAddress()));
      }

      retval = address.isReachable(timeout);
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionPing.ErrorSystemPing", hostname, e.getMessage()));
    }
    return retval;
  }

  private boolean classicPing(String hostname, int nrpackets) {
    boolean retval = false;
    try {
      String lignePing = "";
      String cmdPing = "ping ";
      if (Const.isWindows()) {
        cmdPing += hostname + " " + WINDOWS_CHAR + " " + nrpackets;
      } else {
        cmdPing += hostname + " " + NIX_CHAR + " " + nrpackets;
      }

      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionPing.NbrPackets.Label", "" + nrpackets));
        logDetailed(BaseMessages.getString(PKG, "ActionPing.ExecClassicPing.Label", cmdPing));
      }
      Process processPing = Runtime.getRuntime().exec(cmdPing);
      try {
        processPing.waitFor();
      } catch (InterruptedException e) {
        logDetailed(BaseMessages.getString(PKG, "ActionPing.ClassicPingInterrupted"));
      }
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionPing.Gettingresponse.Label", hostname));
      }
      // Get ping response
      BufferedReader br = new BufferedReader(new InputStreamReader(processPing.getInputStream()));

      // Read response lines
      while ((lignePing = br.readLine()) != null) {
        if (isDetailed()) {
          logDetailed(lignePing);
        }
      }
      // We succeed only when 0% lost of data
      if (processPing.exitValue() == 0) {
        retval = true;
      }
    } catch (IOException ex) {
      logError(BaseMessages.getString(PKG, "ActionPing.Error.Label") + ex.getMessage());
    }
    return retval;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (!Utils.isEmpty(hostname)) {
      String realServername = resolve(hostname);
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
            CONST_HOSTNAME,
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }

  // Here for backwards compatibility with old writing method
  public void setOldNbrPackets(String oldNbrPackets) {
    if (oldNbrPackets != null) {
      this.nbrPackets = oldNbrPackets;
    }
    this.oldNbrPackets = null;
  }
}
