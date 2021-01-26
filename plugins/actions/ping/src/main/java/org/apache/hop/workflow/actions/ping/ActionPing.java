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

package org.apache.hop.workflow.actions.ping;

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.w3c.dom.Node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.List;

/**
 * This defines a ping action.
 *
 * @author Samatar Hassan
 * @since Mar-2007
 */
@Action(
    id = "PING",
    name = "i18n::ActionPing.Name",
    description = "i18n::ActionPing.Description",
    image = "Ping.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/ping.html")
public class ActionPing extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionPing.class; // For Translator

  private String hostname;
  private String timeout;
  public String defaultTimeOut = "3000";
  private String nbrPackets;
  private String Windows_CHAR = "-n";
  private String NIX_CHAR = "-c";

  public String classicPing = "classicPing";
  public int iclassicPing = 0;
  public String systemPing = "systemPing";
  public int isystemPing = 1;
  public String bothPings = "bothPings";
  public int ibothPings = 2;

  public String pingtype;
  public int ipingtype;

  public ActionPing(String n) {
    super(n, "");
    pingtype = classicPing;
    hostname = null;
    nbrPackets = "2";
    timeout = defaultTimeOut;
  }

  public ActionPing() {
    this("");
  }

  public Object clone() {
    ActionPing je = (ActionPing) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(100);

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("hostname", hostname));
    retval.append("      ").append(XmlHandler.addTagValue("nbr_packets", nbrPackets));

    // TODO: The following line may be removed 3 versions after 2.5.0
    retval.append("      ").append(XmlHandler.addTagValue("nbrpaquets", nbrPackets));
    retval.append("      ").append(XmlHandler.addTagValue("timeout", timeout));

    retval.append("      ").append(XmlHandler.addTagValue("pingtype", pingtype));

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      String nbrPaquets;
      super.loadXml(entrynode);
      hostname = XmlHandler.getTagValue(entrynode, "hostname");
      nbrPackets = XmlHandler.getTagValue(entrynode, "nbr_packets");

      // TODO: The following lines may be removed 3 versions after 2.5.0
      nbrPaquets = XmlHandler.getTagValue(entrynode, "nbrpaquets");
      if (nbrPackets == null && nbrPaquets != null) {
        // if only nbrpaquets exists this means that the file was
        // save by a version 2.5.0 ping action
        nbrPackets = nbrPaquets;
      }
      timeout = XmlHandler.getTagValue(entrynode, "timeout");
      pingtype = XmlHandler.getTagValue(entrynode, "pingtype");
      if (Utils.isEmpty(pingtype)) {
        pingtype = classicPing;
        ipingtype = iclassicPing;
      } else {
        if (pingtype.equals(systemPing)) {
          ipingtype = isystemPing;
        } else if (pingtype.equals(bothPings)) {
          ipingtype = ibothPings;
        } else {
          ipingtype = iclassicPing;
        }
      }
    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load action of type 'ping' from XML node", xe);
    }
  }

  public String getNbrPackets() {
    return nbrPackets;
  }

  public String getRealNbrPackets() {
    return resolve(getNbrPackets());
  }

  public void setNbrPackets(String nbrPackets) {
    this.nbrPackets = nbrPackets;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getHostname() {
    return hostname;
  }

  public String getRealHostname() {
    return resolve(getHostname());
  }

  public String getTimeOut() {
    return timeout;
  }

  public String getRealTimeOut() {
    return resolve(getTimeOut());
  }

  public void setTimeOut(String timeout) {
    this.timeout = timeout;
  }

  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;

    result.setNrErrors(1);
    result.setResult(false);

    String hostname = getRealHostname();
    int timeoutInt = Const.toInt(getRealTimeOut(), 300);
    int packets = Const.toInt(getRealNbrPackets(), 2);
    boolean status = false;

    if (Utils.isEmpty(hostname)) {
      // No Host was specified
      logError(BaseMessages.getString(PKG, "JobPing.SpecifyHost.Label"));
      return result;
    }

    try {
      if (ipingtype == isystemPing || ipingtype == ibothPings) {
        // Perform a system (Java) ping ...
        status = systemPing(hostname, timeoutInt);
        if (status) {
          if (log.isDetailed()) {
            log.logDetailed(
                BaseMessages.getString(PKG, "JobPing.SystemPing"),
                BaseMessages.getString(PKG, "JobPing.OK.Label", hostname));
          }
        } else {
          log.logError(
              BaseMessages.getString(PKG, "JobPing.SystemPing"),
              BaseMessages.getString(PKG, "JobPing.NOK.Label", hostname));
        }
      }
      if ((ipingtype == iclassicPing) || (ipingtype == ibothPings && !status)) {
        // Perform a classic ping ..
        status = classicPing(hostname, packets);
        if (status) {
          if (log.isDetailed()) {
            log.logDetailed(
                BaseMessages.getString(PKG, "JobPing.ClassicPing"),
                BaseMessages.getString(PKG, "JobPing.OK.Label", hostname));
          }
        } else {
          log.logError(
              BaseMessages.getString(PKG, "JobPing.ClassicPing"),
              BaseMessages.getString(PKG, "JobPing.NOK.Label", hostname));
        }
      }
    } catch (Exception ex) {
      logError(BaseMessages.getString(PKG, "JobPing.Error.Label") + ex.getMessage());
    }
    if (status) {
      if (log.isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "JobPing.OK.Label", hostname));
      }
      result.setNrErrors(0);
      result.setResult(true);
    } else {
      logError(BaseMessages.getString(PKG, "JobPing.NOK.Label", hostname));
    }
    return result;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  private boolean systemPing(String hostname, int timeout) {
    boolean retval = false;

    InetAddress address = null;
    try {
      address = InetAddress.getByName(hostname);
      if (address == null) {
        logError(BaseMessages.getString(PKG, "JobPing.CanNotGetAddress", hostname));
        return retval;
      }

      if (log.isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "JobPing.HostName", address.getHostName()));
        logDetailed(BaseMessages.getString(PKG, "JobPing.HostAddress", address.getHostAddress()));
      }

      retval = address.isReachable(timeout);
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "JobPing.ErrorSystemPing", hostname, e.getMessage()));
    }
    return retval;
  }

  private boolean classicPing(String hostname, int nrpackets) {
    boolean retval = false;
    try {
      String lignePing = "";
      String CmdPing = "ping ";
      if (Const.isWindows()) {
        CmdPing += hostname + " " + Windows_CHAR + " " + nrpackets;
      } else {
        CmdPing += hostname + " " + NIX_CHAR + " " + nrpackets;
      }

      if (log.isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "JobPing.NbrPackets.Label", "" + nrpackets));
        logDetailed(BaseMessages.getString(PKG, "JobPing.ExecClassicPing.Label", CmdPing));
      }
      Process processPing = Runtime.getRuntime().exec(CmdPing);
      try {
        processPing.waitFor();
      } catch (InterruptedException e) {
        logDetailed(BaseMessages.getString(PKG, "JobPing.ClassicPingInterrupted"));
      }
      if (log.isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "JobPing.Gettingresponse.Label", hostname));
      }
      // Get ping response
      BufferedReader br = new BufferedReader(new InputStreamReader(processPing.getInputStream()));

      // Read response lines
      while ((lignePing = br.readLine()) != null) {
        if (log.isDetailed()) {
          logDetailed(lignePing);
        }
      }
      // We succeed only when 0% lost of data
      if (processPing.exitValue() == 0) {
        retval = true;
      }
    } catch (IOException ex) {
      logError(BaseMessages.getString(PKG, "JobPing.Error.Label") + ex.getMessage());
    }
    return retval;
  }

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
            "hostname",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}
