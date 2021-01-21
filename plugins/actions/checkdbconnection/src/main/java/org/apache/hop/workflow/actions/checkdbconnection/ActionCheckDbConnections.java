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

package org.apache.hop.workflow.actions.checkdbconnection;

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopXmlException;
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

import java.util.List;

/**
 * This check db connections
 *
 * @author Samatar
 * @since 10-12-2007
 */
@Action(
    id = "CHECK_DB_CONNECTIONS",
    name = "i18n::ActionCheckDbConnections.Name",
    description = "i18n::ActionCheckDbConnections.Description",
    image = "CheckDbConnection.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/actions/checkdbconnection.html")
public class ActionCheckDbConnections extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionCheckDbConnections.class; // For Translator

  private DatabaseMeta[] connections;

  protected static final String[] unitTimeDesc =
      new String[] {
        BaseMessages.getString(PKG, "ActionCheckDbConnections.UnitTimeMilliSecond.Label"),
        BaseMessages.getString(PKG, "ActionCheckDbConnections.UnitTimeSecond.Label"),
        BaseMessages.getString(PKG, "ActionCheckDbConnections.UnitTimeMinute.Label"),
        BaseMessages.getString(PKG, "ActionCheckDbConnections.UnitTimeHour.Label"),
      };
  protected static final String[] unitTimeCode =
      new String[] {"millisecond", "second", "minute", "hour"};

  public static final int UNIT_TIME_MILLI_SECOND = 0;
  public static final int UNIT_TIME_SECOND = 1;
  public static final int UNIT_TIME_MINUTE = 2;
  public static final int UNIT_TIME_HOUR = 3;

  private String[] waitfors;
  private int[] waittimes;

  private long timeStart;
  private long now;

  public ActionCheckDbConnections(String name) {
    super(name, "");
    connections = null;
    waitfors = null;
    waittimes = null;
  }

  public ActionCheckDbConnections() {
    this("");
  }

  public Object clone() {
    ActionCheckDbConnections je = (ActionCheckDbConnections) super.clone();
    return je;
  }

  public DatabaseMeta[] getConnections() {
    return connections;
  }

  public void setConnections(DatabaseMeta[] connections) {
    this.connections = connections;
  }

  public String[] getWaitfors() {
    return waitfors;
  }

  public void setWaitfors(String[] waitfors) {
    this.waitfors = waitfors;
  }

  public int[] getWaittimes() {
    return waittimes;
  }

  public void setWaittimes(int[] waittimes) {
    this.waittimes = waittimes;
  }

  public long getTimeStart() {
    return timeStart;
  }

  public long getNow() {
    return now;
  }

  private static String getWaitTimeCode(int i) {
    if (i < 0 || i >= unitTimeCode.length) {
      return unitTimeCode[0];
    }
    return unitTimeCode[i];
  }

  public static String getWaitTimeDesc(int i) {
    if (i < 0 || i >= unitTimeDesc.length) {
      return unitTimeDesc[0];
    }
    return unitTimeDesc[i];
  }

  public static int getWaitTimeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < unitTimeDesc.length; i++) {
      if (unitTimeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getWaitTimeByCode(tt);
  }

  private static int getWaitTimeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < unitTimeCode.length; i++) {
      if (unitTimeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(120);
    retval.append(super.getXml());
    retval.append("      <connections>").append(Const.CR);
    if (connections != null) {
      for (int i = 0; i < connections.length; i++) {
        retval.append("        <connection>").append(Const.CR);
        retval
            .append("          ")
            .append(
                XmlHandler.addTagValue(
                    "name", connections[i] == null ? null : connections[i].getName()));
        retval.append("          ").append(XmlHandler.addTagValue("waitfor", waitfors[i]));
        retval
            .append("          ")
            .append(XmlHandler.addTagValue("waittime", getWaitTimeCode(waittimes[i])));
        retval.append("        </connection>").append(Const.CR);
      }
    }
    retval.append("      </connections>").append(Const.CR);

    return retval.toString();
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      Node fields = XmlHandler.getSubNode(entrynode, "connections");

      // How many hosts?
      int nrFields = XmlHandler.countNodes(fields, "connection");
      connections = new DatabaseMeta[nrFields];
      waitfors = new String[nrFields];
      waittimes = new int[nrFields];
      // Read them all...
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "connection", i);
        String dbname = XmlHandler.getTagValue(fnode, "name");
        connections[i] = DatabaseMeta.loadDatabase(metadataProvider, dbname);
        waitfors[i] = XmlHandler.getTagValue(fnode, "waitfor");
        waittimes[i] = getWaitTimeByCode(Const.NVL(XmlHandler.getTagValue(fnode, "waittime"), ""));
      }
    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG,
              "ActionCheckDbConnections.ERROR_0001_Cannot_Load_Job_Entry_From_Xml_Node",
              xe.getMessage()));
    }
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(true);
    int nrerrors = 0;
    int nrsuccess = 0;

    if (connections != null) {
      for (int i = 0; i < connections.length && !parentWorkflow.isStopped(); i++) {
        Database db = new Database(this, this, connections[i] );
        try {
          db.connect();

          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionCheckDbConnections.Connected",
                    connections[i].getDatabaseName(),
                    connections[i].getName()));
          }

          int iMaximumTimeout = Const.toInt(resolve(waitfors[i]), 0);
          if (iMaximumTimeout > 0) {

            int multiple = 1;
            String waitTimeMessage = unitTimeDesc[0];
            switch (waittimes[i]) {
              case ActionCheckDbConnections.UNIT_TIME_MILLI_SECOND:
                multiple = 1;
                waitTimeMessage = unitTimeDesc[0];
                break;
              case ActionCheckDbConnections.UNIT_TIME_SECOND:
                multiple = 1000; // Second
                waitTimeMessage = unitTimeDesc[1];
                break;
              case ActionCheckDbConnections.UNIT_TIME_MINUTE:
                multiple = 60000; // Minute
                waitTimeMessage = unitTimeDesc[2];
                break;
              case ActionCheckDbConnections.UNIT_TIME_HOUR:
                multiple = 3600000; // Hour
                waitTimeMessage = unitTimeDesc[3];
                break;
              default:
                multiple = 1000; // Second
                waitTimeMessage = unitTimeDesc[1];
                break;
            }
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG, "ActionCheckDbConnections.Wait", "" + iMaximumTimeout, waitTimeMessage));
            }

            // starttime (in seconds ,Minutes or Hours)
            timeStart = System.currentTimeMillis();

            boolean continueLoop = true;
            while (continueLoop && !parentWorkflow.isStopped()) {
              // Update Time value
              now = System.currentTimeMillis();
              // Let's check the limit time
              if ((now >= (timeStart + iMaximumTimeout * multiple))) {
                // We have reached the time limit
                if (isDetailed()) {
                  logDetailed(
                      BaseMessages.getString(
                          PKG,
                          "ActionCheckDbConnections.WaitTimeIsElapsed.Label",
                          connections[i].getDatabaseName(),
                          connections[i].getName()));
                }

                continueLoop = false;
              } else {
                try {
                  Thread.sleep(100);
                } catch (Exception e) {
                  // Ignore sleep errors
                }
              }
            }
          }

          nrsuccess++;
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionCheckDbConnections.ConnectionOK",
                    connections[i].getDatabaseName(),
                    connections[i].getName()));
          }
        } catch (HopDatabaseException e) {
          nrerrors++;
          logError(
              BaseMessages.getString(
                  PKG,
                  "ActionCheckDbConnections.Exception",
                  connections[i].getDatabaseName(),
                  connections[i].getName(),
                  e.toString()));
        } finally {
          if (db != null) {
            try {
              db.disconnect();
              db = null;
            } catch (Exception e) {
              /* Ignore */
            }
          }
        }
      }
    }

    if (nrerrors > 0) {
      result.setNrErrors(nrerrors);
      result.setResult(false);
    }

    if (isDetailed()) {
      logDetailed("=======================================");
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionCheckDbConnections.Log.Info.ConnectionsInError", "" + nrerrors));
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionCheckDbConnections.Log.Info.ConnectionsInSuccess", "" + nrsuccess));
      logDetailed("=======================================");
    }

    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    return connections;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (connections != null) {
      for (int i = 0; i < connections.length; i++) {
        DatabaseMeta connection = connections[i];
        ResourceReference reference = new ResourceReference(this);
        reference
            .getEntries()
            .add(new ResourceEntry(connection.getHostname(), ResourceType.SERVER));
        reference
            .getEntries()
            .add(new ResourceEntry(connection.getDatabaseName(), ResourceType.DATABASENAME));
        references.add(reference);
      }
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
            "tablename",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "columnname",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}
