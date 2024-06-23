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

package org.apache.hop.workflow.actions.checkdbconnection;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/** This check db connections */
@Action(
    id = "CHECK_DB_CONNECTIONS",
    name = "i18n::ActionCheckDbConnections.Name",
    description = "i18n::ActionCheckDbConnections.Description",
    image = "CheckDbConnection.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionCheckDbConnections.keyword",
    documentationUrl = "/workflow/actions/checkdbconnection.html",
    actionTransformTypes = {ActionTransformType.ENV_CHECK, ActionTransformType.RDBMS})
public class ActionCheckDbConnections extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionCheckDbConnections.class; // For Translator

  @HopMetadataProperty(
      groupKey = "connections",
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private List<CDConnection> connections;

  public ActionCheckDbConnections(String name) {
    super(name, "");
    connections = new ArrayList<>();
  }

  public ActionCheckDbConnections() {
    this("");
  }

  public ActionCheckDbConnections(ActionCheckDbConnections other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
    this.connections = other.getConnections();
  }

  @Override
  public Object clone() {
    return new ActionCheckDbConnections(this);
  }

  @Override
  public Result execute(Result result, int nr) {
    result.setResult(true);
    int nrErrors = 0;
    int nrSuccess = 0;

    long timeStart;
    long now;

    if (connections != null) {
      for (CDConnection connection : connections) {
        if (parentWorkflow.isStopped()) {
          break;
        }
        DatabaseMeta databaseMeta = connection.getDatabaseMeta();
        try (Database db = new Database(this, this, databaseMeta)) {
          db.connect();

          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionCheckDbConnections.Connected",
                    databaseMeta.getDatabaseName(),
                    databaseMeta.getName()));
          }

          long iMaximumTimeout = Const.toLong(resolve(connection.getWaitTime()), 0L);
          if (iMaximumTimeout > 0) {

            WaitTimeUnit timeUnit = connection.getWaitTimeUnit();
            long multiple = timeUnit.getFactor();
            String waitTimeMessage = connection.getWaitTimeUnit().getDescription();
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG, "ActionCheckDbConnections.Wait", "" + iMaximumTimeout, waitTimeMessage));
            }

            // The start time (in seconds ,Minutes or Hours)
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
                          databaseMeta.getDatabaseName(),
                          databaseMeta.getName()));
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

          nrSuccess++;
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ActionCheckDbConnections.ConnectionOK",
                    databaseMeta.getDatabaseName(),
                    databaseMeta.getName()));
          }
        } catch (HopDatabaseException e) {
          nrErrors++;
          logError(
              BaseMessages.getString(
                  PKG,
                  "ActionCheckDbConnections.Exception",
                  databaseMeta.getDatabaseName(),
                  databaseMeta.getName(),
                  e.toString()));
        }
      }
    }

    if (nrErrors > 0) {
      result.setNrErrors(nrErrors);
      result.setResult(false);
    }

    if (isDetailed()) {
      logDetailed("=======================================");
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionCheckDbConnections.Log.Info.ConnectionsInError", "" + nrErrors));
      logDetailed(
          BaseMessages.getString(
              PKG, "ActionCheckDbConnections.Log.Info.ConnectionsInSuccess", "" + nrSuccess));
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
    List<DatabaseMeta> used = new ArrayList<>();
    for (CDConnection connection : connections) {
      if (connection.getDatabaseMeta() != null) {
        used.add(connection.getDatabaseMeta());
      }
    }
    return used.toArray(new DatabaseMeta[0]);
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);

    for (CDConnection connection : connections) {
      DatabaseMeta databaseMeta = connection.getDatabaseMeta();
      if (databaseMeta != null) {
        ResourceReference reference = new ResourceReference(this);
        reference
            .getEntries()
            .add(new ResourceEntry(databaseMeta.getHostname(), ResourceType.SERVER));
        reference
            .getEntries()
            .add(new ResourceEntry(databaseMeta.getDatabaseName(), ResourceType.DATABASENAME));
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

  public enum WaitTimeUnit implements IEnumHasCodeAndDescription {
    MILLISECOND(
        "millisecond",
        BaseMessages.getString(PKG, "ActionCheckDbConnections.UnitTimeMilliSecond.Label"),
        1L),
    SECOND(
        "second",
        BaseMessages.getString(PKG, "ActionCheckDbConnections.UnitTimeSecond.Label"),
        1000L),
    MINUTE(
        "minute",
        BaseMessages.getString(PKG, "ActionCheckDbConnections.UnitTimeMinute.Label"),
        60000L),
    HOUR(
        "hour",
        BaseMessages.getString(PKG, "ActionCheckDbConnections.UnitTimeHour.Label"),
        3600000L),
    ;
    private final String code;
    private final String description;
    private final long factor;

    WaitTimeUnit(String code, String description, long factor) {
      this.code = code;
      this.description = description;
      this.factor = factor;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(WaitTimeUnit.class);
    }

    public static WaitTimeUnit lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          WaitTimeUnit.class, description, MILLISECOND);
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    @Override
    public String getDescription() {
      return description;
    }

    /**
     * Gets factor
     *
     * @return value of factor
     */
    public long getFactor() {
      return factor;
    }
  }

  public static final class CDConnection {
    @HopMetadataProperty(key = "name", storeWithName = true)
    private DatabaseMeta databaseMeta;

    @HopMetadataProperty(key = "waitfor")
    private String waitTime;

    @HopMetadataProperty(key = "waittime", storeWithCode = true)
    private WaitTimeUnit waitTimeUnit;

    public CDConnection() {
      waitTime = "200";
      waitTimeUnit = WaitTimeUnit.MILLISECOND;
    }

    public CDConnection(CDConnection c) {
      this();
      this.databaseMeta = c.databaseMeta == null ? null : new DatabaseMeta(c.databaseMeta);
      this.waitTime = c.waitTime;
      this.waitTimeUnit = c.waitTimeUnit;
    }

    /**
     * Gets connection
     *
     * @return value of connection
     */
    public DatabaseMeta getDatabaseMeta() {
      return databaseMeta;
    }

    /**
     * Sets connection
     *
     * @param databaseMeta value of connection
     */
    public void setDatabaseMeta(DatabaseMeta databaseMeta) {
      this.databaseMeta = databaseMeta;
    }

    /**
     * Gets waitTime
     *
     * @return value of waitTime
     */
    public String getWaitTime() {
      return waitTime;
    }

    /**
     * Sets waitTime
     *
     * @param waitTime value of waitTime
     */
    public void setWaitTime(String waitTime) {
      this.waitTime = waitTime;
    }

    /**
     * Gets waitTimeUnit
     *
     * @return value of waitTimeUnit
     */
    public WaitTimeUnit getWaitTimeUnit() {
      return waitTimeUnit;
    }

    /**
     * Sets waitTimeUnit
     *
     * @param waitTimeUnit value of waitTimeUnit
     */
    public void setWaitTimeUnit(WaitTimeUnit waitTimeUnit) {
      this.waitTimeUnit = waitTimeUnit;
    }
  }

  /**
   * Gets connections
   *
   * @return value of connections
   */
  public List<CDConnection> getConnections() {
    return connections;
  }

  /**
   * Sets connections
   *
   * @param connections value of connections
   */
  public void setConnections(List<CDConnection> connections) {
    this.connections = connections;
  }
}
