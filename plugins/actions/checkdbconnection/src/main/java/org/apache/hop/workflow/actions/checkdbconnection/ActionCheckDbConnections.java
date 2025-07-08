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
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
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
  private static final Class<?> PKG = ActionCheckDbConnections.class;

  @HopMetadataProperty(groupKey = "connections", key = "connection")
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
        String databaseName = resolve(connection.getName());
        DatabaseMeta databaseMeta = loadDatabaseMeta(databaseName);
        if (databaseMeta == null) {
          logError(
              BaseMessages.getString(
                  PKG,
                  "ActionCheckDbConnections.DatabaseConnectionCouldNotBeLoaded.Message",
                  databaseName));
          nrErrors++;
        } else {
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
                        PKG,
                        "ActionCheckDbConnections.Wait",
                        "" + iMaximumTimeout,
                        waitTimeMessage));
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

  private DatabaseMeta loadDatabaseMeta(String databaseName) {
    try {
      IHopMetadataSerializer<DatabaseMeta> serializer =
          getMetadataProvider().getSerializer(DatabaseMeta.class);
      return serializer.load(databaseName);
    } catch (HopException e) {
      logError("Error loading database metadata for connection '" + databaseName + "'", e);
      return null;
    }
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);

    for (CDConnection connection : connections) {
      DatabaseMeta databaseMeta = loadDatabaseMeta(resolve(connection.getName()));
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

  @Getter
  @Setter
  public static final class CDConnection {
    @HopMetadataProperty(key = "name")
    private String name;

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
      this.name = c.name;
      this.waitTime = c.waitTime;
      this.waitTimeUnit = c.waitTimeUnit;
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
