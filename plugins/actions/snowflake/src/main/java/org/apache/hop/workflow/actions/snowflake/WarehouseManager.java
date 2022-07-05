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

package org.apache.hop.workflow.actions.snowflake;

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.w3c.dom.Node;

import java.util.List;

import static org.apache.hop.workflow.action.validator.ActionValidatorUtils.andValidator;
import static org.apache.hop.workflow.action.validator.ActionValidatorUtils.notBlankValidator;
import static org.apache.hop.workflow.action.validator.AndValidator.putValidators;

@Action(
    id = "SnowflakeWarehouseManager",
    image = "snowflake-whm.svg",
    name = "Action.Name",
    description = "Action.Description",
    categoryDescription = "Category.Description",
    documentationUrl = "")
public class WarehouseManager extends ActionBase implements Cloneable, IAction {
  public static final String MANAGEMENT_ACTION = "managementAction";
  public static final String REPLACE = "replace";
  public static final String FAIL_IF_EXISTS = "failIfExists";
  public static final String WAREHOUSE_NAME = "warehouseName";
  public static final String WAREHOUSE_SIZE = "warehouseSize";
  public static final String WAREHOUSE_TYPE = "warehouseType";
  public static final String MAX_CLUSTER_COUNT = "maxClusterCount";
  public static final String MIN_CLUSTER_COUNT = "minClusterCount";
  public static final String AUTO_SUSPEND = "autoSuspend";
  public static final String AUTO_RESUME = "autoResume";
  public static final String INITIALLY_SUSPENDED = "initiallySuspended";
  public static final String COMMENT = "comment";
  public static final String RESOURCE_MONITOR = "resourceMonitor";
  public static final String CONNECTION = "connection";
  /** The type of management actions this action supports */
  private static final String[] MANAGEMENT_ACTIONS = {
    "create", "drop", "resume", "suspend", "alter"
  };

  public static final int MANAGEMENT_ACTION_CREATE = 0;
  public static final int MANAGEMENT_ACTION_DROP = 1;
  public static final int MANAGEMENT_ACTION_RESUME = 2;
  public static final int MANAGEMENT_ACTION_SUSPEND = 3;
  public static final int MANAGEMENT_ACTION_ALTER = 4;

  /** The valid warehouse sizes */
  private static final String[] WAREHOUSE_SIZES = {
    "XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE"
  };
  /** The valid warehouse types */
  private static final String[] WAREHOUSE_TYPES = {"Standard", "Enterprise"};

  public static final String FAIL_IF_NOT_EXISTS = "failIfNotExists";
  private static final Class<?> PKG =
      WarehouseManager.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$
  /** The database to connect to. */
  private DatabaseMeta databaseMeta;

  /** The management action to perform. */
  private String managementAction;

  /** The name of the warehouse. */
  private String warehouseName;

  /** CREATE: If the warehouse exists, should it be replaced */
  private boolean replace;

  /** CREATE: Fail if the warehouse exists */
  private boolean failIfExists;

  /** DROP: Fail if the warehouse does not exist */
  private boolean failIfNotExists;

  /** CREATE: The warehouse size to use */
  private String warehouseSize;

  /** CREATE: The warehouse type to use */
  private String warehouseType;

  /** CREATE: The maximum cluster size */
  private String maxClusterCount;

  /** CREATE: The minimum cluster size */
  private String minClusterCount;

  /** CREATE: Should the warehouse automatically suspend */
  private String autoSuspend;

  /** CREATE: Should the warehouse automatically resume when it receives a statement */
  private boolean autoResume;

  /** CREATE: Should the warehouse start in a suspended state */
  private boolean initiallySuspended;

  /** CREATE: The resource monitor to control the warehouse for billing */
  private String resourceMonitor;

  /** CREATE: The comment to associate with the statement */
  private String comment;

  public WarehouseManager(String name) {
    super(name, "");
    setDefault();
  }

  public WarehouseManager() {
    this("");
    setDefault();
  }

  public void setDefault() {
    failIfExists = true;
    failIfNotExists = true;
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  public void setDatabaseMeta(DatabaseMeta databaseMeta) {
    this.databaseMeta = databaseMeta;
  }

  public String getManagementAction() {
    return managementAction;
  }

  public void setManagementAction(String managementAction) {
    this.managementAction = managementAction;
  }

  public int getManagementActionId() {
    if (managementAction != null) {
      for (int i = 0; i < MANAGEMENT_ACTIONS.length; i++) {
        if (managementAction.equals(MANAGEMENT_ACTIONS[i])) {
          return i;
        }
      }
    }
    return -1;
  }

  public void setManagementActionById(int managementActionId) {
    if (managementActionId >= 0 && managementActionId <= MANAGEMENT_ACTIONS.length) {
      managementAction = MANAGEMENT_ACTIONS[managementActionId];
    } else {
      managementAction = null;
    }
  }

  public String getWarehouseName() {
    return warehouseName;
  }

  public void setWarehouseName(String warehouseName) {
    this.warehouseName = warehouseName;
  }

  public boolean isReplace() {
    return replace;
  }

  public void setReplace(boolean replace) {
    this.replace = replace;
  }

  public boolean isFailIfExists() {
    return failIfExists;
  }

  public void setFailIfExists(boolean failIfExists) {
    this.failIfExists = failIfExists;
  }

  public boolean isFailIfNotExists() {
    return failIfNotExists;
  }

  public void setFailIfNotExists(boolean failIfNotExists) {
    this.failIfNotExists = failIfNotExists;
  }

  public String getWarehouseSize() {
    return warehouseSize;
  }

  public void setWarehouseSize(String warehouseSize) {
    this.warehouseSize = warehouseSize;
  }

  public int getWarehouseSizeId() {
    if (warehouseSize != null) {
      for (int i = 0; i < WAREHOUSE_SIZES.length; i++) {
        if (warehouseSize.equals(WAREHOUSE_SIZES[i])) {
          return i;
        }
      }
    }
    return -1;
  }

  public void setWarehouseSizeById(int warehouseSizeId) {
    if (warehouseSizeId >= 0 && warehouseSizeId < WAREHOUSE_SIZES.length) {
      warehouseSize = WAREHOUSE_SIZES[warehouseSizeId];
    } else {
      warehouseSize = null;
    }
  }

  public String getWarehouseType() {
    return warehouseType;
  }

  public void setWarehouseType(String warehouseType) {
    this.warehouseType = warehouseType;
  }

  public int getWarehouseTypeId() {
    if (warehouseType != null) {
      for (int i = 0; i < WAREHOUSE_TYPES.length; i++) {
        if (warehouseType.equals(WAREHOUSE_TYPES[i])) {
          return i;
        }
      }
    }
    return -1;
  }

  public void setWarehouseTypeById(int warehouseTypeId) {
    if (warehouseTypeId >= 0 && warehouseTypeId < WAREHOUSE_TYPES.length) {
      warehouseType = WAREHOUSE_TYPES[warehouseTypeId];
    } else {
      warehouseType = null;
    }
  }

  public String getMaxClusterCount() {
    return maxClusterCount;
  }

  public void setMaxClusterCount(String maxClusterCount) {
    this.maxClusterCount = maxClusterCount;
  }

  public String getMinClusterCount() {
    return minClusterCount;
  }

  public void setMinClusterCount(String minClusterCount) {
    this.minClusterCount = minClusterCount;
  }

  public String getAutoSuspend() {
    return autoSuspend;
  }

  public void setAutoSuspend(String autoSuspend) {
    this.autoSuspend = autoSuspend;
  }

  public boolean isAutoResume() {
    return autoResume;
  }

  public void setAutoResume(boolean autoResume) {
    this.autoResume = autoResume;
  }

  public boolean isInitiallySuspended() {
    return initiallySuspended;
  }

  public void setInitiallySuspended(boolean initiallySuspended) {
    this.initiallySuspended = initiallySuspended;
  }

  public String getResourceMonitor() {
    return resourceMonitor;
  }

  public void setResourceMonitor(String resourceMonitor) {
    this.resourceMonitor = resourceMonitor;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Override
  public String getXml() {
    StringBuffer returnValue = new StringBuffer(300);

    returnValue.append(super.getXml());
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                CONNECTION, databaseMeta == null ? null : databaseMeta.getName()));
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                MANAGEMENT_ACTION, getManagementAction())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(XmlHandler.addTagValue(REPLACE, isReplace())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(FAIL_IF_EXISTS, isFailIfExists())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                WAREHOUSE_NAME, getWarehouseName())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                WAREHOUSE_SIZE, getWarehouseSize())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                WAREHOUSE_TYPE, getWarehouseType())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                MAX_CLUSTER_COUNT, getMaxClusterCount())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                MIN_CLUSTER_COUNT, getMinClusterCount())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(AUTO_SUSPEND, getAutoSuspend())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(XmlHandler.addTagValue(AUTO_RESUME, isAutoResume())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                INITIALLY_SUSPENDED, isInitiallySuspended())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                RESOURCE_MONITOR, getResourceMonitor())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(XmlHandler.addTagValue(COMMENT, getComment())); // $NON-NLS-1$ //$NON-NLS-2$
    returnValue
        .append("      ")
        .append(XmlHandler.addTagValue(FAIL_IF_NOT_EXISTS, isFailIfNotExists()));

    return returnValue.toString();
  }

  @Override
  public void loadXml(Node entryNode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entryNode);
      String dbname = XmlHandler.getTagValue(entryNode, CONNECTION);
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, dbname);

      setManagementAction(XmlHandler.getTagValue(entryNode, MANAGEMENT_ACTION));
      setReplace("Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, REPLACE)));
      setFailIfExists("Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, FAIL_IF_EXISTS)));
      setWarehouseName(XmlHandler.getTagValue(entryNode, WAREHOUSE_NAME));
      setWarehouseSize(XmlHandler.getTagValue(entryNode, WAREHOUSE_SIZE));
      setWarehouseType(XmlHandler.getTagValue(entryNode, WAREHOUSE_TYPE));
      setMaxClusterCount(XmlHandler.getTagValue(entryNode, MAX_CLUSTER_COUNT));
      setMinClusterCount(XmlHandler.getTagValue(entryNode, MIN_CLUSTER_COUNT));
      setAutoSuspend(XmlHandler.getTagValue(entryNode, AUTO_SUSPEND));
      setAutoResume("Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, AUTO_RESUME)));
      setInitiallySuspended(
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, INITIALLY_SUSPENDED)));
      setResourceMonitor(XmlHandler.getTagValue(entryNode, RESOURCE_MONITOR));
      setComment(XmlHandler.getTagValue(entryNode, COMMENT));
      setFailIfNotExists(
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, FAIL_IF_NOT_EXISTS)));
    } catch (HopXmlException dbe) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Error.Exception.UnableLoadXML"),
          dbe);
    }
  }

  @Override
  public void clear() {
    super.clear();

    setManagementAction(null);
    setReplace(false);
    setFailIfExists(false);
    setWarehouseName(null);
    setWarehouseSize(null);
    setWarehouseType(null);
    setMaxClusterCount(null);
    setMinClusterCount(null);
    setAutoSuspend(null);
    setAutoResume(false);
    setInitiallySuspended(false);
    setResourceMonitor(null);
    setComment(null);
    setDatabaseMeta(null);
    setFailIfNotExists(true);
  }

  public boolean validate() {
    boolean result = true;
    if (databaseMeta == null || StringUtil.isEmpty(databaseMeta.getName())) {
      logError(BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Validate.DatabaseIsEmpty"));
      result = false;
    } else if (StringUtil.isEmpty(managementAction)) {
      logError(BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Validate.ManagementAction"));
      result = false;
    } else if (managementAction.equals(MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_CREATE])) {
      if (!StringUtil.isEmpty(resolve(maxClusterCount))
          && Const.toInt(resolve(maxClusterCount), -1) <= 0) {

        logError(
            BaseMessages.getString(
                PKG,
                "SnowflakeWarehouseManager.Validate.MaxClusterCount",
                resolve(maxClusterCount)));
        return false;
      }

      if (!StringUtil.isEmpty(resolve(minClusterCount))
          && Const.toInt(resolve(minClusterCount), -1) < 0) {

        logError(
            BaseMessages.getString(
                PKG,
                "SnowflakeWarehouseManager.Validate.MinClusterCount",
                resolve(minClusterCount)));
        return false;
      }

      if (!StringUtil.isEmpty(resolve(autoSuspend)) && Const.toInt(resolve(autoSuspend), -1) < 0) {
        logError(
            BaseMessages.getString(
                PKG, "SnowflakeWarehouseManager.Validate.AutoSuspend", resolve(autoSuspend)));
        return false;
      }
    }
    return result;
  }

  public Result execute(Result previousResult, int nr) throws HopException {

    Result result = previousResult;
    result.setResult(validate());
    if (!result.getResult()) {
      return result;
    }

    Database db = null;
    try {
      db = new Database(this, this, databaseMeta);
      String sql = null;
      String successMessage = null;

      if (managementAction.equals(MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_CREATE])) {
        sql = getCreateSQL();
        successMessage =
            BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Log.Create.Success");
      } else if (managementAction.equals(MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_DROP])) {
        sql = getDropSQL();
        successMessage = BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Log.Drop.Success");
      } else if (managementAction.equals(MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_RESUME])) {
        sql = getResumeSQL();
        successMessage =
            BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Log.Resume.Success");
      } else if (managementAction.equals(MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_SUSPEND])) {
        sql = getSuspendSQL();
        successMessage =
            BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Log.Suspend.Success");
      } else if (managementAction.equals(MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_ALTER])) {
        sql = getAlterSQL();
        successMessage = BaseMessages.getString(PKG, "SnowflakeWarehouseManager.Log.Alter.Success");
      }

      if (sql == null) {
        throw new HopException("Unable to generate action, could not find action type");
      }

      db.connect();
      logDebug("Executing SQL " + sql);
      db.execStatements(sql);
      logBasic(successMessage);

    } catch (Exception ex) {
      logError("Error managing warehouse", ex);
      result.setResult(false);
    } finally {
      try {
        if (db != null) {
          db.disconnect();
        }
      } catch (Exception ex) {
        logError("Unable to disconnect from database", ex);
      }
    }

    return result;
  }

  private String getDropSQL() {
    StringBuilder sql = new StringBuilder();
    sql.append("DROP WAREHOUSE ");
    if (!failIfNotExists) {
      sql.append("IF EXISTS ");
    }
    sql.append(resolve(warehouseName)).append(";\ncommit;");
    return sql.toString();
  }

  private String getResumeSQL() {
    StringBuilder sql = new StringBuilder();
    sql.append("ALTER WAREHOUSE ");
    if (!failIfNotExists) {
      sql.append("IF EXISTS ");
    }
    sql.append(resolve(warehouseName)).append(" RESUME;\ncommit;");
    return sql.toString();
  }

  private String getSuspendSQL() {
    StringBuilder sql = new StringBuilder();
    sql.append("ALTER WAREHOUSE ");
    if (!failIfNotExists) {
      sql.append("IF EXISTS ");
    }
    sql.append(resolve(warehouseName)).append(" SUSPEND;\ncommit;");
    return sql.toString();
  }

  private String getCreateSQL() {
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE ");
    if (replace) {
      sql.append("OR REPLACE ");
    }
    sql.append("WAREHOUSE ");
    if (!failIfExists && !replace) {
      sql.append("IF NOT EXISTS ");
    }
    sql.append(warehouseName).append(" WITH ");

    if (!StringUtil.isEmpty(resolve(warehouseSize))) {
      sql.append("WAREHOUSE_SIZE = '").append(resolve(warehouseSize)).append("' ");
    }

    if (!StringUtil.isEmpty(resolve(warehouseType))) {
      sql.append("WAREHOUSE_TYPE = ").append(resolve(warehouseType)).append(" ");
    }

    if (!StringUtil.isEmpty(resolve(maxClusterCount))) {
      sql.append("MAX_CLUSTER_COUNT = ").append(resolve(maxClusterCount)).append(" ");
    }

    if (!StringUtil.isEmpty(resolve(minClusterCount))) {
      sql.append("MIN_CLUSTER_COUNT = ").append(resolve(minClusterCount)).append(" ");
    }

    if (!StringUtil.isEmpty(resolve(autoSuspend))) {
      sql.append("AUTO_SUSPEND = ").append(Const.toInt(resolve(autoSuspend), 0) * 60).append(" ");
    }

    sql.append("AUTO_RESUME = ").append(autoResume).append(" ");
    sql.append("INITIALLY_SUSPENDED = ").append(initiallySuspended).append(" ");

    if (!StringUtil.isEmpty(resolve(resourceMonitor))) {
      sql.append("RESOURCE_MONITOR = '").append(resolve(resourceMonitor)).append("' ");
    }

    if (!StringUtil.isEmpty(resolve(comment))) {
      sql.append("COMMENT = \"").append(comment.replaceAll("\"", "\"\"")).append("\" ");
    }

    sql.append(";\ncommit;");
    return sql.toString();
  }

  private String getAlterSQL() {
    StringBuilder sql = new StringBuilder();
    sql.append("ALTER WAREHOUSE ");
    if (!failIfNotExists) {
      sql.append("IF EXISTS ");
    }
    sql.append(warehouseName).append(" SET ");

    if (!StringUtil.isEmpty(resolve(warehouseSize))) {
      sql.append("WAREHOUSE_SIZE = '").append(resolve(warehouseSize)).append("' ");
    }

    if (!StringUtil.isEmpty(resolve(warehouseType))) {
      sql.append("WAREHOUSE_TYPE = ").append(resolve(warehouseType)).append(" ");
    }

    if (!StringUtil.isEmpty(resolve(maxClusterCount))) {
      sql.append("MAX_CLUSTER_COUNT = ").append(resolve(maxClusterCount)).append(" ");
    }

    if (!StringUtil.isEmpty(resolve(minClusterCount))) {
      sql.append("MIN_CLUSTER_COUNT = ").append(resolve(minClusterCount)).append(" ");
    }

    if (!StringUtil.isEmpty(resolve(autoSuspend))) {
      sql.append("AUTO_SUSPEND = ").append(Const.toInt(resolve(autoSuspend), 0) * 60).append(" ");
    }

    sql.append("AUTO_RESUME = ").append(autoResume).append(" ");

    if (!StringUtil.isEmpty(resolve(resourceMonitor))) {
      sql.append("RESOURCE_MONITOR = '").append(resolve(resourceMonitor)).append("' ");
    }

    if (!StringUtil.isEmpty(resolve(comment))) {
      sql.append("COMMENT = \"").append(comment.replaceAll("\"", "\"\"")).append("\" ");
    }

    sql.append(";\ncommit;");
    return sql.toString();
  }

  public boolean evaluates() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    andValidator().validate(this, CONNECTION, remarks, putValidators(notBlankValidator()));
    andValidator().validate(this, WAREHOUSE_NAME, remarks, putValidators(notBlankValidator()));
    andValidator().validate(this, MANAGEMENT_ACTION, remarks, putValidators(notBlankValidator()));
  }
}
