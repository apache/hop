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

package org.apache.hop.workflow.actions.columnsexist;

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
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

import java.util.List;

/**
 * This defines a column exists action.
 *
 * @author Samatar
 * @since 16-06-2008
 */
@Action(
    id = "COLUMNS_EXIST",
    name = "i18n::ActionColumnsExist.Name",
    description = "i18n::ActionColumnsExist.Description",
    image = "ColumnsExist.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/columnsexist.html")
public class ActionColumnsExist extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionColumnsExist.class; // For Translator
  private String schemaname;
  private String tableName;
  private DatabaseMeta connection;
  private String[] arguments;

  public ActionColumnsExist(String n) {
    super(n, "");
    schemaname = null;
    tableName = null;
    connection = null;
  }

  public ActionColumnsExist() {
    this("");
  }

  public void allocate(int nrFields) {
    arguments = new String[nrFields];
  }

  public Object clone() {
    ActionColumnsExist je = (ActionColumnsExist) super.clone();
    if (arguments != null) {
      int nrFields = arguments.length;
      je.allocate(nrFields);
      System.arraycopy(arguments, 0, je.arguments, 0, nrFields);
    }
    return je;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(200);

    retval.append(super.getXml());

    retval.append("      ").append(XmlHandler.addTagValue("tablename", tableName));
    retval.append("      ").append(XmlHandler.addTagValue("schemaname", schemaname));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue("connection", connection == null ? null : connection.getName()));

    retval.append("      <fields>").append(Const.CR);
    if (arguments != null) {
      for (int i = 0; i < arguments.length; i++) {
        retval.append("        <field>").append(Const.CR);
        retval.append("          ").append(XmlHandler.addTagValue("name", arguments[i]));
        retval.append("        </field>").append(Const.CR);
      }
    }
    retval.append("      </fields>").append(Const.CR);

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      tableName = XmlHandler.getTagValue(entrynode, "tablename");
      schemaname = XmlHandler.getTagValue(entrynode, "schemaname");

      String dbname = XmlHandler.getTagValue(entrynode, "connection");
      connection = DatabaseMeta.loadDatabase(metadataProvider, dbname);

      Node fields = XmlHandler.getSubNode(entrynode, "fields");

      // How many field arguments?
      int nrFields = XmlHandler.countNodes(fields, "field");
      allocate(nrFields);

      // Read them all...
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        arguments[i] = XmlHandler.getTagValue(fnode, "name");
      }

    } catch (HopException e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionColumnsExist.Meta.UnableLoadXml"), e);
    }
  }

  public void setTablename(String tableName) {
    this.tableName = tableName;
  }

  public String getTablename() {
    return tableName;
  }

  public void setSchemaname(String schemaname) {
    this.schemaname = schemaname;
  }

  public String getSchemaname() {
    return schemaname;
  }

  public String[] getArguments() {
    return arguments;
  }

  public void setArguments(String[] arguments) {
    this.arguments = arguments;
  }

  public void setDatabase(DatabaseMeta database) {
    this.connection = database;
  }

  public DatabaseMeta getDatabase() {
    return connection;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);
    result.setNrErrors(1);

    int nrexistcolums = 0;
    int nrnotexistcolums = 0;

    if (Utils.isEmpty(tableName)) {
      logError(BaseMessages.getString(PKG, "ActionColumnsExist.Error.TablenameEmpty"));
      return result;
    }
    if (arguments == null) {
      logError(BaseMessages.getString(PKG, "ActionColumnsExist.Error.ColumnameEmpty"));
      return result;
    }
    if (connection != null) {
      Database db = getNewDatabaseFromMeta();
      try {
        String realSchemaname = resolve(schemaname);
        String realTablename = resolve(tableName);

        db.connect();

        if (db.checkTableExists(realSchemaname, realTablename)) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionColumnsExist.Log.TableExists", realTablename));
          }

          for (int i = 0; i < arguments.length && !parentWorkflow.isStopped(); i++) {
            String realColumnname = resolve(arguments[i]);

            if (db.checkColumnExists(realSchemaname, realTablename, realColumnname)) {
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(
                        PKG, "ActionColumnsExist.Log.ColumnExists", realColumnname, realTablename));
              }
              nrexistcolums++;
            } else {
              logError(
                  BaseMessages.getString(
                      PKG,
                      "ActionColumnsExist.Log.ColumnNotExists",
                      realColumnname,
                      realTablename));
              nrnotexistcolums++;
            }
          }
        } else {
          logError(
              BaseMessages.getString(PKG, "ActionColumnsExist.Log.TableNotExists", realTablename));
        }
      } catch (HopDatabaseException dbe) {
        logError(
            BaseMessages.getString(
                PKG, "ActionColumnsExist.Error.UnexpectedError", dbe.getMessage()));
      } finally {
        if (db != null) {
          try {
            db.disconnect();
          } catch (Exception e) {
            /* Ignore */
          }
        }
      }
    } else {
      logError(BaseMessages.getString(PKG, "ActionColumnsExist.Error.NoDbConnection"));
    }

    result.setEntryNr(nrnotexistcolums);
    result.setNrLinesWritten(nrexistcolums);
    // result is true only if all columns found (PDI-15801)
    if (nrexistcolums == arguments.length) {
      result.setNrErrors(0);
      result.setResult(true);
    }
    return result;
  }

  Database getNewDatabaseFromMeta() {
    return new Database(this, this, connection );
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    return new DatabaseMeta[] {
      connection,
    };
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (connection != null) {
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(connection.getHostname(), ResourceType.SERVER));
      reference
          .getEntries()
          .add(new ResourceEntry(connection.getDatabaseName(), ResourceType.DATABASENAME));
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
