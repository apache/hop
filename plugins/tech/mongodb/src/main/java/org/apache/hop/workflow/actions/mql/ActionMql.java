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

package org.apache.hop.workflow.actions.mql;

import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.annotations.ActionTransformType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.bson.Document;

/** This defines a MongoDB QL action. */
@Action(
    id = "MQL",
    name = "i18n::ActionMQL.Name",
    description = "i18n::ActionMQL.Description",
    image = "mql.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Scripting",
    keywords = "i18n::ActionMQL.keyword",
    documentationUrl = "/workflow/actions/mql.html",
    actionTransformTypes = {ActionTransformType.NOSQL})
public class ActionMql extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionMql.class;

  @HopMetadataProperty(key = "mql")
  private String mql;

  @HopMetadataProperty(key = "connection")
  private String connection;

  @HopMetadataProperty(key = "useExternalFile")
  private boolean useExternalFile = false;

  @HopMetadataProperty(key = "commandFile")
  private String commandFile;

  @HopMetadataProperty(key = "useVariableSubstitution")
  private boolean useVariableSubstitution = true;

  public ActionMql(String n) {
    super(n, "");
    mql = null;
    connection = null;
  }

  public ActionMql() {
    this("");
  }

  public void setMql(String mql) {
    this.mql = mql;
  }

  public String getMql() {
    return mql;
  }

  public void setConnection(String connection) {
    this.connection = connection;
  }

  public String getConnection() {
    return connection;
  }

  public boolean isUseExternalFile() {
    return useExternalFile;
  }

  public void setUseExternalFile(boolean useExternalFile) {
    this.useExternalFile = useExternalFile;
  }

  public String getCommandFile() {
    return commandFile;
  }

  public void setCommandFile(String commandFile) {
    this.commandFile = commandFile;
  }

  public boolean isUseVariableSubstitution() {
    return useVariableSubstitution;
  }

  public void setUseVariableSubstitution(boolean subst) {
    useVariableSubstitution = subst;
  }

  @Override
  public Result execute(Result result, int nr) {

    result.setNrErrors(0);

    MongoClientWrapper wrapper = null;

    try {
      String connectionName = resolve(connection);
      if (Utils.isEmpty(connectionName)) {
        throw new HopException(BaseMessages.getString(PKG, "ActionMQL.NoDatabaseConnection"));
      }

      MongoDbConnection mongoConnection =
          getMetadataProvider().getSerializer(MongoDbConnection.class).load(connectionName);

      if (mongoConnection == null) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionMQL.MongoDbConnectionNotFound") + connectionName);
      }

      wrapper = mongoConnection.createWrapper(this, getLogChannel());
      String dbName = mongoConnection.getDbName();

      String mqlToExecute = "";
      // External file
      if (useExternalFile) {
        if (Utils.isEmpty(commandFile)) {
          throw new HopException(BaseMessages.getString(PKG, "ActionMQL.NoFileSpecified"));
        }
        String realPath = resolve(commandFile);
        mqlToExecute =
            java.nio.file.Files.readString(
                java.nio.file.Paths.get(realPath), java.nio.charset.StandardCharsets.UTF_8);
      } else {
        mqlToExecute = mql;
      }

      // Variable substitution
      if (useVariableSubstitution) {
        mqlToExecute = resolve(mqlToExecute);
      }

      final List<Document> commands = parseCommands(mqlToExecute);

      wrapper.perform(
          dbName,
          db -> {
            for (int i = 0; i < commands.size(); i++) {
              Document reply = db.runCommand(commands.get(i));
              checkReply(reply, i + 1, commands.size());
            }
            return null;
          });

    } catch (Exception e) {
      result.setNrErrors(1);

      logError(BaseMessages.getString(PKG, "ActionMQL.ErrorRunningMQL") + " : " + e.getMessage());

    } finally {
      if (wrapper != null) {
        try {
          wrapper.dispose();
        } catch (Exception e) {

        }
      }
    }

    result.setResult(result.getNrErrors() == 0);
    return result;
  }

  /**
   * Parse the commands to run. A JSON array holds one command per element. Anything else is read as
   * a list of comma-separated documents, so that text after the first document is either run or
   * reported as invalid JSON, never silently ignored.
   *
   * @param mqlToExecute the JSON text
   * @return the commands, in the order they are to be run
   * @throws HopException when there is no command
   */
  static List<Document> parseCommands(String mqlToExecute) throws HopException {
    String trimmed = mqlToExecute == null ? "" : mqlToExecute.trim();
    if (trimmed.isEmpty()) {
      throw new HopException(BaseMessages.getString(PKG, "ActionMQL.NoCommandFound"));
    }
    String array = trimmed.startsWith("[") ? trimmed : "[" + trimmed + "]";
    Document wrapper = Document.parse("{ \"array\": " + array + " }");
    if (wrapper.size() != 1) {
      // For example: [ {...} ], "x": 1
      throw new HopException(BaseMessages.getString(PKG, "ActionMQL.TextAfterCommands"));
    }
    List<Document> commands = wrapper.getList("array", Document.class);
    if (commands.isEmpty()) {
      throw new HopException(BaseMessages.getString(PKG, "ActionMQL.NoCommandFound"));
    }
    return commands;
  }

  /**
   * A command that MongoDB accepts can still fail to write documents. Those errors are in the reply
   * (writeErrors, writeConcernError) while ok is 1, so runCommand() does not throw.
   *
   * @param reply the reply of the command
   * @param commandNr the number of the command (1-based)
   * @param nrCommands the number of commands
   * @throws MongoDbException when the reply holds write errors or a write concern error
   */
  static void checkReply(Document reply, int commandNr, int nrCommands) throws MongoDbException {
    if (reply == null) {
      return;
    }
    List<Document> writeErrors = reply.getList("writeErrors", Document.class);
    if (writeErrors != null && !writeErrors.isEmpty()) {
      Document first = writeErrors.get(0);
      throw new MongoDbException(
          BaseMessages.getString(
              PKG,
              "ActionMQL.WriteErrors",
              Integer.toString(commandNr),
              Integer.toString(nrCommands),
              Integer.toString(writeErrors.size()),
              String.valueOf(first.get("code")),
              first.getString("errmsg")));
    }
    Document writeConcernError = reply.get("writeConcernError", Document.class);
    if (writeConcernError != null) {
      throw new MongoDbException(
          BaseMessages.getString(
              PKG,
              "ActionMQL.WriteConcernError",
              Integer.toString(commandNr),
              Integer.toString(nrCommands),
              String.valueOf(writeConcernError.get("code")),
              writeConcernError.getString("errmsg")));
    }
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
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {

    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);

    if (!Utils.isEmpty(connection)) {
      ResourceReference reference = new ResourceReference(this);

      reference.getEntries().add(new ResourceEntry(connection, ResourceType.SERVER));

      references.add(reference);
    }

    if (useExternalFile && !Utils.isEmpty(commandFile)) {
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(commandFile, ResourceType.FILE));
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
            "MQL",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}
