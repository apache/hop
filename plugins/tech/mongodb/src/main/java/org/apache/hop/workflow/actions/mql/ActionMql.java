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

import java.util.ArrayList;
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
import org.bson.BsonType;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.json.JsonParseException;
import org.bson.json.JsonReader;

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
  private static final String CONST_ERRMSG = "errmsg";

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

  @HopMetadataProperty(key = "failOnWriteErrors")
  private boolean failOnWriteErrors = false;

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

  public boolean isFailOnWriteErrors() {
    return failOnWriteErrors;
  }

  public void setFailOnWriteErrors(boolean failOnWriteErrors) {
    this.failOnWriteErrors = failOnWriteErrors;
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
              try {
                checkReply(reply, i + 1, commands.size());
              } catch (MongoDbException e) {
                if (failOnWriteErrors) {
                  throw e;
                }
                logBasic(
                    BaseMessages.getString(PKG, "ActionMQL.WriteErrorsIgnored", e.getMessage()));
              }
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
   * Parse the commands to run: one JSON document, or a JSON array with one document per command.
   * Semicolons and whitespace at the end are allowed. Any other text after the document or the
   * array is an error: it is neither run nor silently ignored.
   *
   * <p>The text is parsed as it is, so a position in a JSON error matches the text in the dialog.
   *
   * @param mqlToExecute the JSON text
   * @return the commands, in the order they are to be run
   * @throws HopException when there is no command or the text is not valid
   */
  static List<Document> parseCommands(String mqlToExecute) throws HopException {
    String text = mqlToExecute == null ? "" : mqlToExecute;
    int end = text.length();
    while (end > 0
        && (Character.isWhitespace(text.charAt(end - 1)) || text.charAt(end - 1) == ';')) {
      end--;
    }
    text = text.substring(0, end);
    if (text.isBlank()) {
      throw new HopException(BaseMessages.getString(PKG, "ActionMQL.NoCommandFound"));
    }

    List<Document> commands = new ArrayList<>();
    DocumentCodec codec = new DocumentCodec();
    DecoderContext context = DecoderContext.builder().build();
    try (JsonReader reader = new JsonReader(text)) {
      BsonType type = reader.readBsonType();
      if (type == BsonType.ARRAY) {
        reader.readStartArray();
        while ((type = reader.readBsonType()) != BsonType.END_OF_DOCUMENT) {
          if (type != BsonType.DOCUMENT) {
            throw new HopException(
                BaseMessages.getString(
                    PKG,
                    "ActionMQL.NotADocument",
                    Integer.toString(commands.size() + 1),
                    type.name()));
          }
          commands.add(codec.decode(reader, context));
        }
        reader.readEndArray();
      } else if (type == BsonType.DOCUMENT) {
        commands.add(codec.decode(reader, context));
      } else {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionMQL.NotADocumentOrArray", type.name()));
      }

      // Nothing may follow the document or the array, for example: { ... }, { ... }
      //
      boolean textAfter;
      try {
        textAfter = reader.readBsonType() != BsonType.END_OF_DOCUMENT;
      } catch (JsonParseException e) {
        textAfter = true;
      }
      if (textAfter) {
        throw new HopException(BaseMessages.getString(PKG, "ActionMQL.TextAfterCommands"));
      }
    } catch (JsonParseException e) {
      throw new HopException(BaseMessages.getString(PKG, "ActionMQL.InvalidJson", e.getMessage()));
    }
    if (commands.isEmpty()) {
      throw new HopException(BaseMessages.getString(PKG, "ActionMQL.NoCommandFound"));
    }
    return commands;
  }

  /**
   * A command that MongoDB accepts can still fail to write documents. Those errors are in the reply
   * (writeErrors, writeConcernError, or nErrors for bulkWrite) while ok is 1, so runCommand() does
   * not throw.
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
              first.getString(CONST_ERRMSG)));
    }
    // The bulkWrite command counts its errors in nErrors, the errors are in its cursor
    //
    if (reply.get("nErrors") instanceof Number nErrors && nErrors.longValue() > 0) {
      Document first = firstBulkWriteError(reply);
      throw new MongoDbException(
          BaseMessages.getString(
              PKG,
              "ActionMQL.WriteErrors",
              Integer.toString(commandNr),
              Integer.toString(nrCommands),
              nErrors.toString(),
              first == null ? "?" : String.valueOf(first.get("code")),
              first == null ? "" : first.getString(CONST_ERRMSG)));
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
              writeConcernError.getString(CONST_ERRMSG)));
    }
  }

  /** The first failed operation in the first batch of a bulkWrite reply, null if it isn't there. */
  private static Document firstBulkWriteError(Document reply) {
    if (!(reply.get("cursor") instanceof Document cursor)
        || !(cursor.get("firstBatch") instanceof List<?> batch)) {
      return null;
    }
    for (Object item : batch) {
      if (item instanceof Document operation
          && operation.get("ok") instanceof Number ok
          && ok.doubleValue() != 1.0) {
        return operation;
      }
    }
    return null;
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
