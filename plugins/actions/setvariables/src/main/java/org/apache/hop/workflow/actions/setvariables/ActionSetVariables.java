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

package org.apache.hop.workflow.actions.setvariables;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopWorkflowException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This defines a 'Set variables' action. */
@Action(
    id = "SET_VARIABLES",
    name = "i18n::ActionSetVariables.Name",
    description = "i18n::ActionSetVariables.Description",
    image = "SetVariables.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    keywords = "i18n::ActionSetVariables.keyword",
    documentationUrl = "/workflow/actions/setvariables.html")
public class ActionSetVariables extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSetVariables.class;

  @HopMetadataProperty(key = "replacevars")
  private boolean replaceVars;

  @HopMetadataProperty(groupKey = "fields", key = "field")
  private List<VariableDefinition> variableDefinitions;

  @HopMetadataProperty(key = "filename")
  private String filename;

  @HopMetadataProperty(key = "file_variable_type")
  private VariableType fileVariableType;

  public enum VariableType implements IEnumHasCodeAndDescription {
    JVM(BaseMessages.getString(PKG, "ActionSetVariables.VariableType.JVM")),
    CURRENT_WORKFLOW(
        BaseMessages.getString(PKG, "ActionSetVariables.VariableType.CurrentWorkflow")),
    PARENT_WORKFLOW(BaseMessages.getString(PKG, "ActionSetVariables.VariableType.ParentWorkflow")),
    ROOT_WORKFLOW(BaseMessages.getString(PKG, "ActionSetVariables.VariableType.RootWorkflow"));

    private final String description;

    VariableType(String description) {
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(VariableType.class);
    }

    public static VariableType lookupDescription(final String description) {
      return IEnumHasCodeAndDescription.lookupDescription(VariableType.class, description, JVM);
    }

    public static VariableType lookupCode(final String code) {
      return IEnumHasCode.lookupCode(VariableType.class, code, JVM);
    }

    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getDescription() {
      return description;
    }
  }

  public static final class VariableDefinition {
    public VariableDefinition() {
      super();
    }

    public VariableDefinition(String name, String value, VariableType type) {
      super();
      this.name = name;
      this.value = value;
      this.type = type;
    }

    @HopMetadataProperty(key = "variable_name")
    private String name;

    @HopMetadataProperty(key = "variable_value")
    private String value;

    @HopMetadataProperty(key = "variable_type")
    private VariableType type;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public VariableType getType() {
      return type;
    }

    public void setType(VariableType type) {
      this.type = type;
    }
  }

  public ActionSetVariables(String n) {
    super(n, "");
    replaceVars = true;
    fileVariableType = VariableType.CURRENT_WORKFLOW;
    variableDefinitions = new ArrayList<>();
  }

  public ActionSetVariables() {
    this("");
  }

  public ActionSetVariables(ActionSetVariables other) {
    super(other.getName(), other.getDescription(), other.getPluginId());
    this.filename = other.getFilename();
    this.fileVariableType = other.getFileVariableType();
    this.replaceVars = other.isReplaceVars();
    this.variableDefinitions = other.getVariableDefinitions();
  }

  @Override
  public Object clone() {
    return new ActionSetVariables(this);
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    result.setResult(true);
    result.setNrErrors(0);
    try {

      List<VariableDefinition> definitions = new ArrayList<>();

      String realFilename = resolve(filename);
      if (!Utils.isEmpty(realFilename)) {
        try (InputStream is = HopVfs.getInputStream(realFilename);
            // for UTF8 properties files
            InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(isr); ) {
          Properties properties = new Properties();
          properties.load(reader);
          for (Object key : properties.keySet()) {
            definitions.add(
                new VariableDefinition(
                    (String) key, (String) properties.get(key), fileVariableType));
          }
        } catch (Exception e) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "ActionSetVariables.Error.UnableReadPropertiesFile", realFilename));
        }
      }

      definitions.addAll(variableDefinitions);

      // if parentWorkflow exists - clear/reset all entrySetVariables before applying the actual
      // ones
      if (parentWorkflow != null) {
        for (String key : getEntryTransformSetVariablesMap().keySet()) {
          String parameterValue = parentWorkflow.getParameterValue(key);
          // if variable is not a namedParameter then it is a EntryTransformSetVariable - reset
          // value to ""
          if (parameterValue == null) {
            parentWorkflow.setVariable(key, "");
            setVariable(key, "");
          } else {
            // if it is a parameter, then get the initial saved value of parent -  saved in
            // entryTransformSetVariables Map
            parentWorkflow.setVariable(key, getEntryTransformSetVariable(key));
            setVariable(key, getEntryTransformSetVariable(key));
          }
        }
      }

      for (VariableDefinition definition : definitions) {
        String name = definition.getName();
        String value = definition.getValue();

        if (replaceVars) {
          name = resolve(name);
          value = resolve(value);
        }

        // OK, where do we set this value...
        switch (definition.getType()) {
          case JVM:
            if (value != null) {
              System.setProperty(name, value);
            } else {
              System.clearProperty(name);
            }
            setVariable(name, value);
            IWorkflowEngine<WorkflowMeta> parentWorkflowTraverse = parentWorkflow;
            while (parentWorkflowTraverse != null) {
              parentWorkflowTraverse.setVariable(name, value);
              parentWorkflowTraverse = parentWorkflowTraverse.getParentWorkflow();
            }
            break;

          case ROOT_WORKFLOW:
            // set variable in this action
            setVariable(name, value);
            IWorkflowEngine<WorkflowMeta> rootWorkflow = parentWorkflow;
            while (rootWorkflow != null) {
              rootWorkflow.setVariable(name, value);
              rootWorkflow = rootWorkflow.getParentWorkflow();
            }
            break;

          case CURRENT_WORKFLOW:
            setVariable(name, value);

            if (parentWorkflow != null) {
              String parameterValue = parentWorkflow.getParameterValue(name);
              // if not a parameter, set the value
              if (parameterValue == null) {
                setEntryTransformSetVariable(name, value);
              } else {
                // if parameter, save the initial parameter value for use in reset/clear variables
                // in future calls
                if (parameterValue != null
                    && !parameterValue.equals(value)
                    && !entryTransformSetVariablesMap.containsKey(name)) {
                  setEntryTransformSetVariable(name, parameterValue);
                }
              }
              parentWorkflow.setVariable(name, value);

            } else {
              throw new HopWorkflowException(
                  BaseMessages.getString(
                      PKG, "ActionSetVariables.Error.UnableSetVariableCurrentWorkflow", name));
            }
            break;

          case PARENT_WORKFLOW:
            setVariable(name, value);

            if (parentWorkflow != null) {
              parentWorkflow.setVariable(name, value);
              IWorkflowEngine<WorkflowMeta> gpWorkflow = parentWorkflow.getParentWorkflow();
              if (gpWorkflow != null) {
                gpWorkflow.setVariable(name, value);
              } else {
                throw new HopWorkflowException(
                    BaseMessages.getString(
                        PKG, "ActionSetVariables.Error.UnableSetVariableParentWorkflow", name));
              }
            } else {
              throw new HopWorkflowException(
                  BaseMessages.getString(
                      PKG, "ActionSetVariables.Error.UnableSetVariableCurrentWorkflow", name));
            }
            break;

          default:
            break;
        }

        // ok we can process this line
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionSetVariables.Log.SetVariableToValue", name, value));
        }
      }
    } catch (Exception e) {
      result.setResult(false);
      result.setNrErrors(1);

      logError(BaseMessages.getString(PKG, "ActionSetVariables.UnExcpectedError", e.getMessage()));
    }

    return result;
  }

  public void setReplaceVars(boolean replaceVars) {
    this.replaceVars = replaceVars;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  public boolean isReplaceVars() {
    return replaceVars;
  }

  public List<VariableDefinition> getVariableDefinitions() {
    return variableDefinitions;
  }

  public void setVariableDefinitions(List<VariableDefinition> variableDefinitions) {
    this.variableDefinitions = variableDefinitions;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    boolean res =
        ActionValidatorUtils.andValidator()
            .validate(
                this,
                "variableName",
                remarks,
                AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));

    if (!res) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);

    String realFilename = resolve(this.filename);
    if (!Utils.isEmpty(realFilename)) {
      ResourceReference reference = new ResourceReference(this);
      references.add(reference);
      reference.getEntries().add(new ResourceEntry(realFilename, ResourceType.FILE));
    }

    return references;
  }

  /**
   * @return the filename
   */
  @Override
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename the filename to set
   */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /**
   * @return the fileVariableType
   */
  public VariableType getFileVariableType() {
    return fileVariableType;
  }

  /**
   * @param scope the fileVariableType to set
   */
  public void setFileVariableType(VariableType scope) {
    this.fileVariableType = scope;
  }
}
