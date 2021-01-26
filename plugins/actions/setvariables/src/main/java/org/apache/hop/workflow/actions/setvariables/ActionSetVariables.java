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

import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopWorkflowException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
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
import org.w3c.dom.Node;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * This defines a 'Set variables' action.
 *
 * @author Samatar Hassan
 * @since 06-05-2007
 */
@Action(
    id = "SET_VARIABLES",
    name = "i18n::ActionSetVariables.Name",
    description = "i18n::ActionSetVariables.Description",
    image = "SetVariables.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/setvariables.html")
public class ActionSetVariables extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSetVariables.class; // For Translator

  public boolean replaceVars;

  public String[] variableName;

  public String[] variableValue;

  public int[] variableType;

  public String filename;

  public int fileVariableType;

  public static final int VARIABLE_TYPE_JVM = 0;
  public static final int VARIABLE_TYPE_CURRENT_WORKFLOW = 1;
  public static final int VARIABLE_TYPE_PARENT_WORKFLOW = 2;
  public static final int VARIABLE_TYPE_ROOT_WORKFLOW = 3;

  public static final String[] variableTypeCode = {
    "JVM", "CURRENT_WORKFLOW", "PARENT_WORKFLOW", "ROOT_WORKFLOW"
  };
  private static final String[] variableTypeDesc = {
    BaseMessages.getString(PKG, "ActionSetVariables.VariableType.JVM"),
    BaseMessages.getString(PKG, "ActionSetVariables.VariableType.CurrentWorkflow"),
    BaseMessages.getString(PKG, "ActionSetVariables.VariableType.ParentWorkflow"),
    BaseMessages.getString(PKG, "ActionSetVariables.VariableType.RootWorkflow"),
  };

  public ActionSetVariables(String n) {
    super(n, "");
    replaceVars = true;
    variableName = null;
    variableValue = null;
  }

  public ActionSetVariables() {
    this("");
  }

  public void allocate(int nrFields) {
    variableName = new String[nrFields];
    variableValue = new String[nrFields];
    variableType = new int[nrFields];
  }

  public Object clone() {
    ActionSetVariables je = (ActionSetVariables) super.clone();
    if (variableName != null) {
      int nrFields = variableName.length;
      je.allocate(nrFields);
      System.arraycopy(variableName, 0, je.variableName, 0, nrFields);
      System.arraycopy(variableValue, 0, je.variableValue, 0, nrFields);
      System.arraycopy(variableType, 0, je.variableType, 0, nrFields);
    }
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);
    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("replacevars", replaceVars));

    retval.append("      ").append(XmlHandler.addTagValue("filename", filename));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue("file_variable_type", getVariableTypeCode(fileVariableType)));

    retval.append("      <fields>").append(Const.CR);
    if (variableName != null) {
      for (int i = 0; i < variableName.length; i++) {
        retval.append("        <field>").append(Const.CR);
        retval
            .append("          ")
            .append(XmlHandler.addTagValue("variable_name", variableName[i]));
        retval
            .append("          ")
            .append(XmlHandler.addTagValue("variable_value", variableValue[i]));
        retval
            .append("          ")
            .append(XmlHandler.addTagValue("variable_type", getVariableTypeCode(variableType[i])));
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
      replaceVars = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "replacevars"));

      filename = XmlHandler.getTagValue(entrynode, "filename");
      fileVariableType = getVariableType(XmlHandler.getTagValue(entrynode, "file_variable_type"));

      Node fields = XmlHandler.getSubNode(entrynode, "fields");
      // How many field variableName?
      int nrFields = XmlHandler.countNodes(fields, "field");
      allocate(nrFields);

      // Read them all...
      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);

        variableName[i] = XmlHandler.getTagValue(fnode, "variable_name");
        variableValue[i] = XmlHandler.getTagValue(fnode, "variable_value");
        variableType[i] = getVariableType(XmlHandler.getTagValue(fnode, "variable_type"));
      }
    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionSetVariables.Meta.UnableLoadXML", xe.getMessage()),
          xe);
    }
  }

  public Result execute(Result result, int nr) throws HopException {
    result.setResult(true);
    result.setNrErrors(0);
    try {

      List<String> variables = new ArrayList<>();
      List<String> variableValues = new ArrayList<>();
      List<Integer> variableTypes = new ArrayList<>();

      String realFilename = resolve(filename);
      if (!Utils.isEmpty(realFilename)) {
        try (InputStream is = HopVfs.getInputStream(realFilename);
            // for UTF8 properties files
            InputStreamReader isr = new InputStreamReader(is, "UTF-8");
            BufferedReader reader = new BufferedReader(isr); ) {
          Properties properties = new Properties();
          properties.load(reader);
          for (Object key : properties.keySet()) {
            variables.add((String) key);
            variableValues.add((String) properties.get(key));
            variableTypes.add(fileVariableType);
          }
        } catch (Exception e) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "ActionSetVariables.Error.UnableReadPropertiesFile", realFilename));
        }
      }

      if (variableName != null) {
        for (int i = 0; i < variableName.length; i++) {
          variables.add(variableName[i]);
          variableValues.add(variableValue[i]);
          variableTypes.add(variableType[i]);
        }
      }

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

      for (int i = 0; i < variables.size(); i++) {
        String varname = variables.get(i);
        String value = variableValues.get(i);
        int type = variableTypes.get(i);

        if (replaceVars) {
          varname = resolve(varname);
          value = resolve(value);
        }

        // OK, where do we set this value...
        switch (type) {
          case VARIABLE_TYPE_JVM:
            if (value != null) {
              System.setProperty(varname, value);
            } else {
              System.clearProperty(varname); // PDI-17536
            }
            setVariable(varname, value);
            IWorkflowEngine<WorkflowMeta> parentWorkflowTraverse = parentWorkflow;
            while (parentWorkflowTraverse != null) {
              parentWorkflowTraverse.setVariable(varname, value);
              parentWorkflowTraverse = parentWorkflowTraverse.getParentWorkflow();
            }
            break;

          case VARIABLE_TYPE_ROOT_WORKFLOW:
            // set variable in this action
            setVariable(varname, value);
            IWorkflowEngine<WorkflowMeta> rootWorkflow = parentWorkflow;
            while (rootWorkflow != null) {
              rootWorkflow.setVariable(varname, value);
              rootWorkflow = rootWorkflow.getParentWorkflow();
            }
            break;

          case VARIABLE_TYPE_CURRENT_WORKFLOW:
            setVariable(varname, value);

            if (parentWorkflow != null) {
              String parameterValue = parentWorkflow.getParameterValue(varname);
              // if not a parameter, set the value
              if (parameterValue == null) {
                setEntryTransformSetVariable(varname, value);
              } else {
                // if parameter, save the initial parameter value for use in reset/clear variables
                // in future calls
                if (parameterValue != null
                    && parameterValue != value
                    && !entryTransformSetVariablesMap.containsKey(varname)) {
                  setEntryTransformSetVariable(varname, parameterValue);
                }
              }
              parentWorkflow.setVariable(varname, value);

            } else {
              throw new HopWorkflowException(
                  BaseMessages.getString(
                      PKG, "ActionSetVariables.Error.UnableSetVariableCurrentWorkflow", varname));
            }
            break;

          case VARIABLE_TYPE_PARENT_WORKFLOW:
            setVariable(varname, value);

            if (parentWorkflow != null) {
              parentWorkflow.setVariable(varname, value);
              IWorkflowEngine<WorkflowMeta> gpWorkflow = parentWorkflow.getParentWorkflow();
              if (gpWorkflow != null) {
                gpWorkflow.setVariable(varname, value);
              } else {
                throw new HopWorkflowException(
                    BaseMessages.getString(
                        PKG, "ActionSetVariables.Error.UnableSetVariableParentWorkflow", varname));
              }
            } else {
              throw new HopWorkflowException(
                  BaseMessages.getString(
                      PKG, "ActionSetVariables.Error.UnableSetVariableCurrentWorkflow", varname));
            }
            break;

          default:
            break;
        }

        // ok we can process this line
        if (log.isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionSetVariables.Log.SetVariableToValue", varname, value));
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

  @Override public boolean isEvaluation() {
    return true;
  }

  public boolean isReplaceVars() {
    return replaceVars;
  }

  public String[] getVariableValue() {
    return variableValue;
  }

  /** @param fieldValue The fieldValue to set. */
  public void setVariableName(String[] fieldValue) {
    this.variableName = fieldValue;
  }

  /**
   * @return Returns the local variable flag: true if this variable is only valid in the parents
   *     workflow.
   */
  public int[] getVariableType() {
    return variableType;
  }

  /**
   * @param variableType The variable type, see also VARIABLE_TYPE_...
   * @return the variable type code for this variable type
   */
  public static final String getVariableTypeCode(int variableType) {
    return variableTypeCode[variableType];
  }

  /**
   * @param variableType The variable type, see also VARIABLE_TYPE_...
   * @return the variable type description for this variable type
   */
  public static final String getVariableTypeDescription(int variableType) {
    return variableTypeDesc[variableType];
  }

  /**
   * @param variableType The code or description of the variable type
   * @return The variable type
   */
  public static final int getVariableType(String variableType) {
    for (int i = 0; i < variableTypeCode.length; i++) {
      if (variableTypeCode[i].equalsIgnoreCase(variableType)) {
        return i;
      }
    }
    for (int i = 0; i < variableTypeDesc.length; i++) {
      if (variableTypeDesc[i].equalsIgnoreCase(variableType)) {
        return i;
      }
    }
    return VARIABLE_TYPE_JVM;
  }

  /** @param localVariable The localVariable to set. */
  public void setVariableType(int[] localVariable) {
    this.variableType = localVariable;
  }

  public static final String[] getVariableTypeDescriptions() {
    return variableTypeDesc;
  }

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

    if (res == false) {
      return;
    }

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notNullValidator(), ActionValidatorUtils.fileExistsValidator());

    for (int i = 0; i < variableName.length; i++) {
      ActionValidatorUtils.andValidator().validate(this, "variableName[" + i + "]", remarks, ctx);
    }
  }

  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (variableName != null) {
      ResourceReference reference = null;
      for (int i = 0; i < variableName.length; i++) {
        String filename = resolve(variableName[i]);
        if (reference == null) {
          reference = new ResourceReference(this);
          references.add(reference);
        }
        reference.getEntries().add(new ResourceEntry(filename, ResourceType.FILE));
      }
    }
    return references;
  }

  /** @return the filename */
  public String getFilename() {
    return filename;
  }

  /** @param filename the filename to set */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  /** @return the fileVariableType */
  public int getFileVariableType() {
    return fileVariableType;
  }

  /** @param fileVariableType the fileVariableType to set */
  public void setFileVariableType(int fileVariableType) {
    this.fileVariableType = fileVariableType;
  }
}
