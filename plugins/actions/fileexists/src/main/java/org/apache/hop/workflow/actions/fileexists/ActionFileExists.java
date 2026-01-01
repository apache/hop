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

package org.apache.hop.workflow.actions.fileexists;

import java.util.List;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/**
 * The File Exists action verifies that a specified file exists on the server on which Hop is
 * running.
 */
@Action(
    id = "FILE_EXISTS",
    name = "i18n::ActionFileExists.Name",
    description = "i18n::ActionFileExists.Description",
    image = "FileExists.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Conditions",
    keywords = "i18n::ActionFileExists.keyword",
    documentationUrl = "/workflow/actions/fileexists.html")
public class ActionFileExists extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFileExists.class;

  @HopMetadataProperty(key = "filename")
  private String filename;

  public ActionFileExists(String n) {
    super(n, "");
    filename = null;
  }

  public ActionFileExists() {
    this("");
  }

  public ActionFileExists(ActionFileExists meta) {
    super(meta.getName(), meta.getDescription(), meta.getPluginId());
    this.filename = meta.filename;
  }

  @Override
  public Object clone() {
    return new ActionFileExists(this);
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  @Override
  public String getFilename() {
    return filename;
  }

  @Override
  public String getRealFilename() {
    return resolve(getFilename());
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);
    result.setNrErrors(0);

    if (filename != null) {

      String realFilename = getRealFilename();
      try {
        FileObject file = HopVfs.getFileObject(realFilename, getVariables());
        if (file.exists() && file.isReadable()) {
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionFileExists.File_Exists", realFilename));
          }
          result.setResult(true);
        } else {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionFileExists.File_Does_Not_Exist", realFilename));
          }
        }
      } catch (Exception e) {
        result.setNrErrors(1);
        logError(
            BaseMessages.getString(PKG, "ActionFileExists.ERROR_0004_IO_Exception", e.getMessage()),
            e);
      }
    } else {
      result.setNrErrors(1);
      logError(BaseMessages.getString(PKG, "ActionFileExists.ERROR_0005_No_Filename_Defined"));
    }

    return result;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (!Utils.isEmpty(filename)) {
      String realFileName = resolve(filename);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realFileName, ResourceType.FILE));
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
            "filename",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }

  /**
   * Exports the object to a flat-file system, adding content with filename keys to a set of
   * definitions. The supplied resource naming interface allows the object to name appropriately
   * without worrying about those parts of the implementation specific details.
   *
   * @param variables The variable variables to resolve (environment) variables with.
   * @param definitions The map containing the filenames and content
   * @param namingInterface The resource naming interface allows the object to be named
   *     appropriately
   * @param metadataProvider the metadataProvider to load external metadata from
   * @return The filename for this object. (also contained in the definitions map)
   * @throws HopException in case something goes wrong during the export
   */
  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming namingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      if (!Utils.isEmpty(filename)) {
        // From : ${FOLDER}/../foo/bar.csv
        // To : /home/matt/test/files/foo/bar.csv
        //
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(filename), getVariables());

        // If the file doesn't exist, forget about this effort too!
        //
        if (fileObject.exists()) {
          // Convert to an absolute path...
          //
          filename = namingInterface.nameResource(fileObject, variables, true);

          return filename;
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
