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
 *
 */

package org.apache.hop.pipeline.transforms.execinfo;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "ExecInfo",
    image = "ui/images/execution.svg",
    name = "i18n::ExecInfo.Name",
    description = "i18n::ExecInfo.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::ExecInfo.Keyword",
    documentationUrl = "/pipeline/transforms/execinfo.html")
public class ExecInfoMeta extends BaseTransformMeta<ExecInfo, ExecInfoData> {
  private static final Class<?> PKG = ExecInfoMeta.class;

  @HopMetadataProperty private String location;

  @HopMetadataProperty private OperationType operationType;

  @HopMetadataProperty private String idFieldName;

  @HopMetadataProperty private String parentIdFieldName;

  @HopMetadataProperty private String typeFieldName;

  @HopMetadataProperty private String nameFieldName;

  @HopMetadataProperty private String includeChildrenFieldName;

  @HopMetadataProperty private String limitFieldName;

  public ExecInfoMeta() {
    super(); // allocate BaseTransformMeta
  }

  public ExecInfoMeta(ExecInfoMeta m) {
    this.location = m.location;
    this.operationType = m.operationType;
    this.idFieldName = m.idFieldName;
    this.parentIdFieldName = m.parentIdFieldName;
    this.typeFieldName = m.typeFieldName;
    this.nameFieldName = m.nameFieldName;
    this.includeChildrenFieldName = m.includeChildrenFieldName;
    this.limitFieldName = m.limitFieldName;
  }

  @Override
  public ExecInfoMeta clone() {
    return new ExecInfoMeta(this);
  }

  @Override
  public void setDefault() {
    operationType = OperationType.GetExecutionIds;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    if (operationType == null) {
      return;
    }

    switch (operationType) {
      case GetExecutionIds, FindChildIds, FindParentId:
        // The output is a single execution ID field
        //
        inputRowMeta.addRowMeta(new RowMetaBuilder().addString("id").build());
        return;
      case GetExecutionAndState, FindPreviousSuccessfulExecution, FindLastExecution, FindExecutions:
        inputRowMeta.addRowMeta(
            new RowMetaBuilder()
                .addString("executionId")
                .addString("parentId")
                .addString("name")
                .addString("executionType")
                .addString("filename")
                .addString("executorXml")
                .addString("metadataJson")
                .addDate("registrationDate")
                .addDate("executionStartDate")
                .addString("runConfigurationName")
                .addString("logLevel")
                .addDate("updateTime")
                .addString("loggingText")
                .addBoolean("failed")
                .addString("statusDescription")
                .addDate("executionEndDate")
                .build());
        return;
      case DeleteExecution:
        inputRowMeta.addRowMeta(new RowMetaBuilder().addBoolean("deleted").build());
        return;
      case GetExecutionData:
        inputRowMeta.addRowMeta(
            new RowMetaBuilder()
                .addString("parentId")
                .addString("ownerId")
                .addDate("collectionDate")
                .addBoolean("finished")
                // We also the collected data sets
                .addString("setKey")
                .addString("name")
                .addString("copyNr")
                .addString("description")
                .addString("logChannelId")
                .addString("setRowMetaJson")
                .addInteger("setRowNr")
                .addString("setRowDataJson")
                .build());
        return;
      default:
        throw new HopTransformException("Unknown operation type " + operationType.description);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    if (location == null) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecInfoMeta.CheckResult.LocationMissing"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExecInfoMeta.CheckResult.LocationOK"),
              transformMeta));
    }

    if (operationType == null) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ExecInfoMeta.CheckResult.OperationTypeMissing"),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ExecInfoMeta.CheckResult.OperationTypeOK", operationType.getDescription()),
              transformMeta));
    }

    // Does the input require an ID as input?
    //
    if (operationType.isAcceptingExecutionId()) {
      if (StringUtils.isEmpty(idFieldName)) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "ExecInfoMeta.CheckResult.OperationRequiresId",
                    operationType.getDescription()),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "ExecInfoMeta.CheckResult.OperationHasId", operationType.getDescription()),
                transformMeta));
      }
    }

    // Does the input require a parent ID as input?
    //
    if (operationType.isAcceptingParentExecutionId()) {
      if (StringUtils.isEmpty(parentIdFieldName)) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "ExecInfoMeta.CheckResult.OperationRequiresParentId",
                    operationType.getDescription()),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "ExecInfoMeta.CheckResult.OperationHasParentId",
                    operationType.getDescription()),
                transformMeta));
      }
    }

    // Does the input require a name as input?
    //
    if (operationType.isAcceptingName()) {
      if (StringUtils.isEmpty(nameFieldName)) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "ExecInfoMeta.CheckResult.OperationRequiresName",
                    operationType.getDescription()),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "ExecInfoMeta.CheckResult.OperationHasName",
                    operationType.getDescription()),
                transformMeta));
      }
    }

    // Does the input require an "Includes Children" field as input?
    //
    if (operationType.isAcceptingIncludeChildren()) {
      if (StringUtils.isEmpty(includeChildrenFieldName)) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "ExecInfoMeta.CheckResult.OperationRequiresIncludesChildren",
                    operationType.getDescription()),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "ExecInfoMeta.CheckResult.OperationHasIncludesChildren",
                    operationType.getDescription()),
                transformMeta));
      }
    }

    // Does the input require an "Includes Children" field as input?
    //
    if (operationType.isAcceptingLimit()) {
      if (StringUtils.isEmpty(limitFieldName)) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "ExecInfoMeta.CheckResult.OperationRequiresLimit",
                    operationType.getDescription()),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "ExecInfoMeta.CheckResult.OperationHasLimit",
                    operationType.getDescription()),
                transformMeta));
      }
    }
  }

  @SuppressWarnings("java:S115")
  public static enum OperationType {
    GetExecutionIds(
        BaseMessages.getString(PKG, "ExecInfoMeta.OperationType.Description.GetExecutionIds"),
        false,
        false,
        false,
        false,
        true,
        true),
    GetExecutionAndState(
        BaseMessages.getString(PKG, "ExecInfoMeta.OperationType.Description.GetExecutionAndState"),
        true,
        false,
        false,
        false,
        false,
        false),
    FindExecutions(
        BaseMessages.getString(PKG, "ExecInfoMeta.OperationType.Description.FindExecutions"),
        false,
        true,
        false,
        false,
        false,
        false),
    DeleteExecution(
        BaseMessages.getString(PKG, "ExecInfoMeta.OperationType.Description.DeleteExecution"),
        true,
        false,
        false,
        false,
        false,
        false),
    FindPreviousSuccessfulExecution(
        BaseMessages.getString(
            PKG, "ExecInfoMeta.OperationType.Description.FindPreviousSuccessfulExecution"),
        false,
        false,
        true,
        true,
        false,
        false),
    GetExecutionData(
        BaseMessages.getString(PKG, "ExecInfoMeta.OperationType.Description.GetExecutionData"),
        true,
        true,
        false,
        false,
        false,
        false),
    FindLastExecution(
        BaseMessages.getString(PKG, "ExecInfoMeta.OperationType.Description.FindLastExecution"),
        false,
        false,
        true,
        true,
        false,
        false),
    FindChildIds(
        BaseMessages.getString(PKG, "ExecInfoMeta.OperationType.Description.FindChildIds"),
        false,
        true,
        true,
        false,
        false,
        false),
    FindParentId(
        BaseMessages.getString(PKG, "ExecInfoMeta.OperationType.Description.FindParentId"),
        true,
        false,
        false,
        false,
        false,
        false),
    ;
    private final String description;
    private final boolean acceptingExecutionId;
    private final boolean acceptingParentExecutionId;
    private final boolean acceptingExecutionType;
    private final boolean acceptingName;
    private final boolean acceptingIncludeChildren;
    private final boolean acceptingLimit;

    OperationType(
        String description,
        boolean acceptingExecutionId,
        boolean acceptingParentExecutionId,
        boolean acceptingExecutionType,
        boolean acceptingName,
        boolean acceptingIncludeChildren,
        boolean acceptingLimit) {
      this.description = description;
      this.acceptingExecutionId = acceptingExecutionId;
      this.acceptingParentExecutionId = acceptingParentExecutionId;
      this.acceptingExecutionType = acceptingExecutionType;
      this.acceptingName = acceptingName;
      this.acceptingIncludeChildren = acceptingIncludeChildren;
      this.acceptingLimit = acceptingLimit;
    }

    public static final String[] getDescriptions() {
      String[] descriptions = new String[values().length];
      for (int d = 0; d < descriptions.length; d++) {
        descriptions[d] = values()[d].getDescription();
      }
      return descriptions;
    }

    public static final OperationType getTypeByDescription(String description) {
      for (OperationType type : values()) {
        if (type.getDescription().equalsIgnoreCase(description)) {
          return type;
        }
      }
      return null;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    public String getDescription() {
      return description;
    }

    /**
     * Gets acceptingExecutionId
     *
     * @return value of acceptingExecutionId
     */
    public boolean isAcceptingExecutionId() {
      return acceptingExecutionId;
    }

    /**
     * Gets acceptingParentExecutionId
     *
     * @return value of acceptingParentExecutionId
     */
    public boolean isAcceptingParentExecutionId() {
      return acceptingParentExecutionId;
    }

    /**
     * Gets acceptingExecutionType
     *
     * @return value of acceptingExecutionType
     */
    public boolean isAcceptingExecutionType() {
      return acceptingExecutionType;
    }

    /**
     * Gets acceptingName
     *
     * @return value of acceptingName
     */
    public boolean isAcceptingName() {
      return acceptingName;
    }

    /**
     * Gets acceptingIncludeChildren
     *
     * @return value of acceptingIncludeChildren
     */
    public boolean isAcceptingIncludeChildren() {
      return acceptingIncludeChildren;
    }

    /**
     * Gets acceptingLimit
     *
     * @return value of acceptingLimit
     */
    public boolean isAcceptingLimit() {
      return acceptingLimit;
    }
  }

  /**
   * Gets location
   *
   * @return value of location
   */
  public String getLocation() {
    return location;
  }

  /**
   * Sets location
   *
   * @param location value of location
   */
  public void setLocation(String location) {
    this.location = location;
  }

  /**
   * Gets operationType
   *
   * @return value of operationType
   */
  public OperationType getOperationType() {
    return operationType;
  }

  /**
   * Sets operationType
   *
   * @param operationType value of operationType
   */
  public void setOperationType(OperationType operationType) {
    this.operationType = operationType;
  }

  /**
   * Gets idFieldName
   *
   * @return value of idFieldName
   */
  public String getIdFieldName() {
    return idFieldName;
  }

  /**
   * Sets idFieldName
   *
   * @param idFieldName value of idFieldName
   */
  public void setIdFieldName(String idFieldName) {
    this.idFieldName = idFieldName;
  }

  /**
   * Gets parentIdFieldName
   *
   * @return value of parentIdFieldName
   */
  public String getParentIdFieldName() {
    return parentIdFieldName;
  }

  /**
   * Sets parentIdFieldName
   *
   * @param parentIdFieldName value of parentIdFieldName
   */
  public void setParentIdFieldName(String parentIdFieldName) {
    this.parentIdFieldName = parentIdFieldName;
  }

  /**
   * Gets typeFieldName
   *
   * @return value of typeFieldName
   */
  public String getTypeFieldName() {
    return typeFieldName;
  }

  /**
   * Sets typeFieldName
   *
   * @param typeFieldName value of typeFieldName
   */
  public void setTypeFieldName(String typeFieldName) {
    this.typeFieldName = typeFieldName;
  }

  /**
   * Gets nameFieldName
   *
   * @return value of nameFieldName
   */
  public String getNameFieldName() {
    return nameFieldName;
  }

  /**
   * Sets nameFieldName
   *
   * @param nameFieldName value of nameFieldName
   */
  public void setNameFieldName(String nameFieldName) {
    this.nameFieldName = nameFieldName;
  }

  /**
   * Gets includeChildrenFieldName
   *
   * @return value of includeChildrenFieldName
   */
  public String getIncludeChildrenFieldName() {
    return includeChildrenFieldName;
  }

  /**
   * Sets includeChildrenFieldName
   *
   * @param includeChildrenFieldName value of includeChildrenFieldName
   */
  public void setIncludeChildrenFieldName(String includeChildrenFieldName) {
    this.includeChildrenFieldName = includeChildrenFieldName;
  }

  /**
   * Gets limitFieldName
   *
   * @return value of limitFieldName
   */
  public String getLimitFieldName() {
    return limitFieldName;
  }

  /**
   * Sets limitFieldName
   *
   * @param limitFieldName value of limitFieldName
   */
  public void setLimitFieldName(String limitFieldName) {
    this.limitFieldName = limitFieldName;
  }
}
