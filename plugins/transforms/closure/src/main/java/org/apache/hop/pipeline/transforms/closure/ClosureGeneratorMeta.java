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

package org.apache.hop.pipeline.transforms.closure;

import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "ClosureGenerator",
    image = "closuregenerator.svg",
    name = "i18n::ClosureGenerator.Name",
    description = "i18n::ClosureGenerator.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::ClosureGeneratorMeta.keyword",
    documentationUrl = "/pipeline/transforms/closure.html")
public class ClosureGeneratorMeta
    extends BaseTransformMeta<ClosureGenerator, ClosureGeneratorData> {

  @HopMetadataProperty(key = "is_root_zero")
  private boolean rootIdZero;

  @HopMetadataProperty(key = "parent_id_field")
  private String parentIdFieldName;

  @HopMetadataProperty(key = "child_id_field")
  private String childIdFieldName;

  @HopMetadataProperty(key = "distance_field")
  private String distanceFieldName;

  public ClosureGeneratorMeta() {
    super();
  }

  public ClosureGeneratorMeta(ClosureGeneratorMeta m) {
    this.rootIdZero = m.rootIdZero;
    this.parentIdFieldName = m.parentIdFieldName;
    this.childIdFieldName = m.childIdFieldName;
    this.distanceFieldName = m.distanceFieldName;
  }

  @Override
  public ClosureGeneratorMeta clone() {
    return new ClosureGeneratorMeta(this);
  }

  @Override
  public void setDefault() {
    // Do nothing
  }

  @Override
  public void getFields(
      IRowMeta row,
      String origin,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // The output for the closure table is:
    //
    // - parentId
    // - childId
    // - distance
    //
    // Nothing else.
    //
    IRowMeta result = new RowMeta();
    IValueMeta parentValueMeta = row.searchValueMeta(parentIdFieldName);
    if (parentValueMeta != null) {
      result.addValueMeta(parentValueMeta);
    }

    IValueMeta childValueMeta = row.searchValueMeta(childIdFieldName);
    if (childValueMeta != null) {
      result.addValueMeta(childValueMeta);
    }

    IValueMeta distanceValueMeta = new ValueMetaInteger(distanceFieldName);
    distanceValueMeta.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH);
    result.addValueMeta(distanceValueMeta);

    row.clear();
    row.addRowMeta(result);
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
    CheckResult cr;

    IValueMeta parentValueMeta = prev.searchValueMeta(parentIdFieldName);
    if (parentValueMeta != null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "The fieldname of the parent id could not be found.",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "The fieldname of the parent id could be found",
              transformMeta);
      remarks.add(cr);
    }

    IValueMeta childValueMeta = prev.searchValueMeta(childIdFieldName);
    if (childValueMeta != null) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              "The fieldname of the child id could not be found.",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              "The fieldname of the child id could be found",
              transformMeta);
      remarks.add(cr);
    }
  }

  /**
   * @return the rootIdZero
   */
  public boolean isRootIdZero() {
    return rootIdZero;
  }

  /**
   * @param rootIdZero the rootIdZero to set
   */
  public void setRootIdZero(boolean rootIdZero) {
    this.rootIdZero = rootIdZero;
  }

  /**
   * @return the parentIdFieldName
   */
  public String getParentIdFieldName() {
    return parentIdFieldName;
  }

  /**
   * @param parentIdFieldName the parentIdFieldName to set
   */
  public void setParentIdFieldName(String parentIdFieldName) {
    this.parentIdFieldName = parentIdFieldName;
  }

  /**
   * @return the childIdFieldName
   */
  public String getChildIdFieldName() {
    return childIdFieldName;
  }

  /**
   * @param childIdFieldName the childIdFieldName to set
   */
  public void setChildIdFieldName(String childIdFieldName) {
    this.childIdFieldName = childIdFieldName;
  }

  /**
   * @return the distanceFieldName
   */
  public String getDistanceFieldName() {
    return distanceFieldName;
  }

  /**
   * @param distanceFieldName the distanceFieldName to set
   */
  public void setDistanceFieldName(String distanceFieldName) {
    this.distanceFieldName = distanceFieldName;
  }
}
