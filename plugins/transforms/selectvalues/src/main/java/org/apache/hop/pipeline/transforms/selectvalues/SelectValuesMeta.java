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

package org.apache.hop.pipeline.transforms.selectvalues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "SelectValues",
    image = "selectvalues.svg",
    name = "i18n:org.apache.hop.pipeline.transforms.selectvalues:SelectValues.Name",
    description = "i18n:org.apache.hop.pipeline.transforms.selectvalues:SelectValues.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    keywords = "i18n::SelectValuesMeta.keyword",
    documentationUrl = "/pipeline/transforms/selectvalues.html")
public class SelectValuesMeta extends BaseTransformMeta<SelectValues, SelectValuesData> {
  private static final Class<?> PKG = SelectValuesMeta.class;

  public static final int UNDEFINED = -2;

  @HopMetadataProperty(key = "fields")
  private SelectOptions selectOption;

  // SELECT mode
  // @InjectionDeep private SelectField[] selectFields = {};

  /**
   * Select: flag to indicate that the non-selected fields should also be taken along, ordered by
   * fieldname
   */
  public SelectValuesMeta() {
    super(); // allocate BaseTransformMeta
    selectOption = new SelectOptions();
  }

  @Override
  public SelectValuesMeta clone() {
    return new SelectValuesMeta(this);
  }

  public SelectValuesMeta(SelectValuesMeta m) {
    super.clone();
    this.selectOption = new SelectOptions(m.getSelectOption());
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
    try {
      IRowMeta rowMeta = inputRowMeta.clone();
      inputRowMeta.clear();
      inputRowMeta.addRowMeta(rowMeta);

      getSelectFields(inputRowMeta, name);
      getDeleteFields(inputRowMeta);
      getMetadataFields(inputRowMeta, name, variables);
    } catch (Exception e) {
      throw new HopTransformException(e);
    }
  }

  public String[] getSelectName() {
    String[] selectName = new String[getSelectOption().getSelectFields().size()];
    for (int i = 0; i < selectName.length; i++) {
      selectName[i] = getSelectOption().getSelectFields().get(i).getName();
    }
    return selectName;
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

    if (prev != null && !prev.isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SelectValuesMeta.CheckResult.TransformReceivingFields", prev.size() + ""),
              transformMeta);
      remarks.add(cr);

      /*
       * Take care of the normal SELECT fields...
       */
      String errorMessage = "";
      boolean errorFound = false;

      // Starting from selected fields in ...
      for (int i = 0; i < getSelectOption().getSelectFields().size(); i++) {
        var selectField = getSelectOption().getSelectFields().get(i);
        int idx = prev.indexOfValue(selectField.getName());
        if (idx < 0) {
          errorMessage += "\t\t" + selectField.getName() + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "SelectValuesMeta.CheckResult.SelectedFieldsNotFound")
                + Const.CR
                + Const.CR
                + errorMessage;

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "SelectValuesMeta.CheckResult.AllSelectedFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }

      if (!getSelectOption().getSelectFields().isEmpty()) {
        // Starting from prev...
        for (int i = 0; i < prev.size(); i++) {
          IValueMeta pv = prev.getValueMeta(i);
          int idx = Const.indexOfString(pv.getName(), getSelectName());
          if (idx < 0) {
            errorMessage += "\t\t" + pv.getName() + " (" + pv.getTypeDesc() + ")" + Const.CR;
            errorFound = true;
          }
        }
        if (errorFound) {
          errorMessage =
              BaseMessages.getString(PKG, "SelectValuesMeta.CheckResult.FieldsNotFound")
                  + Const.CR
                  + Const.CR
                  + errorMessage;

          cr = new CheckResult(ICheckResult.TYPE_RESULT_COMMENT, errorMessage, transformMeta);
          remarks.add(cr);
        } else {
          cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_OK,
                  BaseMessages.getString(
                      PKG, "SelectValuesMeta.CheckResult.AllSelectedFieldsFound2"),
                  transformMeta);
          remarks.add(cr);
        }
      }

      /*
       * How about the DE-SELECT (remove) fields...
       */

      errorMessage = "";
      errorFound = false;

      // Starting from selected fields in ...
      for (int i = 0; i < getSelectOption().getDeleteName().size(); i++) {
        int idx = prev.indexOfValue(getSelectOption().getDeleteName().get(i).getName());
        if (idx < 0) {
          errorMessage += "\t\t" + getSelectOption().getDeleteName().get(i) + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "SelectValuesMeta.CheckResult.DeSelectedFieldsNotFound")
                + Const.CR
                + Const.CR
                + errorMessage;

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "SelectValuesMeta.CheckResult.AllDeSelectedFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }

      /*
       * How about the Meta-fields...?
       */
      errorMessage = "";
      errorFound = false;

      // Starting from selected fields in ...
      for (int i = 0; i < getSelectOption().getMeta().size(); i++) {
        var currentName = getSelectOption().getMeta().get(i).getName();
        int idx = prev.indexOfValue(currentName);
        if (idx < 0) {
          errorMessage += "\t\t" + currentName + Const.CR;
          errorFound = true;
        }
      }
      if (errorFound) {
        errorMessage =
            BaseMessages.getString(PKG, "SelectValuesMeta.CheckResult.MetadataFieldsNotFound")
                + Const.CR
                + Const.CR
                + errorMessage;

        cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "SelectValuesMeta.CheckResult.AllMetadataFieldsFound"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SelectValuesMeta.CheckResult.FieldsNotFound2"),
              transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "SelectValuesMeta.CheckResult.TransformReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "SelectValuesMeta.CheckResult.NoInputReceivedError"),
              transformMeta);
      remarks.add(cr);
    }

    // Check for doubles in the selected fields...
    var selectFieldsSize = getSelectOption().getSelectFields().size();
    int[] cnt = new int[selectFieldsSize];
    boolean errorFound = false;
    String errorMessage = "";

    for (int i = 0; i < selectFieldsSize; i++) {
      cnt[i] = 0;
      for (int j = 0; j < selectFieldsSize; j++) {
        if (getSelectOption()
            .getSelectFields()
            .get(i)
            .getName()
            .equals(getSelectOption().getSelectFields().get(j).getName())) {
          cnt[i]++;
        }
      }

      if (cnt[i] > 1) {
        if (!errorFound) { // first time...
          errorMessage =
              BaseMessages.getString(PKG, "SelectValuesMeta.CheckResult.DuplicateFieldsSpecified")
                  + Const.CR;
        } else {
          errorFound = true;
        }
        errorMessage +=
            BaseMessages.getString(
                    PKG,
                    "SelectValuesMeta.CheckResult.OccurentRow",
                    i
                        + " : "
                        + getSelectOption().getSelectFields().get(i).getName()
                        + "  ("
                        + cnt[i])
                + Const.CR;
        errorFound = true;
      }
    }
    if (errorFound) {
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public SelectOptions getSelectOption() {
    return selectOption;
  }

  public void setSelectOption(SelectOptions selectOption) {
    this.selectOption = selectOption;
  }

  public void getSelectFields(IRowMeta inputRowMeta, String name) throws HopTransformException {
    IRowMeta row;

    var selectFields = selectOption.getSelectFields();
    if (!Utils.isEmpty(selectFields)) { // SELECT values

      // 0. Start with an empty row
      // 1. Keep only the selected values
      // 2. Rename the selected values
      // 3. Keep the order in which they are specified... (not the input order!)
      //

      row = new RowMeta();
      for (SelectField field : selectFields) {
        IValueMeta v = inputRowMeta.searchValueMeta(field.getName());
        var selectField = field;
        if (v != null) { // We found the value

          v = v.clone();
          // Do we need to rename ?
          if (!v.getName().equals(selectField.getRename())
              && selectField.getRename() != null
              && !selectField.getRename().isEmpty()) {
            v.setName(selectField.getRename());
            v.setOrigin(name);
          }
          if (selectField.getLength() != UNDEFINED) {
            v.setLength(selectField.getLength());
            v.setOrigin(name);
          }
          if (selectField.getPrecision() != UNDEFINED) {
            v.setPrecision(selectField.getPrecision());
            v.setOrigin(name);
          }

          // Add to the resulting row!
          row.addValueMeta(v);
        }
      }

      if (getSelectOption().isSelectingAndSortingUnspecifiedFields()) {
        // Select the unspecified fields.
        // Sort the fields
        // Add them after the specified fields...
        //
        List<String> extra = new ArrayList<>();
        for (int i = 0; i < inputRowMeta.size(); i++) {
          String fieldName = inputRowMeta.getValueMeta(i).getName();
          if (Const.indexOfString(fieldName, getSelectName()) < 0) {
            extra.add(fieldName);
          }
        }
        Collections.sort(extra);
        for (String fieldName : extra) {
          IValueMeta extraValue = inputRowMeta.searchValueMeta(fieldName);
          row.addValueMeta(extraValue);
        }
      }

      // OK, now remove all from r and re-add row:
      inputRowMeta.clear();
      inputRowMeta.addRowMeta(row);
    }
  }

  public void getDeleteFields(IRowMeta inputRowMeta) throws HopTransformException {
    var deleteNames = getSelectOption().getDeleteName();
    if (!Utils.isEmpty(deleteNames)) { // DESELECT values from the stream...
      for (var deleteName : deleteNames) {
        try {
          inputRowMeta.removeValueMeta(deleteName.getName());
        } catch (HopValueException e) {
          throw new HopTransformException(e);
        }
      }
    }
  }

  public void getMetadataFields(IRowMeta inputRowMeta, String name, IVariables variables)
      throws HopPluginException {
    var meta = getSelectOption().getMeta();
    if (!Utils.isEmpty(meta)) {
      // METADATA mode: change the meta-data of the values mentioned...

      for (SelectMetadataChange metaChange : meta) {
        int idx = inputRowMeta.indexOfValue(metaChange.getName());
        if (idx >= 0) { // We found the value

          // This is the value we need to change:
          IValueMeta v = inputRowMeta.getValueMeta(idx);

          // Do we need to rename ?
          if (!v.getName().equals(metaChange.getRename())
              && !Utils.isEmpty(metaChange.getRename())) {
            v.setName(metaChange.getRename());
            v.setOrigin(name);
          }
          // Change the type?
          if (!metaChange.getType().equals(ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_NONE))
              && !ValueMetaFactory.getValueMetaName(v.getType()).equals(metaChange.getType())) {
            v =
                ValueMetaFactory.cloneValueMeta(
                    v, ValueMetaFactory.getIdForValueMeta(metaChange.getType()));

            // This is now a copy, replace it in the row!
            //
            inputRowMeta.setValueMeta(idx, v);

            // This also moves the data to normal storage type
            //
            v.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
          }
          if (metaChange.getLength() != UNDEFINED) {
            v.setLength(metaChange.getLength());
            v.setOrigin(name);
          }
          if (metaChange.getPrecision() != UNDEFINED) {
            v.setPrecision(metaChange.getPrecision());
            v.setOrigin(name);
          }
          if (metaChange.getStorageType()
              != null) { // TODO check if this is correct because previously it was an integer and,
            // most likely, it could be negative when not set
            v.setStorageType(ValueMetaFactory.getIdForValueMeta(metaChange.getStorageType()));
            v.setOrigin(name);
          }
          if (!Utils.isEmpty(metaChange.getConversionMask())) {
            v.setConversionMask(metaChange.getConversionMask());
            v.setOrigin(name);
          }

          v.setDateFormatLenient(metaChange.isDateFormatLenient());
          v.setDateFormatLocale(EnvUtil.createLocale(metaChange.getDateFormatLocale()));
          v.setDateFormatTimeZone(EnvUtil.createTimeZone(metaChange.getDateFormatTimeZone()));
          v.setLenientStringToNumber(metaChange.isLenientStringToNumber());

          if (!Utils.isEmpty(metaChange.getEncoding())) {
            v.setStringEncoding(metaChange.getEncoding());
            v.setOrigin(name);
          }
          if (!Utils.isEmpty(metaChange.getDecimalSymbol())) {
            v.setDecimalSymbol(metaChange.getDecimalSymbol());
            v.setOrigin(name);
          }
          if (!Utils.isEmpty(metaChange.getGroupingSymbol())) {
            v.setGroupingSymbol(metaChange.getGroupingSymbol());
            v.setOrigin(name);
          }
          if (!Utils.isEmpty(metaChange.getCurrencySymbol())) {
            v.setCurrencySymbol(metaChange.getCurrencySymbol());
            v.setOrigin(name);
          }
          if (!Utils.isEmpty(metaChange.getRoundingType())) {
            v.setRoundingType(metaChange.getRoundingType());
            v.setOrigin(name);
          }
        }
      }
    }
  }
}
