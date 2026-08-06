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

package org.apache.hop.pipeline.transforms.validator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transform.stream.IStream;

public class Validator extends BaseTransform<ValidatorMeta, ValidatorData> implements ITransform {
  private static final Class<?> PKG = ValidatorMeta.class;

  /** Placeholder used in log/error messages when failed data must not be exposed. */
  static final String OMITTED_VALUE = "?";

  public Validator(
      TransformMeta transformMeta,
      ValidatorMeta meta,
      ValidatorData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r;
    if (first) {
      first = false;

      readSourceValuesFromInfoTransforms();

      // Read the row AFTER the info rows
      // That way the info-rowsets are out of the way
      //
      r = getRow(); // get row, set busy!
      if (r == null) { // no more input to be expected...

        setOutputDone();
        return false;
      }

      calculateFieldIndexes();
    } else {
      // Read the row AFTER the info rows.
      // That way the info-rowsets are out of the way.
      //
      r = getRow(); // get row, set busy!
      if (r == null) { // no more input to be expected...

        setOutputDone();
        return false;
      }
    }

    if (isRowLevel()) {
      logRowlevel("Read row #" + getLinesRead() + " : " + getInputRowMeta().getString(r));
    }

    try {
      List<HopValidatorException> exceptions = validateFields(getInputRowMeta(), r);
      if (!exceptions.isEmpty()) {
        if (getTransformMeta().isDoingErrorHandling()) {
          if (meta.isConcatenatingErrors()) {
            StringBuilder messages = new StringBuilder();
            StringBuilder fields = new StringBuilder();
            StringBuilder codes = new StringBuilder();
            boolean notFirst = false;
            for (HopValidatorException e : exceptions) {
              if (notFirst) {
                messages.append(meta.getConcatenationSeparator());
                fields.append(meta.getConcatenationSeparator());
                codes.append(meta.getConcatenationSeparator());
              } else {
                notFirst = true;
              }
              messages.append(e.getMessage());
              fields.append(e.getFieldName());
              codes.append(e.getCodeDesc());
            }
            putError(
                data.inputRowMeta,
                r,
                exceptions.size(),
                messages.toString(),
                fields.toString(),
                codes.toString());
          } else {
            for (HopValidatorException e : exceptions) {
              putError(data.inputRowMeta, r, 1, e.getMessage(), e.getFieldName(), e.getCodeDesc());
            }
          }
        } else {
          HopValidatorException e = exceptions.get(0);
          throw new HopException(e.getMessage(), e);
        }
      } else {
        putRow(getInputRowMeta(), r); // copy row to possible alternate rowset(s).
      }
    } catch (HopValidatorException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(data.inputRowMeta, r, 1, e.getMessage(), e.getFieldName(), e.getCodeDesc());
      } else {
        throw new HopException(e.getMessage(), e);
      }
    }

    if (isRowLevel()) {
      logRowlevel("Wrote row #" + getLinesWritten() + " : " + getInputRowMeta().getString(r));
    }
    if (checkFeedback(getLinesRead()) && isBasic()) {
      logBasic("Linenr " + getLinesRead());
    }

    return true;
  }

  private void calculateFieldIndexes() throws HopTransformException {
    data.fieldIndexes = new int[meta.getValidations().size()];

    // Calculate the indexes of the values and arguments in the target data or temporary data
    // We do this in advance to save time later on.
    //
    for (int i = 0; i < meta.getValidations().size(); i++) {
      Validation field = meta.getValidations().get(i);

      if (StringUtils.isNotEmpty(field.getFieldName())) {
        data.fieldIndexes[i] = getInputRowMeta().indexOfValue(field.getFieldName());
        if (data.fieldIndexes[i] < 0) {
          // Nope: throw an exception
          throw new HopTransformException(
              "Unable to find the specified field name '"
                  + field.getFieldName()
                  + "' for validation number "
                  + (i + 1));
        }
      } else {
        throw new HopTransformException(
            "There is no name specified for validator field #" + (i + 1));
      }
    }
  }

  /**
   * Every copy of this transform builds its own list of allowed values from the info transforms, so
   * every copy needs to receive all the rows of those transforms. That only happens when the info
   * transform copies its rows to all target copies. With the default round robin data movement each
   * copy would see a different slice of the allowed values and reject perfectly valid rows, so we
   * refuse to run instead of silently producing wrong results.
   *
   * <p>Partitioned transforms are left alone: there the copies are matched one to one with the
   * copies of the info transform, which is a deliberate setup.
   *
   * @return true when the allowed values reach every copy of this transform
   */
  private boolean verifyInfoTransformsReachEveryCopy() {
    if (getTransformMeta().isPartitioned() || getTransformMeta().getCopies(this) <= 1) {
      return true;
    }

    boolean ok = true;
    List<IStream> streams = meta.getTransformIOMeta().getInfoStreams();
    for (int i = 0; i < meta.getValidations().size(); i++) {
      Validation field = meta.getValidations().get(i);
      if (!field.isSourcingValues() || i >= streams.size()) {
        continue;
      }
      TransformMeta sourceTransform = streams.get(i).getTransformMeta();
      if (sourceTransform == null || !sourceTransform.isDistributes()) {
        continue;
      }
      logError(
          BaseMessages.getString(
              PKG,
              "Validator.Exception.InfoTransformIsDistributing",
              sourceTransform.getName(),
              getTransformName(),
              Integer.toString(getTransformMeta().getCopies(this)),
              Const.NVL(field.getName(), field.getFieldName())));
      ok = false;
    }
    return ok;
  }

  private void readSourceValuesFromInfoTransforms() throws HopTransformException {
    // The input and the output are the same but without the "info" source transforms.
    //
    data.inputRowMeta = getPipelineMeta().getTransformFields(variables, getTransformName());

    List<IStream> streams = meta.getTransformIOMeta().getInfoStreams();

    // Group the validations that source their allowed values by source transform. An info stream
    // can only be drained once, so validations reading from the same transform have to be served
    // from a single pass over that stream. They can still read different fields from it.
    //
    Map<String, List<Integer>> validationsPerTransform = new LinkedHashMap<>();
    for (int i = 0; i < meta.getValidations().size(); i++) {
      Validation field = meta.getValidations().get(i);

      // If we need to source the "allowed values" data from a different transform, we do this here
      // as well.
      //
      if (field.isSourcingValues()) {
        if (streams.get(i).getTransformMeta() == null) {
          throw new HopTransformException(
              "There is no valid source transform specified for the allowed values of validation ["
                  + field.getName()
                  + "]");
        }
        if (StringUtils.isEmpty(field.getSourcingField())) {
          throw new HopTransformException(
              "There is no valid source field specified for the allowed values of validation ["
                  + field.getName()
                  + "]");
        }

        validationsPerTransform
            .computeIfAbsent(streams.get(i).getTransformName(), name -> new ArrayList<>())
            .add(i);
      }
    }

    for (Map.Entry<String, List<Integer>> entry : validationsPerTransform.entrySet()) {
      readSourceValuesFromInfoTransform(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Read the allowed values of all the validations that source their data from the given transform.
   * The stream is read once, the values are stored in data.listValues and data.constantsMeta.
   *
   * @param transformName the info transform to read the allowed values from
   * @param validationIndexes the validations served by this info transform
   */
  private void readSourceValuesFromInfoTransform(
      String transformName, List<Integer> validationIndexes) throws HopTransformException {
    IRowSet allowedRowSet = findInputRowSet(transformName);
    if (allowedRowSet == null) {
      throw new HopTransformException(
          "Unable to find the rows with allowed values coming from transform ["
              + transformName
              + "]. Please verify that the hop between ["
              + transformName
              + "] and ["
              + getTransformName()
              + "] is enabled.");
    }

    int nrValidations = validationIndexes.size();
    int[] sourceIndexes = new int[nrValidations];
    Arrays.fill(sourceIndexes, -1);
    List<List<Object>> allowedValues = new ArrayList<>(nrValidations);
    for (int v = 0; v < nrValidations; v++) {
      allowedValues.add(new ArrayList<>());
    }

    Object[] allowedRowData = getRowFrom(allowedRowSet);
    while (allowedRowData != null) {
      IRowMeta allowedRowMeta = allowedRowSet.getRowMeta();
      for (int v = 0; v < nrValidations; v++) {
        if (sourceIndexes[v] < 0) {
          Validation field = meta.getValidations().get(validationIndexes.get(v));
          sourceIndexes[v] = allowedRowMeta.indexOfValue(field.getSourcingField());
          if (sourceIndexes[v] < 0) {
            throw new HopTransformException(
                "Source field ["
                    + field.getSourcingField()
                    + "] is not found in the source row data");
          }
          data.constantsMeta[validationIndexes.get(v)] =
              allowedRowMeta.getValueMeta(sourceIndexes[v]);
        }
        Object allowedValue = allowedRowData[sourceIndexes[v]];
        if (allowedValue != null) {
          allowedValues.get(v).add(allowedValue);
        }
      }

      // Grab another row too...
      //
      allowedRowData = getRowFrom(allowedRowSet);
    }

    // Set the list values in the data block...
    //
    for (int v = 0; v < nrValidations; v++) {
      int i = validationIndexes.get(v);
      data.listValues[i] = allowedValues.get(v).toArray(new Object[0]);
      data.listValuesSet[i] = createLookupSet(data.constantsMeta[i], data.listValues[i]);
    }
  }

  /**
   * Build a hash based view on the allowed values so we don't have to walk the whole list for every
   * row. This is only safe for data types where two values are equal exactly when they compare as
   * equal, so other types simply keep using the linear scan.
   *
   * @param constantsMeta the value metadata the allowed values are stored in
   * @param listValues the allowed values
   * @return a set of the allowed values, or null when the type doesn't allow this optimisation
   */
  private static Set<Object> createLookupSet(IValueMeta constantsMeta, Object[] listValues) {
    if (constantsMeta == null
        || listValues.length == 0
        || constantsMeta.getStorageType() != IValueMeta.STORAGE_TYPE_NORMAL
        || !isHashableType(constantsMeta.getType())) {
      return null;
    }
    Set<Object> set = new HashSet<>((int) (listValues.length / 0.75f) + 1);
    for (Object value : listValues) {
      // Null values never match: the validation skips null values before the list is consulted.
      if (value != null) {
        set.add(value);
      }
    }
    return set;
  }

  @SuppressWarnings("unchecked")
  private static Set<Object>[] newSetArray(int size) {
    return new Set[size];
  }

  /**
   * Types for which equals() and compare()==0 agree. Doubles (-0.0, NaN) and BigDecimals (scale)
   * are deliberately left out, and so are dates because they can be mixed with timestamps.
   */
  private static boolean isHashableType(int type) {
    return type == IValueMeta.TYPE_STRING
        || type == IValueMeta.TYPE_INTEGER
        || type == IValueMeta.TYPE_BOOLEAN;
  }

  /**
   * The hash lookup is only equivalent to the linear scan when the incoming value is compared with
   * plain equality. A different data type, a non normal storage type or a string that is compared
   * with a collator, case insensitively or while ignoring whitespace all change the outcome, so in
   * those cases we fall back to comparing value by value.
   */
  private static boolean canUseLookupSet(IValueMeta valueMeta, IValueMeta constantsMeta) {
    if (valueMeta.getType() != constantsMeta.getType()
        || valueMeta.getStorageType() != IValueMeta.STORAGE_TYPE_NORMAL) {
      return false;
    }
    return valueMeta.getType() != IValueMeta.TYPE_STRING
        || (valueMeta.isCollatorDisabled()
            && !valueMeta.isCaseInsensitive()
            && !valueMeta.isIgnoreWhitespace());
  }

  /**
   * @param inputRowMeta the input row metadata
   * @param r the input row (data)
   * @throws HopValidatorException in case there is a validation error, details are stored in the
   *     exception.
   */
  private List<HopValidatorException> validateFields(IRowMeta inputRowMeta, Object[] r)
      throws HopValueException {
    // Reused per row: processRow is done with the list before the next row comes in.
    List<HopValidatorException> exceptions = data.exceptions;
    exceptions.clear();

    for (int i = 0; i < meta.getValidations().size(); i++) {
      Validation field = meta.getValidations().get(i);

      int valueIndex = data.fieldIndexes[i];
      IValueMeta validatorMeta = data.constantsMeta[i];

      IValueMeta valueMeta = inputRowMeta.getValueMeta(valueIndex);
      Object valueData = r[valueIndex];

      // Check for null
      //
      boolean isNull = valueMeta.isNull(valueData);
      if (!field.isNullAllowed() && isNull) {
        HopValidatorException exception =
            new HopValidatorException(
                this,
                field,
                HopValidatorException.ERROR_NULL_VALUE_NOT_ALLOWED,
                BaseMessages.getString(
                    PKG,
                    "Validator.Exception.NullNotAllowed",
                    field.getFieldName(),
                    getRowForMessage(inputRowMeta, r)),
                field.getFieldName());
        exceptions.add(exception);
        if (!meta.isValidatingAll()) {
          return exceptions;
        }
      }

      if (field.isOnlyNullAllowed() && !isNull) {
        HopValidatorException exception =
            new HopValidatorException(
                this,
                field,
                HopValidatorException.ERROR_ONLY_NULL_VALUE_ALLOWED,
                BaseMessages.getString(
                    PKG,
                    "Validator.Exception.OnlyNullAllowed",
                    field.getFieldName(),
                    getRowForMessage(inputRowMeta, r)),
                field.getFieldName());
        exceptions.add(exception);
        if (!meta.isValidatingAll()) {
          return exceptions;
        }
      }

      int dataType = data.dataTypes[i];

      // Check the data type!
      // Same data type?
      //
      if (field.isDataTypeVerified()
          && dataType != IValueMeta.TYPE_NONE
          && dataType != valueMeta.getType()) {
        HopValidatorException exception =
            new HopValidatorException(
                this,
                field,
                HopValidatorException.ERROR_UNEXPECTED_DATA_TYPE,
                BaseMessages.getString(
                    PKG,
                    "Validator.Exception.UnexpectedDataType",
                    field.getFieldName(),
                    valueMeta.toStringMeta(),
                    validatorMeta.toStringMeta()),
                field.getFieldName());
        exceptions.add(exception);
        if (!meta.isValidatingAll()) {
          return exceptions;
        }
      }

      // Check various things if the value is not null.
      //
      if (isNull) {
        continue;
      }

      if (data.hasValueChecks[i]) {

        // Only render the value as a string when one of the configured rules actually needs it.
        // The placeholder below is never looked at: data.needsStringValue covers exactly the rules
        // that use stringValue or stringLength, so keep the two in sync when adding a rule.
        //
        String stringValue = data.needsStringValue[i] ? valueMeta.getString(valueData) : "";
        int stringLength = stringValue.length();

        // Minimum length
        //
        if (data.fieldsMinimumLengthAsInt[i] >= 0
            && stringLength < data.fieldsMinimumLengthAsInt[i]) {
          HopValidatorException exception =
              new HopValidatorException(
                  this,
                  field,
                  HopValidatorException.ERROR_SHORTER_THAN_MINIMUM_LENGTH,
                  BaseMessages.getString(
                      PKG,
                      "Validator.Exception.ShorterThanMininumLength",
                      field.getFieldName(),
                      getValueForMessage(valueMeta, valueData),
                      Integer.toString(stringValue.length()),
                      field.getMinimumLength()),
                  field.getFieldName());
          exceptions.add(exception);
          if (!meta.isValidatingAll()) {
            return exceptions;
          }
        }

        // Maximum length
        //
        if (data.fieldsMaximumLengthAsInt[i] >= 0
            && stringLength > data.fieldsMaximumLengthAsInt[i]) {
          HopValidatorException exception =
              new HopValidatorException(
                  this,
                  field,
                  HopValidatorException.ERROR_LONGER_THAN_MAXIMUM_LENGTH,
                  BaseMessages.getString(
                      PKG,
                      "Validator.Exception.LongerThanMaximumLength",
                      field.getFieldName(),
                      getValueForMessage(valueMeta, valueData),
                      Integer.toString(stringValue.length()),
                      field.getMaximumLength()),
                  field.getFieldName());
          exceptions.add(exception);
          if (!meta.isValidatingAll()) {
            return exceptions;
          }
        }

        // Minimal value
        //
        if (data.minimumValue[i] != null
            && valueMeta.compare(valueData, validatorMeta, data.minimumValue[i]) < 0) {
          HopValidatorException exception =
              new HopValidatorException(
                  this,
                  field,
                  HopValidatorException.ERROR_LOWER_THAN_ALLOWED_MINIMUM,
                  BaseMessages.getString(
                      PKG,
                      "Validator.Exception.LowerThanMinimumValue",
                      field.getFieldName(),
                      getValueForMessage(valueMeta, valueData),
                      data.constantsMeta[i].getString(data.minimumValue[i])),
                  field.getFieldName());
          exceptions.add(exception);
          if (!meta.isValidatingAll()) {
            return exceptions;
          }
        }

        // Maximum value
        //
        if (data.maximumValue[i] != null
            && valueMeta.compare(valueData, validatorMeta, data.maximumValue[i]) > 0) {
          HopValidatorException exception =
              new HopValidatorException(
                  this,
                  field,
                  HopValidatorException.ERROR_HIGHER_THAN_ALLOWED_MAXIMUM,
                  BaseMessages.getString(
                      PKG,
                      "Validator.Exception.HigherThanMaximumValue",
                      field.getFieldName(),
                      getValueForMessage(valueMeta, valueData),
                      data.constantsMeta[i].getString(data.maximumValue[i])),
                  field.getFieldName());
          exceptions.add(exception);
          if (!meta.isValidatingAll()) {
            return exceptions;
          }
        }

        // In list?
        //
        if (field.isSourcingValues() || data.listValues[i].length > 0) {
          if (!isInAllowedValues(i, valueMeta, validatorMeta, valueData)) {
            HopValidatorException exception =
                new HopValidatorException(
                    this,
                    field,
                    HopValidatorException.ERROR_VALUE_NOT_IN_LIST,
                    BaseMessages.getString(
                        PKG,
                        "Validator.Exception.NotInList",
                        field.getFieldName(),
                        getValueForMessage(valueMeta, valueData)),
                    field.getFieldName());
            exceptions.add(exception);
            if (!meta.isValidatingAll()) {
              return exceptions;
            }
          }
        }

        // Numeric data or strings with only
        if (field.isOnlyNumericAllowed()) {
          HopValidatorException exception =
              assertNumeric(
                  valueMeta, valueData, field, data.needsStringValue[i] ? stringValue : null);
          if (exception != null) {
            exceptions.add(exception);
            if (!meta.isValidatingAll()) {
              return exceptions;
            }
          }
        }

        // Does not start with string value
        //
        if (StringUtils.isNotEmpty(data.startString[i])
            && !stringValue.startsWith(data.startString[i])) {
          HopValidatorException exception =
              new HopValidatorException(
                  this,
                  field,
                  HopValidatorException.ERROR_DOES_NOT_START_WITH_STRING,
                  BaseMessages.getString(
                      PKG,
                      "Validator.Exception.DoesNotStartWithString",
                      field.getFieldName(),
                      getValueForMessage(valueMeta, valueData),
                      field.getStartString()),
                  field.getFieldName());
          exceptions.add(exception);
          if (!meta.isValidatingAll()) {
            return exceptions;
          }
        }

        // Ends with string value
        //
        if (StringUtils.isNotEmpty(data.endString[i]) && !stringValue.endsWith(data.endString[i])) {
          HopValidatorException exception =
              new HopValidatorException(
                  this,
                  field,
                  HopValidatorException.ERROR_DOES_NOT_END_WITH_STRING,
                  BaseMessages.getString(
                      PKG,
                      "Validator.Exception.DoesNotEndWithString",
                      field.getFieldName(),
                      getValueForMessage(valueMeta, valueData),
                      field.getEndString()),
                  field.getFieldName());
          exceptions.add(exception);
          if (!meta.isValidatingAll()) {
            return exceptions;
          }
        }

        // Starts with string value
        //
        if (StringUtils.isNotEmpty(data.startStringNotAllowed[i])
            && stringValue.startsWith(data.startStringNotAllowed[i])) {
          HopValidatorException exception =
              new HopValidatorException(
                  this,
                  field,
                  HopValidatorException.ERROR_STARTS_WITH_STRING,
                  BaseMessages.getString(
                      PKG,
                      "Validator.Exception.StartsWithString",
                      field.getFieldName(),
                      getValueForMessage(valueMeta, valueData),
                      field.getStartStringNotAllowed()),
                  field.getFieldName());
          exceptions.add(exception);
          if (!meta.isValidatingAll()) {
            return exceptions;
          }
        }

        // Ends with string value
        //
        if (StringUtils.isNotEmpty(data.endStringNotAllowed[i])
            && stringValue.endsWith(data.endStringNotAllowed[i])) {
          HopValidatorException exception =
              new HopValidatorException(
                  this,
                  field,
                  HopValidatorException.ERROR_ENDS_WITH_STRING,
                  BaseMessages.getString(
                      PKG,
                      "Validator.Exception.EndsWithString",
                      field.getFieldName(),
                      getValueForMessage(valueMeta, valueData),
                      field.getEndStringNotAllowed()),
                  field.getFieldName());
          exceptions.add(exception);
          if (!meta.isValidatingAll()) {
            return exceptions;
          }
        }

        // Matching regular expression allowed?
        //
        if (data.matcherExpected[i] != null) {
          if (!data.matcherExpected[i].reset(stringValue).matches()) {
            HopValidatorException exception =
                new HopValidatorException(
                    this,
                    field,
                    HopValidatorException.ERROR_MATCHING_REGULAR_EXPRESSION_EXPECTED,
                    BaseMessages.getString(
                        PKG,
                        "Validator.Exception.MatchingRegExpExpected",
                        field.getFieldName(),
                        getValueForMessage(valueMeta, valueData),
                        data.regularExpression[i]),
                    field.getFieldName());
            exceptions.add(exception);
            if (!meta.isValidatingAll()) {
              return exceptions;
            }
          }
        }

        // Matching regular expression NOT allowed?
        //
        if (data.matcherDisallowed[i] != null) {
          if (data.matcherDisallowed[i].reset(stringValue).matches()) {
            HopValidatorException exception =
                new HopValidatorException(
                    this,
                    field,
                    HopValidatorException.ERROR_MATCHING_REGULAR_EXPRESSION_NOT_ALLOWED,
                    BaseMessages.getString(
                        PKG,
                        "Validator.Exception.MatchingRegExpNotAllowed",
                        field.getFieldName(),
                        getValueForMessage(valueMeta, valueData),
                        data.regularExpressionNotAllowed[i]),
                    field.getFieldName());
            exceptions.add(exception);
            if (!meta.isValidatingAll()) {
              return exceptions;
            }
          }
        }
      }
    }

    return exceptions;
  }

  /**
   * Look the value up in the allowed values of validation i. Uses the hash based lookup when the
   * data type allows it and falls back to comparing the value with every allowed value otherwise.
   *
   * @param i the index of the validation
   * @param valueMeta the metadata of the incoming value
   * @param constantsMeta the metadata the allowed values are stored in
   * @param valueData the incoming value
   * @return true if the value is one of the allowed values
   */
  private boolean isInAllowedValues(
      int i, IValueMeta valueMeta, IValueMeta constantsMeta, Object valueData)
      throws HopValueException {
    if (data.listValuesSet[i] != null && canUseLookupSet(valueMeta, constantsMeta)) {
      return data.listValuesSet[i].contains(valueData);
    }
    for (Object object : data.listValues[i]) {
      if (object != null && valueMeta.compare(valueData, constantsMeta, object) == 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * The engine writes a log line for every rejected row. On a transform whose job is to reject rows
   * that log line costs far more than the validation itself, so it can be switched off while error
   * rows keep flowing to the error handling hop.
   */
  @Override
  protected boolean isLoggingErrorDescriptions() {
    return !meta.isSuppressingErrorLog();
  }

  /**
   * Returns the field value for inclusion in validation messages, or a placeholder when logging of
   * failed data is suppressed.
   */
  String getValueForMessage(IValueMeta valueMeta, Object valueData) throws HopValueException {
    if (meta.isSuppressingLogFailedData()) {
      return OMITTED_VALUE;
    }
    return valueMeta.getString(valueData);
  }

  /**
   * Returns the row for inclusion in validation messages, or a placeholder when logging of failed
   * data is suppressed.
   */
  String getRowForMessage(IRowMeta inputRowMeta, Object[] r) throws HopValueException {
    if (meta.isSuppressingLogFailedData()) {
      return OMITTED_VALUE;
    }
    return inputRowMeta.getString(r);
  }

  // package-local visibility for testing purposes
  HopValidatorException assertNumeric(IValueMeta valueMeta, Object valueData, Validation field)
      throws HopValueException {
    return assertNumeric(valueMeta, valueData, field, null);
  }

  /**
   * @param stringValue the value already rendered as a string, or null when it still has to be
   *     converted. Numeric fields never need the conversion at all.
   */
  private HopValidatorException assertNumeric(
      IValueMeta valueMeta, Object valueData, Validation field, String stringValue)
      throws HopValueException {
    if (valueMeta.isNumeric()
        || containsOnlyDigits(stringValue != null ? stringValue : valueMeta.getString(valueData))) {
      return null;
    }
    return new HopValidatorException(
        this,
        field,
        HopValidatorException.ERROR_NON_NUMERIC_DATA,
        BaseMessages.getString(
            PKG,
            "Validator.Exception.NonNumericDataNotAllowed",
            field.getFieldName(),
            valueMeta.toStringMeta(),
            getValueForMessage(valueMeta, valueData)),
        field.getFieldName());
  }

  private boolean containsOnlyDigits(String string) {
    for (int i = 0; i < string.length(); i++) {
      char c = string.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }

    // initialize transforms by names
    List<TransformMeta> transforms = new ArrayList<>();
    List<TransformMetaDataCombi> pipelineTransforms =
        ((LocalPipelineEngine) getPipeline()).getTransforms();
    if (pipelineTransforms != null) {
      for (TransformMetaDataCombi s : pipelineTransforms) {
        transforms.add(s.transformMeta);
      }
    }
    meta.searchInfoAndTargetTransforms(transforms);

    if (!verifyInfoTransformsReachEveryCopy()) {
      return false;
    }

    // initialize arrays of validation data
    data.constantsMeta = new IValueMeta[meta.getValidations().size()];
    data.dataTypes = new int[meta.getValidations().size()];
    data.listValuesSet = newSetArray(meta.getValidations().size());
    data.needsStringValue = new boolean[meta.getValidations().size()];
    data.hasValueChecks = new boolean[meta.getValidations().size()];
    data.minimumValueAsString = new String[meta.getValidations().size()];
    data.maximumValueAsString = new String[meta.getValidations().size()];
    data.fieldsMinimumLengthAsInt = new int[meta.getValidations().size()];
    data.fieldsMaximumLengthAsInt = new int[meta.getValidations().size()];
    data.minimumValue = new Object[meta.getValidations().size()];
    data.maximumValue = new Object[meta.getValidations().size()];
    data.listValues = new Object[meta.getValidations().size()][];
    data.errorCode = new String[meta.getValidations().size()];
    data.errorDescription = new String[meta.getValidations().size()];
    data.conversionMask = new String[meta.getValidations().size()];
    data.decimalSymbol = new String[meta.getValidations().size()];
    data.groupingSymbol = new String[meta.getValidations().size()];
    data.maximumLength = new String[meta.getValidations().size()];
    data.minimumLength = new String[meta.getValidations().size()];
    data.startString = new String[meta.getValidations().size()];
    data.endString = new String[meta.getValidations().size()];
    data.startStringNotAllowed = new String[meta.getValidations().size()];
    data.endStringNotAllowed = new String[meta.getValidations().size()];
    data.regularExpression = new String[meta.getValidations().size()];
    data.regularExpressionNotAllowed = new String[meta.getValidations().size()];
    data.patternExpected = new Pattern[meta.getValidations().size()];
    data.patternDisallowed = new Pattern[meta.getValidations().size()];
    data.matcherExpected = new Matcher[meta.getValidations().size()];
    data.matcherDisallowed = new Matcher[meta.getValidations().size()];
    data.exceptions = new ArrayList<>();

    for (int i = 0; i < meta.getValidations().size(); i++) {
      Validation field = meta.getValidations().get(i);
      // Resolving the data type goes over the plugin registry, so do it once instead of per row.
      int dataType = ValueMetaFactory.getIdForValueMeta(field.getDataType());
      data.dataTypes[i] = dataType;

      try {
        initBasics(i, field);
        IValueMeta stringMeta = cloneValueMeta(data.constantsMeta[i], IValueMeta.TYPE_STRING);
        initMinMaxValues(i, stringMeta);
        initMinStringLength(i);
        initMaxStringLength(i);
        initListValues(i, field, stringMeta);
      } catch (HopException e) {
        if (dataType == IValueMeta.TYPE_NONE) {
          logError(BaseMessages.getString(PKG, "Validator.Exception.SpecifyDataType"), e);
        } else {
          logError(
              BaseMessages.getString(PKG, "Validator.Exception.DataConversionErrorEncountered"), e);
        }
        return false;
      }

      if (StringUtils.isNotEmpty(data.regularExpression[i])) {
        data.patternExpected[i] = Pattern.compile(data.regularExpression[i]);
        data.matcherExpected[i] = data.patternExpected[i].matcher("");
      }
      if (StringUtils.isNotEmpty(data.regularExpressionNotAllowed[i])) {
        data.patternDisallowed[i] = Pattern.compile(data.regularExpressionNotAllowed[i]);
        data.matcherDisallowed[i] = data.patternDisallowed[i].matcher("");
      }

      initRuleFlags(i, field);
      data.listValuesSet[i] = createLookupSet(data.constantsMeta[i], data.listValues[i]);
    }

    return true;
  }

  /**
   * Work out once which groups of rules are configured for a validation, so that the row loop
   * doesn't have to test every single option again for every row.
   */
  private void initRuleFlags(int i, Validation field) {
    data.needsStringValue[i] =
        data.fieldsMinimumLengthAsInt[i] >= 0
            || data.fieldsMaximumLengthAsInt[i] >= 0
            || StringUtils.isNotEmpty(data.startString[i])
            || StringUtils.isNotEmpty(data.endString[i])
            || StringUtils.isNotEmpty(data.startStringNotAllowed[i])
            || StringUtils.isNotEmpty(data.endStringNotAllowed[i])
            || data.patternExpected[i] != null
            || data.patternDisallowed[i] != null;

    data.hasValueChecks[i] =
        data.needsStringValue[i]
            || data.minimumValue[i] != null
            || data.maximumValue[i] != null
            || data.listValues[i].length > 0
            || field.isSourcingValues()
            || field.isOnlyNumericAllowed();
  }

  private void initBasics(int i, Validation field) throws HopPluginException {
    data.constantsMeta[i] = createValueMeta(field.getFieldName(), field.getDataType());
    data.constantsMeta[i].setConversionMask(field.getConversionMask());
    data.constantsMeta[i].setDecimalSymbol(field.getDecimalSymbol());
    data.constantsMeta[i].setGroupingSymbol(field.getGroupingSymbol());
    data.errorCode[i] = resolve(Const.NVL(field.getErrorCode(), ""));
    data.errorDescription[i] = resolve(Const.NVL(field.getErrorDescription(), ""));
    data.conversionMask[i] = resolve(Const.NVL(field.getConversionMask(), ""));
    data.decimalSymbol[i] = resolve(Const.NVL(field.getDecimalSymbol(), ""));
    data.groupingSymbol[i] = resolve(Const.NVL(field.getGroupingSymbol(), ""));
    data.maximumLength[i] = resolve(Const.NVL(field.getMaximumLength(), ""));
    data.minimumLength[i] = resolve(Const.NVL(field.getMinimumLength(), ""));
    data.maximumValueAsString[i] = resolve(Const.NVL(field.getMaximumValue(), ""));
    data.minimumValueAsString[i] = resolve(Const.NVL(field.getMinimumValue(), ""));
    data.startString[i] = resolve(Const.NVL(field.getStartString(), ""));
    data.endString[i] = resolve(Const.NVL(field.getEndString(), ""));
    data.startStringNotAllowed[i] = resolve(Const.NVL(field.getStartStringNotAllowed(), ""));
    data.endStringNotAllowed[i] = resolve(Const.NVL(field.getEndStringNotAllowed(), ""));
    data.regularExpression[i] = resolve(Const.NVL(field.getRegularExpression(), ""));
    data.regularExpressionNotAllowed[i] =
        resolve(Const.NVL(field.getRegularExpressionNotAllowed(), ""));
  }

  private void initMinMaxValues(int i, IValueMeta stringMeta) throws HopValueException {
    data.minimumValue[i] =
        StringUtils.isEmpty(data.minimumValueAsString[i])
            ? null
            : data.constantsMeta[i].convertData(stringMeta, data.minimumValueAsString[i]);
    data.maximumValue[i] =
        StringUtils.isEmpty(data.maximumValueAsString[i])
            ? null
            : data.constantsMeta[i].convertData(stringMeta, data.maximumValueAsString[i]);
  }

  private void initListValues(int i, Validation field, IValueMeta stringMeta)
      throws HopValueException {
    int listSize = field.getAllowedValues() != null ? field.getAllowedValues().size() : 0;
    data.listValues[i] = new Object[listSize];
    for (int s = 0; s < listSize; s++) {
      data.listValues[i][s] =
          StringUtils.isEmpty(field.getAllowedValues().get(s))
              ? null
              : data.constantsMeta[i].convertData(
                  stringMeta, resolve(field.getAllowedValues().get(s)));
    }
  }

  private void initMaxStringLength(int i) throws HopValueException {
    try {
      data.fieldsMaximumLengthAsInt[i] = Integer.parseInt(Const.NVL(data.maximumLength[i], "-1"));
    } catch (NumberFormatException nfe) {
      throw new HopValueException(
          "Caught a number format exception converting minimum length with value "
              + data.maximumLength[i]
              + " to an int.",
          nfe);
    }
  }

  private void initMinStringLength(int i) throws HopValueException {
    try {
      data.fieldsMinimumLengthAsInt[i] = Integer.parseInt(Const.NVL(data.minimumLength[i], "-1"));
    } catch (NumberFormatException nfe) {
      throw new HopValueException(
          "Caught a number format exception converting minimum length with value "
              + data.minimumLength[i]
              + " to an int.",
          nfe);
    }
  }

  protected IValueMeta createValueMeta(String name, String type) throws HopPluginException {
    int dataType = ValueMetaFactory.getIdForValueMeta(type);
    return ValueMetaFactory.createValueMeta(name, dataType);
  }

  protected IValueMeta cloneValueMeta(IValueMeta valueMeta, int type) throws HopPluginException {
    return ValueMetaFactory.cloneValueMeta(valueMeta, type);
  }
}
