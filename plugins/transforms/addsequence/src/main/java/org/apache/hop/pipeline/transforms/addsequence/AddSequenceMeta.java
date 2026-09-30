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

package org.apache.hop.pipeline.transforms.addsequence;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IOptionalDatabaseConnection;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.IStream.StreamType;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;

/** Meta data for the Add Sequence transform. */
@Transform(
    id = "Sequence",
    image = "addsequence.svg",
    name = "i18n::BaseTransform.TypeLongDesc.AddSequence",
    description = "i18n::BaseTransform.TypeTooltipDesc.AddSequence",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "/pipeline/transforms/addsequence.html",
    keywords = "i18n::AddSequenceMeta.keyword")
@Getter
@Setter
public class AddSequenceMeta extends BaseTransformMeta<AddSequence, AddSequenceData>
    implements IOptionalDatabaseConnection {

  private static final Class<?> PKG = AddSequenceMeta.class;

  @HopMetadataProperty(
      key = "valuename",
      injectionKeyDescription = "AddSequenceDialog.Valuename.Label")
  private String valueName;

  @HopMetadataProperty(
      key = "use_database",
      injectionKeyDescription = "AddSequenceDialog.UseDatabase.Label")
  private boolean databaseUsed;

  @HopMetadataProperty(
      key = "connection",
      injectionKeyDescription = "AddSequenceMeta.Injection.Connection",
      hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
  private String connection;

  @HopMetadataProperty(
      key = "schema",
      injectionKeyDescription = "AddSequenceMeta.Injection.SchemaName")
  private String schemaName;

  @HopMetadataProperty(
      key = "seqname",
      injectionKeyDescription = "AddSequenceMeta.Injection.SequenceName")
  private String sequenceName;

  @HopMetadataProperty(
      key = "use_counter",
      injectionKeyDescription = "AddSequenceMeta.Injection.UseCounter")
  private boolean counterUsed;

  @HopMetadataProperty(
      key = "counter_name",
      injectionKeyDescription = "AddSequenceMeta.Injection.CounterName")
  private String counterName;

  @HopMetadataProperty(
      key = "start_at",
      injectionKeyDescription = "AddSequenceMeta.Injection.StartAt")
  private String startAt;

  @HopMetadataProperty(
      key = "increment_by",
      injectionKeyDescription = "AddSequenceMeta.Injection.IncrementBy")
  private String incrementBy;

  @HopMetadataProperty(
      key = "max_value",
      injectionKeyDescription = "AddSequenceMeta.Injection.MaxValue")
  private String maxValue;

  /** Info transform that provides one row with the counter start, end, and increment. */
  @HopMetadataProperty(
      key = "configuration_transform",
      injectionKeyDescription = "AddSequenceMeta.Injection.ConfigurationTransform")
  private String configurationTransform;

  @HopMetadataProperty(
      key = "start_field",
      injectionKeyDescription = "AddSequenceMeta.Injection.StartField")
  private String startField;

  @HopMetadataProperty(
      key = "end_field",
      injectionKeyDescription = "AddSequenceMeta.Injection.EndField")
  private String endField;

  @HopMetadataProperty(
      key = "increment_field",
      injectionKeyDescription = "AddSequenceMeta.Injection.IncrementField")
  private String incrementField;

  /**
   * Counter values come from one row of {@link #configurationTransform} instead of the typed start,
   * increment, and maximum.
   */
  public boolean isConfigurationFromTransform() {
    return counterUsed && !databaseUsed && !Utils.isEmpty(configurationTransform);
  }

  /**
   * @param maxValue The maxValue to set.
   */
  public void setMaxValueByValue(long maxValue) {
    this.maxValue = Long.toString(maxValue);
  }

  /**
   * @param startAt The starting point of the sequence to set.
   */
  public void setStartAtByValue(long startAt) {
    this.startAt = Long.toString(startAt);
  }

  /**
   * @param incrementBy The incrementBy to set.
   */
  public void setIncrementByValue(long incrementBy) {
    this.incrementBy = Long.toString(incrementBy);
  }

  @Override
  public void setDefault() {
    valueName = "valuename";

    databaseUsed = false;
    schemaName = "";
    sequenceName = "SEQ_";
    counterUsed = true;
    counterName = null;
    startAt = "1";
    incrementBy = "1";
    maxValue = "999999999";
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    IValueMeta v = new ValueMetaInteger(valueName);
    v.setOrigin(name);
    row.addValueMeta(v);
  }

  /**
   * The connection is only used when a database sequence is selected. A counter leaves it unset,
   * and that must not be reported as a missing connection.
   */
  @Override
  public boolean isDatabaseConnectionUsed(String key) {
    return databaseUsed;
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
    // The counter does not open a connection. Loading an unset name here only raises "you need to
    // specify the name of the metadata object to load", which the verify dialog shows as a database
    // error. Issue #8561.
    if (databaseUsed) {
      checkDatabaseSequence(remarks, transformMeta, variables, metadataProvider);
    }

    CheckResult cr;
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "AddSequenceMeta.CheckResult.TransformIsReceving.Title"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AddSequenceMeta.CheckResult.NoInputReceived.Title"),
              transformMeta);
      remarks.add(cr);
    }

    checkConfigurationTransform(remarks, pipelineMeta, transformMeta, info);
  }

  /**
   * The configuration transform is optional. When it is set, the start, end, and increment field
   * names have to be set as well, and the transform has to exist.
   */
  private void checkConfigurationTransform(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta info) {
    if (!isConfigurationFromTransform()) {
      return;
    }

    boolean fieldsMissing =
        Utils.isEmpty(startField) || Utils.isEmpty(endField) || Utils.isEmpty(incrementField);
    if (fieldsMissing) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AddSequenceMeta.CheckResult.ConfigurationFieldsMissing"),
              transformMeta));
    }

    if (pipelineMeta != null) {
      if (pipelineMeta.findTransform(configurationTransform) == null) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG,
                    "AddSequenceMeta.CheckResult.ConfigurationTransformNotFound",
                    configurationTransform),
                transformMeta));
      } else {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "AddSequenceMeta.CheckResult.ConfigurationTransformSelected",
                    configurationTransform),
                transformMeta));
      }
    }

    if (fieldsMissing || info == null || info.isEmpty()) {
      return;
    }

    StringBuilder missing = new StringBuilder();
    appendMissingConfigurationField(missing, info, startField);
    appendMissingConfigurationField(missing, info, endField);
    appendMissingConfigurationField(missing, info, incrementField);
    if (!missing.isEmpty()) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(
                  PKG, "AddSequenceMeta.CheckResult.ConfigurationFieldsNotFound", missing),
              transformMeta));
    } else {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "AddSequenceMeta.CheckResult.ConfigurationFieldsFound"),
              transformMeta));
    }
  }

  private static void appendMissingConfigurationField(
      StringBuilder missing, IRowMeta info, String fieldName) {
    String name = Const.trim(fieldName);
    if (Utils.isEmpty(name) || info.indexOfValue(name) >= 0) {
      return;
    }
    if (!missing.isEmpty()) {
      missing.append(", ");
    }
    missing.append(name);
  }

  /**
   * Verify the database sequence. An unset connection is left to {@code
   * ReferencedDatabaseConnectionChecker}, which reports it with a code the linter can baseline.
   */
  private void checkDatabaseSequence(
      List<ICheckResult> remarks,
      TransformMeta transformMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    String resolvedConnection = variables.resolve(connection);
    if (Utils.isEmpty(resolvedConnection)) {
      return;
    }

    Database db = null;
    try {
      DatabaseMeta databaseMeta =
          metadataProvider.getSerializer(DatabaseMeta.class).load(resolvedConnection);
      db = new Database(loggingObject, variables, databaseMeta);
      db.connect();
      CheckResult cr;
      if (db.checkSequenceExists(variables.resolve(schemaName), variables.resolve(sequenceName))) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "AddSequenceMeta.CheckResult.SequenceExists.Title"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "AddSequenceMeta.CheckResult.SequenceCouldNotBeFound.Title", sequenceName),
                transformMeta);
      }
      remarks.add(cr);
    } catch (HopException e) {
      remarks.add(
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "AddSequenceMeta.CheckResult.UnableToConnectDB.Title")
                  + Const.CR
                  + e.getMessage(),
              transformMeta));
    } finally {
      if (db != null) {
        db.close();
      }
    }
  }

  @Override
  public SqlStatement getSqlStatements(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      IHopMetadataProvider metadataProvider) {
    SqlStatement retval = new SqlStatement(transformMeta.getName(), null, null);
    if (!databaseUsed) {
      return retval;
    }

    Database db = null;
    try {
      String resolvedConnection = variables.resolve(connection);
      DatabaseMeta databaseMeta = null;
      if (!Utils.isEmpty(resolvedConnection)) {
        databaseMeta = metadataProvider.getSerializer(DatabaseMeta.class).load(resolvedConnection);
      }
      retval.setDatabase(databaseMeta);
      if (databaseMeta != null) {
        db = new Database(loggingObject, variables, databaseMeta);
        db.connect();
        if (!db.checkSequenceExists(schemaName, sequenceName)) {
          String crTable =
              db.getCreateSequenceStatement(sequenceName, startAt, incrementBy, maxValue, true);
          retval.setSql(crTable);
        } else {
          retval.setSql(null); // Empty string means: nothing to do: set it to null...
        }
      } else {
        retval.setError(
            BaseMessages.getString(PKG, "AddSequenceMeta.ErrorMessage.NoConnectionDefined"));
      }
    } catch (HopException e) {
      retval.setError(
          BaseMessages.getString(PKG, "AddSequenceMeta.ErrorMessage.UnableToConnectDB")
              + Const.CR
              + e.getMessage());
    } finally {
      if (db != null) {
        db.close();
      }
    }

    return retval;
  }

  /**
   * Keeps {@link #configurationTransform} in sync when the info hop is drawn, split, or detached.
   * {@link #searchInfoAndTargetTransforms} resolves the stream from that name.
   */
  @Override
  public void handleStreamSelection(IStream stream) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    if (infoStreams.isEmpty() || stream == null || !infoStreams.contains(stream)) {
      return;
    }
    TransformMeta source = stream.getTransformMeta();
    if (source == null) {
      return;
    }
    setConfigurationTransform(source.getName());
    stream.setSubject(source.getName());
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    if (infoStreams.isEmpty()) {
      return;
    }
    IStream stream = infoStreams.get(0);
    if (!isConfigurationFromTransform()) {
      stream.setTransformMeta(null);
      return;
    }
    String lookupName = stream.getSubject();
    if (!Utils.isEmpty(configurationTransform)) {
      lookupName = configurationTransform;
      stream.setSubject(configurationTransform);
    }
    stream.setTransformMeta(TransformMeta.findTransform(transforms, Const.trim(lookupName)));
  }

  @Override
  public void convertIOMetaToTransformNames() {
    List<IStream> infoStreams = getTransformIOMeta().getInfoStreams();
    if (infoStreams.isEmpty()) {
      return;
    }
    String name = infoStreams.get(0).getTransformName();
    if (!Utils.isEmpty(name)) {
      configurationTransform = name;
    }
  }

  @Override
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta(false);
    if (ioMeta == null) {
      ioMeta = new TransformIOMeta(true, true, false, false, false, false);
      ioMeta.addStream(
          new Stream(
              StreamType.INFO,
              null,
              BaseMessages.getString(PKG, "AddSequenceMeta.InfoStream.Description"),
              StreamIcon.INFO,
              configurationTransform));
      setTransformIOMeta(ioMeta);
    }
    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
    // Keep the configuration info stream. Recreating it here drops the transform it points at.
  }

  /**
   * The configuration row does not have the same layout as the main input. Skip the safe-mode row
   * mixing check while that info hop is in use.
   */
  @Override
  public boolean excludeFromRowLayoutVerification() {
    return isConfigurationFromTransform();
  }
}
