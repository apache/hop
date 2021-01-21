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

package org.apache.hop.pipeline.transforms.addsequence;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Meta data for the Add Sequence transform.
 *
 * <p>Created on 13-may-2003
 */
@Transform(
    id = "Sequence",
    image = "addsequence.svg",
    name = "i18n::BaseTransform.TypeLongDesc.AddSequence",
    description = "i18n::BaseTransform.TypeTooltipDesc.AddSequence",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/addsequence.html")
public class AddSequenceMeta extends BaseTransformMeta
    implements ITransformMeta<AddSequence, AddSequenceData> {

  private static final Class<?> PKG = AddSequenceMeta.class; // For Translator

  private String valuename;

  private boolean useDatabase;
  private DatabaseMeta databaseMeta;
  private String schemaName;
  private String sequenceName;

  private boolean useCounter;
  private String counterName;
  private String startAt;
  private String incrementBy;
  private String maxValue;

  /** @return Returns the connection. */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /** @param connection The connection to set. */
  public void setDatabaseMeta(DatabaseMeta connection) {
    this.databaseMeta = connection;
  }

  /** @return Returns the incrementBy. */
  public String getIncrementBy() {
    return incrementBy;
  }

  /** @param incrementBy The incrementBy to set. */
  public void setIncrementBy(String incrementBy) {
    this.incrementBy = incrementBy;
  }

  /** @return Returns the maxValue. */
  public String getMaxValue() {
    return maxValue;
  }

  /** @param maxValue The maxValue to set. */
  public void setMaxValue(String maxValue) {
    this.maxValue = maxValue;
  }

  /** @return Returns the sequenceName. */
  public String getSequenceName() {
    return sequenceName;
  }

  /** @param sequenceName The sequenceName to set. */
  public void setSequenceName(String sequenceName) {
    this.sequenceName = sequenceName;
  }

  /** @param maxValue The maxValue to set. */
  public void setMaxValue(long maxValue) {
    this.maxValue = Long.toString(maxValue);
  }

  /** @param startAt The starting point of the sequence to set. */
  public void setStartAt(long startAt) {
    this.startAt = Long.toString(startAt);
  }

  /** @param incrementBy The incrementBy to set. */
  public void setIncrementBy(long incrementBy) {
    this.incrementBy = Long.toString(incrementBy);
  }

  /** @return Returns the start of the sequence. */
  public String getStartAt() {
    return startAt;
  }

  /** @param startAt The starting point of the sequence to set. */
  public void setStartAt(String startAt) {
    this.startAt = startAt;
  }

  /** @return Returns the useCounter. */
  public boolean isCounterUsed() {
    return useCounter;
  }

  /** @param useCounter The useCounter to set. */
  public void setUseCounter(boolean useCounter) {
    this.useCounter = useCounter;
  }

  /** @return Returns the useDatabase. */
  public boolean isDatabaseUsed() {
    return useDatabase;
  }

  /** @param useDatabase The useDatabase to set. */
  public void setUseDatabase(boolean useDatabase) {
    this.useDatabase = useDatabase;
  }

  /** @return Returns the valuename. */
  public String getValuename() {
    return valuename;
  }

  /** @param valuename The valuename to set. */
  public void setValuename(String valuename) {
    this.valuename = valuename;
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      valuename = XmlHandler.getTagValue(transformNode, "valuename");

      useDatabase = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "use_database"));
      String conn = XmlHandler.getTagValue(transformNode, "connection");
      databaseMeta = DatabaseMeta.loadDatabase(metadataProvider, conn);
      schemaName = XmlHandler.getTagValue(transformNode, "schema");
      sequenceName = XmlHandler.getTagValue(transformNode, "seqname");

      useCounter = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "use_counter"));
      counterName = XmlHandler.getTagValue(transformNode, "counter_name");
      startAt = XmlHandler.getTagValue(transformNode, "start_at");
      incrementBy = XmlHandler.getTagValue(transformNode, "increment_by");
      maxValue = XmlHandler.getTagValue(transformNode, "max_value");

      // TODO startAt = Const.toLong(XmlHandler.getTagValue(transformNode, "start_at"), 1);
      // incrementBy = Const.toLong(XmlHandler.getTagValue(transformNode, "increment_by"), 1);
      // maxValue = Const.toLong(XmlHandler.getTagValue(transformNode, "max_value"), 999999999L);
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "AddSequenceMeta.Exception.ErrorLoadingTransformMeta"), e);
    }
  }

  @Override
  public void setDefault() {
    valuename = "valuename";

    useDatabase = false;
    schemaName = "";
    sequenceName = "SEQ_";
    databaseMeta = null;

    useCounter = true;
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
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    IValueMeta v = new ValueMetaInteger(valuename);
    // v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0); Removed for 2.5.x compatibility reasons.
    v.setOrigin(name);
    row.addValueMeta(v);
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("      ").append(XmlHandler.addTagValue("valuename", valuename));
    retval.append("      ").append(XmlHandler.addTagValue("use_database", useDatabase));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "connection", databaseMeta == null ? "" : databaseMeta.getName()));
    retval.append("      ").append(XmlHandler.addTagValue("schema", schemaName));
    retval.append("      ").append(XmlHandler.addTagValue("seqname", sequenceName));

    retval.append("      ").append(XmlHandler.addTagValue("use_counter", useCounter));
    retval.append("      ").append(XmlHandler.addTagValue("counter_name", counterName));
    retval.append("      ").append(XmlHandler.addTagValue("start_at", startAt));
    retval.append("      ").append(XmlHandler.addTagValue("increment_by", incrementBy));
    retval.append("      ").append(XmlHandler.addTagValue("max_value", maxValue));

    return retval.toString();
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
    if (useDatabase) {
      Database db = new Database(loggingObject, variables, databaseMeta );
      try {
        db.connect();
        if (db.checkSequenceExists(
            variables.resolve(schemaName), variables.resolve(sequenceName))) {
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
                      PKG,
                      "AddSequenceMeta.CheckResult.SequenceCouldNotBeFound.Title",
                      sequenceName),
                  transformMeta);
        }
      } catch (HopException e) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "AddSequenceMeta.CheckResult.UnableToConnectDB.Title")
                    + Const.CR
                    + e.getMessage(),
                transformMeta);
      } finally {
        db.disconnect();
      }
      remarks.add(cr);
    }

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
  }

  @Override
  public SqlStatement getSqlStatements(
      IVariables variables,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      IHopMetadataProvider metadataProvider) {
    SqlStatement retval =
        new SqlStatement(transformMeta.getName(), databaseMeta, null); // default: nothing to do!

    if (useDatabase) {
      // Otherwise, don't bother!
      if (databaseMeta != null) {
        Database db = new Database(loggingObject, variables, databaseMeta );
        try {
          db.connect();
          if (!db.checkSequenceExists(schemaName, sequenceName)) {
            String crTable =
                db.getCreateSequenceStatement(sequenceName, startAt, incrementBy, maxValue, true);
            retval.setSql(crTable);
          } else {
            retval.setSql(null); // Empty string means: nothing to do: set it to null...
          }
        } catch (HopException e) {
          retval.setError(
              BaseMessages.getString(PKG, "AddSequenceMeta.ErrorMessage.UnableToConnectDB")
                  + Const.CR
                  + e.getMessage());
        } finally {
          db.disconnect();
        }
      } else {
        retval.setError(
            BaseMessages.getString(PKG, "AddSequenceMeta.ErrorMessage.NoConnectionDefined"));
      }
    }

    return retval;
  }

  @Override
  public AddSequence createTransform(
      TransformMeta transformMeta,
      AddSequenceData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new AddSequence(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public AddSequenceData getTransformData() {
    return new AddSequenceData();
  }

  @Override
  public DatabaseMeta[] getUsedDatabaseConnections() {
    if (databaseMeta != null) {
      return new DatabaseMeta[] {databaseMeta};
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /** @return the counterName */
  public String getCounterName() {
    return counterName;
  }

  /** @param counterName the counterName to set */
  public void setCounterName(String counterName) {
    this.counterName = counterName;
  }

  /** @return the schemaName */
  public String getSchemaName() {
    return schemaName;
  }

  /** @param schemaName the schemaName to set */
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }
}
