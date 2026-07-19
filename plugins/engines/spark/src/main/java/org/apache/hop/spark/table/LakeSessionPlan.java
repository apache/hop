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

package org.apache.hop.spark.table;

import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.metadata.SparkCatalog;
import org.apache.hop.spark.pipeline.HopPipelineMetaToSparkConverter;
import org.apache.hop.spark.transforms.table.SparkLakeTableInputMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableMaintenanceMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableMergeMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableOutputMeta;
import org.apache.hop.spark.util.SparkConst;
import org.apache.spark.sql.SparkSession;
import org.w3c.dom.Node;

/**
 * Pre-session scan of active lake transforms: formats, referenced {@link SparkCatalog} metadata,
 * and session conf for Delta / Iceberg PATH + TABLE modes.
 */
public final class LakeSessionPlan {

  private static final Set<String> LAKE_PLUGIN_IDS =
      Set.of(
          SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID,
          SparkConst.SPARK_LAKE_TABLE_OUTPUT_PLUGIN_ID,
          SparkConst.SPARK_LAKE_TABLE_MERGE_PLUGIN_ID,
          SparkConst.SPARK_LAKE_TABLE_MAINTENANCE_PLUGIN_ID);

  private final Set<String> formatsNeeded = new LinkedHashSet<>();
  private final Map<String, SparkCatalog> catalogsByMetaName = new LinkedHashMap<>();
  private boolean needsTableMode;

  private LakeSessionPlan() {}

  public Set<String> getFormatsNeeded() {
    return formatsNeeded;
  }

  public Map<String, SparkCatalog> getCatalogsByMetaName() {
    return catalogsByMetaName;
  }

  public boolean isEmpty() {
    return formatsNeeded.isEmpty();
  }

  public boolean needsDelta() {
    return formatsNeeded.contains(SparkLakeFormats.FORMAT_DELTA);
  }

  public boolean needsIceberg() {
    return formatsNeeded.contains(SparkLakeFormats.FORMAT_ICEBERG);
  }

  public boolean needsTableMode() {
    return needsTableMode;
  }

  /** Scan active transforms for lake plugin ids, formats, and SparkCatalog metadata references. */
  public static LakeSessionPlan from(
      PipelineMeta pipelineMeta, IHopMetadataProvider metadataProvider) throws HopException {
    return from(pipelineMeta, metadataProvider, null);
  }

  public static LakeSessionPlan from(
      PipelineMeta pipelineMeta, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopException {
    LakeSessionPlan plan = new LakeSessionPlan();
    if (pipelineMeta == null) {
      return plan;
    }
    List<TransformMeta> active =
        HopPipelineMetaToSparkConverter.collectActiveTransforms(pipelineMeta);
    for (TransformMeta tm : active) {
      String id = tm.getTransformPluginId();
      if (id == null || !LAKE_PLUGIN_IDS.contains(id)) {
        continue;
      }
      if (SparkConst.SPARK_LAKE_TABLE_INPUT_PLUGIN_ID.equals(id)) {
        SparkLakeTableInputMeta meta = new SparkLakeTableInputMeta();
        loadMeta(meta, tm, metadataProvider);
        SparkLakeTableSupport.collectFormat(meta.getFormat(), plan.formatsNeeded);
        collectCatalogRef(
            plan,
            metadataProvider,
            variables,
            meta.getIdentifierMode(),
            meta.getCatalogMetadataName());
      } else if (SparkConst.SPARK_LAKE_TABLE_OUTPUT_PLUGIN_ID.equals(id)) {
        SparkLakeTableOutputMeta meta = new SparkLakeTableOutputMeta();
        loadMeta(meta, tm, metadataProvider);
        SparkLakeTableSupport.collectFormat(meta.getFormat(), plan.formatsNeeded);
        collectCatalogRef(
            plan,
            metadataProvider,
            variables,
            meta.getIdentifierMode(),
            meta.getCatalogMetadataName());
      } else if (SparkConst.SPARK_LAKE_TABLE_MERGE_PLUGIN_ID.equals(id)) {
        SparkLakeTableMergeMeta meta = new SparkLakeTableMergeMeta();
        loadMeta(meta, tm, metadataProvider);
        SparkLakeTableSupport.collectFormat(meta.getFormat(), plan.formatsNeeded);
        collectCatalogRef(
            plan,
            metadataProvider,
            variables,
            meta.getIdentifierMode(),
            meta.getCatalogMetadataName());
      } else if (SparkConst.SPARK_LAKE_TABLE_MAINTENANCE_PLUGIN_ID.equals(id)) {
        SparkLakeTableMaintenanceMeta meta = new SparkLakeTableMaintenanceMeta();
        loadMeta(meta, tm, metadataProvider);
        SparkLakeTableSupport.collectFormat(meta.getFormat(), plan.formatsNeeded);
        collectCatalogRef(
            plan,
            metadataProvider,
            variables,
            meta.getIdentifierMode(),
            meta.getCatalogMetadataName());
      }
    }
    return plan;
  }

  private static void collectCatalogRef(
      LakeSessionPlan plan,
      IHopMetadataProvider metadataProvider,
      IVariables variables,
      String identifierMode,
      String catalogMetadataName)
      throws HopException {
    String mode;
    try {
      mode = SparkLakeTableSupport.normalizeIdentifierMode(identifierMode);
    } catch (HopException e) {
      mode = SparkLakeTableInputMeta.MODE_PATH;
    }
    if (SparkLakeTableInputMeta.MODE_TABLE.equals(mode)) {
      plan.needsTableMode = true;
    }
    if (StringUtils.isEmpty(catalogMetadataName) || metadataProvider == null) {
      return;
    }
    String metaName =
        variables != null ? variables.resolve(catalogMetadataName) : catalogMetadataName.trim();
    if (StringUtils.isEmpty(metaName) || plan.catalogsByMetaName.containsKey(metaName)) {
      return;
    }
    try {
      SparkCatalog catalog = metadataProvider.getSerializer(SparkCatalog.class).load(metaName);
      if (catalog == null) {
        throw new HopException(
            "SparkCatalog metadata '"
                + metaName
                + "' not found (referenced by a Lake Table transform)");
      }
      plan.catalogsByMetaName.put(metaName, catalog);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to load SparkCatalog metadata '" + metaName + "'", e);
    }
  }

  /** Classpath probe for all formats in this plan. */
  public void verifyClasspath(ClassLoader classLoader) throws HopException {
    if (formatsNeeded.isEmpty()) {
      return;
    }
    SparkLakeConnectorProbe.verifyClasspath(formatsNeeded, classLoader);
  }

  /**
   * Apply Delta / Iceberg session conf and referenced SparkCatalog entries on a new hop-run session
   * builder.
   */
  public void applyToBuilder(SparkSession.Builder builder) throws HopException {
    applyToBuilder(builder, null);
  }

  public void applyToBuilder(SparkSession.Builder builder, IVariables variables)
      throws HopException {
    if (formatsNeeded.isEmpty() && catalogsByMetaName.isEmpty()) {
      return;
    }
    LinkedHashSet<String> extensions = new LinkedHashSet<>();
    if (needsDelta()) {
      extensions.add(SparkLakeFormats.DELTA_EXTENSION);
      builder.config(SparkLakeFormats.SPARK_CONF_SPARK_CATALOG, SparkLakeFormats.DELTA_CATALOG);
    }
    if (needsIceberg()) {
      extensions.add(SparkLakeFormats.ICEBERG_EXTENSIONS);
      // PATH-mode helper catalog (also useful as default for path identifiers)
      builder.config(
          SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG, SparkLakeFormats.ICEBERG_CATALOG);
      builder.config(SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG_TYPE, "hadoop");
      builder.config(
          SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG_WAREHOUSE,
          defaultIcebergPathWarehouse());
    }
    if (!extensions.isEmpty()) {
      builder.config(SparkLakeFormats.SPARK_CONF_EXTENSIONS, String.join(",", extensions));
    }
    for (SparkCatalog catalog : catalogsByMetaName.values()) {
      SparkCatalogApplier.applyToBuilder(builder, catalog, variables);
    }
  }

  /**
   * For an active/reused session (spark-submit): require connector classes and Delta conf; register
   * missing Iceberg path catalog and user SparkCatalog entries when possible.
   */
  public void verifyActiveSession(SparkSession session, ILogChannel log) throws HopException {
    verifyActiveSession(session, log, null);
  }

  public void verifyActiveSession(SparkSession session, ILogChannel log, IVariables variables)
      throws HopException {
    if (formatsNeeded.isEmpty() && catalogsByMetaName.isEmpty()) {
      return;
    }
    ClassLoader cl = LakeSessionPlan.class.getClassLoader();
    if (!formatsNeeded.isEmpty()) {
      verifyClasspath(cl);
    }

    if (needsDelta()) {
      String catalog = session.conf().get(SparkLakeFormats.SPARK_CONF_SPARK_CATALOG, "");
      String extensions = session.conf().get(SparkLakeFormats.SPARK_CONF_EXTENSIONS, "");
      boolean catalogOk = catalog != null && catalog.contains("DeltaCatalog");
      boolean extOk =
          extensions != null
              && extensions.toLowerCase(Locale.ROOT).contains("deltasparksessionextension");
      if (!catalogOk || !extOk) {
        throw new HopException(
            "Active SparkSession is missing Delta session configuration required for lake"
                + " transforms. Set when launching (spark-submit --conf):\n"
                + "  "
                + SparkLakeFormats.SPARK_CONF_EXTENSIONS
                + "="
                + SparkLakeFormats.DELTA_EXTENSION
                + "\n  "
                + SparkLakeFormats.SPARK_CONF_SPARK_CATALOG
                + "="
                + SparkLakeFormats.DELTA_CATALOG
                + "\nAlso ensure the Delta connector is on the driver/executor classpath"
                + " (--packages io.delta:delta-spark_4.1_2.13:4.3.1). Current"
                + " spark.sql.extensions='"
                + extensions
                + "', spark.sql.catalog.spark_catalog='"
                + catalog
                + "'.");
      }
    }

    if (needsIceberg()) {
      String extensions = session.conf().get(SparkLakeFormats.SPARK_CONF_EXTENSIONS, "");
      boolean extOk =
          extensions != null
              && extensions.toLowerCase(Locale.ROOT).contains("icebergsparksessionextensions");
      if (!extOk) {
        throw new HopException(
            "Active SparkSession is missing Iceberg extensions required for lake transforms. Set"
                + " when launching (spark-submit --conf):\n"
                + "  "
                + SparkLakeFormats.SPARK_CONF_EXTENSIONS
                + "="
                + SparkLakeFormats.ICEBERG_EXTENSIONS
                + "\nAlso --packages"
                + " org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0."
                + " Current spark.sql.extensions='"
                + extensions
                + "'.");
      }
      String pathCat = session.conf().get(SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG, "");
      if (StringUtils.isEmpty(pathCat)) {
        if (log != null) {
          log.logBasic(
              "Registering Iceberg PATH catalog '"
                  + SparkLakeFormats.ICEBERG_PATH_CATALOG_NAME
                  + "' on active session");
        }
        SparkLakeTableSupport.ensureIcebergPathCatalog(session);
      }
    }

    // Apply / verify user SparkCatalog metadata (TABLE mode)
    for (Map.Entry<String, SparkCatalog> e : catalogsByMetaName.entrySet()) {
      SparkCatalog cat = e.getValue();
      String sparkName = StringUtils.defaultIfBlank(cat.getCatalogName(), cat.getName());
      if (variables != null) {
        sparkName = variables.resolve(sparkName);
      }
      String confKey = "spark.sql.catalog." + sparkName;
      String existing = session.conf().get(confKey, "");
      if (StringUtils.isEmpty(existing)) {
        if (log != null) {
          log.logBasic(
              "Registering SparkCatalog '"
                  + e.getKey()
                  + "' (spark name="
                  + sparkName
                  + ") on active session");
        }
        try {
          SparkCatalogApplier.applyToSession(session, cat, variables);
        } catch (Exception ex) {
          throw new HopException(
              "Unable to register SparkCatalog '"
                  + e.getKey()
                  + "' on active SparkSession. Pass spark.sql.catalog."
                  + sparkName
                  + ".* via spark-submit --conf. Cause: "
                  + ex.getMessage(),
              ex);
        }
      }
    }
  }

  static String defaultIcebergPathWarehouse() {
    return Paths.get(System.getProperty("java.io.tmpdir"), "hop-iceberg-path-catalog-warehouse")
        .toAbsolutePath()
        .normalize()
        .toUri()
        .toString();
  }

  private static void loadMeta(
      ITransformMeta meta, TransformMeta transformMeta, IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      String xml = transformMeta.getXml();
      Node node = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), TransformMeta.XML_TAG);
      meta.loadXml(node, metadataProvider);
    } catch (Exception e) {
      ITransformMeta live = transformMeta.getTransform();
      if (meta instanceof SparkLakeTableInputMeta target
          && live instanceof SparkLakeTableInputMeta in) {
        target.setFormat(in.getFormat());
        target.setIdentifierMode(in.getIdentifierMode());
        target.setTablePath(in.getTablePath());
        target.setTableIdentifier(in.getTableIdentifier());
        target.setCatalogMetadataName(in.getCatalogMetadataName());
        target.setTimeTravelType(in.getTimeTravelType());
        target.setTimeTravelVersion(in.getTimeTravelVersion());
        target.setTimeTravelTimestamp(in.getTimeTravelTimestamp());
        target.setExtraOptions(in.getExtraOptions());
        target.setFields(in.getFields());
        return;
      }
      if (meta instanceof SparkLakeTableOutputMeta target
          && live instanceof SparkLakeTableOutputMeta out) {
        target.setFormat(out.getFormat());
        target.setIdentifierMode(out.getIdentifierMode());
        target.setTablePath(out.getTablePath());
        target.setTableIdentifier(out.getTableIdentifier());
        target.setCatalogMetadataName(out.getCatalogMetadataName());
        target.setSaveMode(out.getSaveMode());
        target.setExtraOptions(out.getExtraOptions());
        target.setPartitionByColumns(out.getPartitionByColumns());
        target.setCoalescePartitions(out.getCoalescePartitions());
        return;
      }
      if (meta instanceof SparkLakeTableMergeMeta target
          && live instanceof SparkLakeTableMergeMeta merge) {
        target.setFormat(merge.getFormat());
        target.setIdentifierMode(merge.getIdentifierMode());
        target.setTablePath(merge.getTablePath());
        target.setTableIdentifier(merge.getTableIdentifier());
        target.setCatalogMetadataName(merge.getCatalogMetadataName());
        target.setMergeCondition(merge.getMergeCondition());
        target.setMatchedAction(merge.getMatchedAction());
        target.setNotMatchedAction(merge.getNotMatchedAction());
        target.setNotMatchedBySourceAction(merge.getNotMatchedBySourceAction());
        target.setRawMergeSql(merge.getRawMergeSql());
        return;
      }
      if (meta instanceof SparkLakeTableMaintenanceMeta target
          && live instanceof SparkLakeTableMaintenanceMeta maint) {
        target.setFormat(maint.getFormat());
        target.setIdentifierMode(maint.getIdentifierMode());
        target.setTablePath(maint.getTablePath());
        target.setTableIdentifier(maint.getTableIdentifier());
        target.setCatalogMetadataName(maint.getCatalogMetadataName());
        target.setOperation(maint.getOperation());
        target.setRetentionHours(maint.getRetentionHours());
        target.setRetainLast(maint.getRetainLast());
        target.setWhereClause(maint.getWhereClause());
        target.setZOrderColumns(maint.getZOrderColumns());
        target.setAcknowledgeDestructive(maint.isAcknowledgeDestructive());
        return;
      }
      throw new HopException(
          "Unable to load lake transform metadata for '" + transformMeta.getName() + "'", e);
    }
  }
}
