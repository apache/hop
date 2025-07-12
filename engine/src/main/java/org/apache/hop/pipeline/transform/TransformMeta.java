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

package org.apache.hop.pipeline.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.base.IBaseMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.IAttributes;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;
import org.apache.hop.core.attributes.AttributesUtil;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginLoaderException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.gui.IGuiPosition;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transforms.missing.Missing;
import org.apache.hop.resource.IResourceExport;
import org.apache.hop.resource.IResourceHolder;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/** This class contains everything that is needed to define a transform. */
public class TransformMeta
    implements Cloneable,
        Comparable<TransformMeta>,
        IGuiPosition,
        ICheckResultSource,
        IResourceExport,
        IResourceHolder,
        IAttributes,
        IBaseMeta {
  private static final Class<?> PKG = TransformMeta.class;

  public static final String XML_TAG = "transform";
  public static final String STRING_ID_MAPPING = "Mapping";
  public static final String STRING_ID_SINGLE_THREADER = "SingleThreader";
  public static final String STRING_ID_ETL_META_INJECT = "MetaInject";
  public static final String STRING_ID_WORKFLOW_EXECUTOR = "WorkflowExecutor";
  public static final String STRING_ID_MAPPING_INPUT = "MappingInput";
  public static final String STRING_ID_MAPPING_OUTPUT = "MappingOutput";
  private static final String CONST_TARGET_TRANSFORM_PARTITIONING = "target_transform_partitioning";

  @HopMetadataProperty(key = "type")
  private String transformPluginId; // --> transform plugin id

  @HopMetadataProperty private String name;

  @HopMetadataProperty(inline = true)
  private ITransformMeta transform;

  @HopMetadataProperty private boolean selected;

  @HopMetadataProperty private boolean distributes;

  private boolean isDeprecated;

  private String suggestion = "";

  private IRowDistribution rowDistribution;

  @HopMetadataProperty(key = "copies")
  private String copiesString;

  @HopMetadataProperty(key = "GUI")
  private Point location;

  @HopMetadataProperty private String description;

  private boolean terminator;

  @HopMetadataProperty(key = "partitioning")
  private TransformPartitioningMeta transformPartitioningMeta;

  @HopMetadataProperty(key = CONST_TARGET_TRANSFORM_PARTITIONING)
  private TransformPartitioningMeta targetTransformPartitioningMeta;

  private TransformErrorMeta transformErrorMeta;

  private PipelineMeta parentPipelineMeta;

  private Integer copiesCache = null;

  /** protected for easy access by subclasses */
  protected Map<String, Map<String, String>> attributesMap;

  /**
   * @param transformId The plugin ID of the transform
   * @param transformName The name of the new transform
   * @param transform The transform metadata interface to use (TextFileInputMeta, etc)
   */
  public TransformMeta(String transformId, String transformName, ITransformMeta transform) {
    this(transformName, transform);
    if (this.transformPluginId == null) {
      this.transformPluginId = transformId;
    }
  }

  /**
   * @param transformName The name of the new transform
   * @param transform The transform metadata interface to use (TextFileInputMeta, etc)
   */
  @SuppressWarnings("java:S106")
  public TransformMeta(String transformName, ITransformMeta transform) {
    if (transform != null) {
      PluginRegistry registry = PluginRegistry.getInstance();
      this.transformPluginId = registry.getPluginId(TransformPluginType.class, transform);
      if (this.transformPluginId == null) {
        System.err.println(
            "WARNING: transform plugin class '"
                + transform.getClass().getName()
                + "' couldn't be found in the plugin registry. Check the classpath.");
      }
    }
    this.name = transformName;
    setTransform(transform);

    selected = false;
    distributes = true;
    copiesString = "1";
    location = new Point(0, 0);
    description = null;
    transformPartitioningMeta = new TransformPartitioningMeta();
    targetTransformPartitioningMeta = null;

    attributesMap = new HashMap<>();
  }

  public TransformMeta() {
    this(null, null, null);
  }

  public String getXml() throws HopException {

    StringBuilder xml = new StringBuilder(200);

    xml.append("  ").append(XmlHandler.openTag(XML_TAG)).append(Const.CR);
    xml.append("    ").append(XmlHandler.addTagValue("name", getName()));
    xml.append("    ").append(XmlHandler.addTagValue("type", getTransformPluginId()));
    xml.append("    ").append(XmlHandler.addTagValue("description", description));
    xml.append("    ").append(XmlHandler.addTagValue("distribute", distributes));
    xml.append("    ")
        .append(
            XmlHandler.addTagValue(
                "custom_distribution", rowDistribution == null ? null : rowDistribution.getCode()));
    xml.append("    ").append(XmlHandler.addTagValue("copies", copiesString));

    xml.append(transformPartitioningMeta.getXml());
    if (targetTransformPartitioningMeta != null) {
      xml.append(XmlHandler.openTag(CONST_TARGET_TRANSFORM_PARTITIONING))
          .append(targetTransformPartitioningMeta.getXml())
          .append(XmlHandler.closeTag(CONST_TARGET_TRANSFORM_PARTITIONING));
    }

    xml.append(transform.getXml());

    xml.append(AttributesUtil.getAttributesXml(attributesMap));

    xml.append("    ").append(XmlHandler.openTag("GUI")).append(Const.CR);
    xml.append("      ").append(XmlHandler.addTagValue("xloc", location.x));
    xml.append("      ").append(XmlHandler.addTagValue("yloc", location.y));
    xml.append("    ").append(XmlHandler.closeTag("GUI")).append(Const.CR);
    xml.append("    ").append(XmlHandler.closeTag(XML_TAG)).append(Const.CR).append(Const.CR);

    return xml.toString();
  }

  /**
   * Read the transform data from XML
   *
   * @param transformNode The XML transform node.
   * @param metadataProvider where to get the metadata.
   */
  public TransformMeta(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException, HopPluginLoaderException {
    this();
    PluginRegistry registry = PluginRegistry.getInstance();

    try {
      name = XmlHandler.getTagValue(transformNode, "name");
      transformPluginId = XmlHandler.getTagValue(transformNode, "type");

      // Create a new ITransformMeta object...
      IPlugin transformPlugin =
          registry.findPluginWithId(TransformPluginType.class, transformPluginId, true);

      if (transformPlugin == null) {
        setTransform(new Missing(name, transformPluginId));
      } else {
        setTransform((ITransformMeta) registry.loadClass(transformPlugin));
      }
      if (this.transform != null) {
        if (transformPlugin != null) {
          transformPluginId =
              transformPlugin
                  .getIds()[0]; // revert to the default in case we loaded an alternate version
          suggestion = Const.NVL(transformPlugin.getSuggestion(), "");
        }

        // Load the specifics from XML...
        if (transform != null) {
          transform.loadXml(transformNode, metadataProvider);
        }

        /* Handle info general to all transform types... */
        description = XmlHandler.getTagValue(transformNode, "description");
        copiesString = XmlHandler.getTagValue(transformNode, "copies");
        String sdistri = XmlHandler.getTagValue(transformNode, "distribute");
        distributes = "Y".equalsIgnoreCase(sdistri);
        if (sdistri == null) {
          distributes = true; // default=distribute
        }

        // Load the attribute groups map
        //
        attributesMap =
            AttributesUtil.loadAttributes(
                XmlHandler.getSubNode(transformNode, AttributesUtil.XML_TAG));

        // Determine the row distribution
        //
        String rowDistributionCode = XmlHandler.getTagValue(transformNode, "custom_distribution");
        rowDistribution =
            PluginRegistry.getInstance()
                .loadClass(
                    RowDistributionPluginType.class, rowDistributionCode, IRowDistribution.class);

        // Handle GUI information: location x and y coordinates
        //

        String xloc = XmlHandler.getTagValue(transformNode, "GUI", "xloc");
        String yloc = XmlHandler.getTagValue(transformNode, "GUI", "yloc");
        int x;
        try {
          x = Integer.parseInt(xloc);
        } catch (Exception e) {
          x = 0;
        }
        int y;
        try {
          y = Integer.parseInt(yloc);
        } catch (Exception e) {
          y = 0;
        }
        location = new Point(x, y);

        // The partitioning information?
        //
        Node partNode = XmlHandler.getSubNode(transformNode, "partitioning");
        transformPartitioningMeta = new TransformPartitioningMeta(partNode, metadataProvider);

        // Target partitioning information?
        //
        Node targetPartNode =
            XmlHandler.getSubNode(transformNode, CONST_TARGET_TRANSFORM_PARTITIONING);
        partNode = XmlHandler.getSubNode(targetPartNode, "partitioning");
        if (partNode != null) {
          targetTransformPartitioningMeta =
              new TransformPartitioningMeta(partNode, metadataProvider);
        }
      }
    } catch (HopPluginLoaderException e) {
      throw e;
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "TransformMeta.Exception.UnableToLoadTransformMeta") + e, e);
    }
  }

  public static TransformMeta fromXml(String metaXml) {
    Document doc;
    try {
      doc = XmlHandler.loadXmlString(metaXml);
      Node transformNode = XmlHandler.getSubNode(doc, XML_TAG);
      return new TransformMeta(transformNode, null);
    } catch (HopXmlException | HopPluginLoaderException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets the number of parallel copies that this transform will be launched with.
   *
   * @param c The number of copies.
   */
  public void setCopies(int c) {
    setChanged();
    copiesString = Integer.toString(c);
    copiesCache = c;
  }

  /**
   * Get the number of copies to start of a transform. This takes into account the partitioning
   * logic.
   *
   * @return the number of transform copies to start.
   */
  public int getCopies(IVariables variables) {
    // If the transform is partitioned, that's going to determine the number of copies, nothing
    // else...
    //
    if (isPartitioned() && getTransformPartitioningMeta().getPartitionSchema() != null) {
      List<String> partitionIDs =
          getTransformPartitioningMeta().getPartitionSchema().calculatePartitionIds(variables);
      if (partitionIDs != null
          && !partitionIDs.isEmpty()) { // these are the partitions the transform can "reach"
        return partitionIDs.size();
      }
    }

    if (copiesCache != null) {
      return copiesCache.intValue();
    }

    if (parentPipelineMeta != null) {
      // Return -1 to indicate that the variable or string value couldn't be converted to number
      //
      copiesCache = Const.toInt(variables.resolve(copiesString), -1);
    } else {
      copiesCache = Const.toInt(copiesString, 1);
    }

    return copiesCache;
  }

  /**
   * Two transforms are equal if their names are equal.
   *
   * @return true if the two transforms are equal.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    TransformMeta transformMeta = (TransformMeta) obj;
    // getName() is returning transformName, matching the hashCode() algorithm
    return getName().equalsIgnoreCase(transformMeta.getName());
  }

  @Override
  public int hashCode() {
    return name.toLowerCase().hashCode();
  }

  @Override
  public int compareTo(TransformMeta o) {
    return toString().compareTo(o.toString());
  }

  public synchronized boolean hasChanged() {
    // Check both the wrapper level changed flag and the inner metadata
    ITransformMeta meta = this.getTransform();
    return meta != null && meta.hasChanged();
  }

  public synchronized void setChanged(boolean ch) {
    // Propagate to inner metadata
    ITransformMeta meta = this.getTransform();
    if (meta != null && ch) {
      meta.setChanged();
    }
  }

  public synchronized void setChanged() {
    setChanged(true);
  }

  public boolean chosesTargetTransforms() {
    if (getTransform() != null) {
      List<IStream> targetStreams = getTransform().getTransformIOMeta().getTargetStreams();
      return targetStreams.isEmpty();
    }
    return false;
  }

  @Override
  public Object clone() {
    // Use the copy factory for improved cloning with proper state preservation
    return org.apache.hop.pipeline.transform.copy.DefaultTransformMetaCopyFactory.getInstance()
        .copy(this);
  }

  public synchronized void replaceMeta(TransformMeta transformMeta) {
    // Use the copy factory to replace metadata with proper state preservation
    TransformMeta copy =
        org.apache.hop.pipeline.transform.copy.DefaultTransformMetaCopyFactory.getInstance()
            .copy(transformMeta, org.apache.hop.pipeline.transform.copy.CopyContext.SAME_PIPELINE);

    // Copy all fields from the generated copy to this instance
    this.transformPluginId = copy.transformPluginId;
    this.name = copy.name;
    this.transform = copy.transform;
    this.selected = copy.selected;
    this.distributes = copy.distributes;
    this.setRowDistribution(copy.getRowDistribution());
    this.copiesString = copy.copiesString;
    this.copiesCache = null; // force re-calculation
    this.location = copy.location;
    this.description = copy.description;
    this.terminator = copy.terminator;
    this.transformPartitioningMeta = copy.transformPartitioningMeta;
    this.targetTransformPartitioningMeta = copy.targetTransformPartitioningMeta;
    this.transformErrorMeta = copy.transformErrorMeta;
    this.attributesMap = copy.attributesMap;

    // Ensure parent references are maintained for this instance
    if (this.transform != null) {
      this.transform.setParentTransformMeta(this);
    }
  }

  private static Map<String, Map<String, String>> copyStringMap(
      Map<String, Map<String, String>> map) {
    if (map == null) {
      return new HashMap<>();
    }

    Map<String, Map<String, String>> result = new HashMap<>(map.size());
    for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
      Map<String, String> value = entry.getValue();
      HashMap<String, String> copy = (value == null) ? null : new HashMap<>(value);
      result.put(entry.getKey(), copy);
    }
    return result;
  }

  public ITransformMeta getTransform() {
    return transform;
  }

  public void setTransform(ITransformMeta transform) {
    this.transform = transform;
    if (transform != null) {
      this.transform.setParentTransformMeta(this);

      // Check if transform is deprecated by annotation
      Deprecated deprecated = transform.getClass().getDeclaredAnnotation(Deprecated.class);
      if (deprecated != null) {
        this.isDeprecated = true;
      }
    }
  }

  public String getTransformPluginId() {
    return transformPluginId;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String sname) {
    name = sname;
  }

  @Override
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void setSelected(boolean sel) {
    selected = sel;
  }

  public void flipSelected() {
    selected = !selected;
  }

  @Override
  public boolean isSelected() {
    return selected;
  }

  public void setTerminator() {
    setTerminator(true);
  }

  public void setTerminator(boolean t) {
    terminator = t;
  }

  public boolean hasTerminator() {
    return terminator;
  }

  @Override
  public void setLocation(int x, int y) {
    int nx = (x >= 0 ? x : 0);
    int ny = (y >= 0 ? y : 0);

    Point loc = new Point(nx, ny);
    if (!loc.equals(location)) {
      setChanged();
    }
    location = loc;
  }

  @Override
  public void setLocation(Point loc) {
    if (loc != null && !loc.equals(location)) {
      setChanged();
    }
    location = loc;
  }

  @Override
  public Point getLocation() {
    return location;
  }

  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    transform.check(
        remarks, pipelineMeta, this, prev, input, output, info, variables, metadataProvider);
  }

  @Override
  public String toString() {
    if (getName() == null) {
      return getClass().getName();
    }
    return getName();
  }

  /**
   * @return true is the transform is partitioned
   */
  public boolean isPartitioned() {
    return transformPartitioningMeta != null && transformPartitioningMeta.isPartitioned();
  }

  /**
   * @return true is the transform is partitioned
   */
  public boolean isTargetPartitioned() {
    return targetTransformPartitioningMeta != null
        && targetTransformPartitioningMeta.isPartitioned();
  }

  /**
   * @return the transformPartitioningMeta
   */
  public TransformPartitioningMeta getTransformPartitioningMeta() {
    return transformPartitioningMeta;
  }

  /**
   * @param transformPartitioningMeta the transformPartitioningMeta to set
   */
  public void setTransformPartitioningMeta(TransformPartitioningMeta transformPartitioningMeta) {
    this.transformPartitioningMeta = transformPartitioningMeta;
  }

  /**
   * @return the distributes
   */
  public boolean isDistributes() {
    return distributes;
  }

  /**
   * @param distributes the distributes to set
   */
  public void setDistributes(boolean distributes) {
    if (this.distributes != distributes) {
      this.distributes = distributes;
      setChanged();
    }
  }

  /**
   * @return the TransformErrorMeta error handling metadata for this transform
   */
  public TransformErrorMeta getTransformErrorMeta() {
    return transformErrorMeta;
  }

  /**
   * @param transformErrorMeta the error handling metadata for this transform
   */
  public void setTransformErrorMeta(TransformErrorMeta transformErrorMeta) {
    this.transformErrorMeta = transformErrorMeta;
  }

  /**
   * Find a transform with its name in a given ArrayList of transforms
   *
   * @param transforms The List of transforms to search
   * @param transformName The name of the transform
   * @return The transform if it was found, null if nothing was found
   */
  public static final TransformMeta findTransform(
      List<TransformMeta> transforms, String transformName) {
    if (transforms == null) {
      return null;
    }

    for (TransformMeta transformMeta : transforms) {
      if (transformMeta.getName().equalsIgnoreCase(transformName)) {
        return transformMeta;
      }
    }
    return null;
  }

  public boolean supportsMultiCopyExecution() {
    return transform.supportsMultiCopyExecution();
  }

  public boolean supportsErrorHandling() {
    return transform.supportsErrorHandling();
  }

  /**
   * Check if error handling is supported for this transform and if error handling is defined and
   * enabled to a target transform.
   *
   * @return true if error handling is defined and enabled
   */
  public boolean isDoingErrorHandling() {
    return transform.supportsErrorHandling()
        && transformErrorMeta != null
        && transformErrorMeta.getTargetTransform() != null
        && transformErrorMeta.isEnabled();
  }

  /**
   * Checks if error handling is defined for the target transform, even if it is enabled or
   * disabled.
   *
   * @return true if error handling is defined
   */
  public boolean isSendingErrorRowsToTransform(TransformMeta targetTransform) {
    return (transform.supportsErrorHandling()
        && transformErrorMeta != null
        && transformErrorMeta.getTargetTransform() != null
        && transformErrorMeta.getTargetTransform().equals(targetTransform));
  }

  /** Support for ICheckResultSource */
  @Override
  public String getTypeId() {
    return this.transformPluginId;
  }

  @Override
  public String getPluginId() {
    return this.transformPluginId;
  }

  public boolean isMapping() {
    return STRING_ID_MAPPING.equals(transformPluginId);
  }

  public boolean isSingleThreader() {
    return STRING_ID_SINGLE_THREADER.equals(transformPluginId);
  }

  public boolean isEtlMetaInject() {
    return STRING_ID_ETL_META_INJECT.equals(transformPluginId);
  }

  public boolean isWorkflowExecutor() {
    return STRING_ID_WORKFLOW_EXECUTOR.equals(transformPluginId);
  }

  public boolean isMappingInput() {
    return STRING_ID_MAPPING_INPUT.equals(transformPluginId);
  }

  public boolean isMappingOutput() {
    return STRING_ID_MAPPING_OUTPUT.equals(transformPluginId);
  }

  /**
   * Get a list of all the resource dependencies that the transform is depending on.
   *
   * @return a list of all the resource dependencies that the transform is depending on
   */
  public List<ResourceReference> getResourceDependencies(IVariables variables) {
    return transform.getResourceDependencies(variables, this);
  }

  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming iResourceNaming,
      IHopMetadataProvider metadataProvider)
      throws HopException {

    // The transform calls out to ITransformMeta.
    // This object can in turn add anything to the map in terms of resources, reference files, etc.
    //
    return transform.exportResources(variables, definitions, iResourceNaming, metadataProvider);
  }

  /**
   * @return the targetTransformPartitioningMeta
   */
  public TransformPartitioningMeta getTargetTransformPartitioningMeta() {
    return targetTransformPartitioningMeta;
  }

  /**
   * @param targetTransformPartitioningMeta the targetTransformPartitioningMeta to set
   */
  public void setTargetTransformPartitioningMeta(
      TransformPartitioningMeta targetTransformPartitioningMeta) {
    this.targetTransformPartitioningMeta = targetTransformPartitioningMeta;
  }

  public boolean isRepartitioning() {
    if (!isPartitioned() && isTargetPartitioned()) {
      return true;
    }
    return isPartitioned()
        && isTargetPartitioned()
        && !transformPartitioningMeta.equals(targetTransformPartitioningMeta);
  }

  /**
   * Set the plugin transform id (code)
   *
   * @param transformPluginId
   */
  public void setTransformPluginId(String transformPluginId) {
    this.transformPluginId = transformPluginId;
  }

  public void setParentPipelineMeta(PipelineMeta parentPipelineMeta) {
    this.parentPipelineMeta = parentPipelineMeta;
  }

  public PipelineMeta getParentPipelineMeta() {
    return parentPipelineMeta;
  }

  public IRowDistribution getRowDistribution() {
    return rowDistribution;
  }

  public void setRowDistribution(IRowDistribution rowDistribution) {
    this.rowDistribution = rowDistribution;
    if (rowDistribution != null) {
      setDistributes(true);
    }
    setChanged();
  }

  /**
   * @return the copiesString
   */
  public String getCopiesString() {
    return copiesString;
  }

  /**
   * @param copiesString the copiesString to set
   */
  public void setCopiesString(String copiesString) {
    this.copiesString = copiesString;
    copiesCache = null;
  }

  @Override
  public void setAttributesMap(Map<String, Map<String, String>> attributesMap) {
    this.attributesMap = attributesMap;
  }

  @Override
  public Map<String, Map<String, String>> getAttributesMap() {
    return attributesMap;
  }

  @Override
  public void setAttribute(String groupName, String key, String value) {
    Map<String, String> attributes = getAttributes(groupName);
    if (attributes == null) {
      attributes = new HashMap<>();
      attributesMap.put(groupName, attributes);
    }
    attributes.put(key, value);
  }

  @Override
  public void setAttributes(String groupName, Map<String, String> attributes) {
    attributesMap.put(groupName, attributes);
  }

  @Override
  public Map<String, String> getAttributes(String groupName) {
    return attributesMap.get(groupName);
  }

  @Override
  public String getAttribute(String groupName, String key) {
    Map<String, String> attributes = attributesMap.get(groupName);
    if (attributes == null) {
      return null;
    }
    return attributes.get(key);
  }

  public boolean isMissing() {
    return this.transform instanceof Missing;
  }

  public boolean isDeprecated() {
    return isDeprecated;
  }

  public String getSuggestion() {
    return suggestion;
  }
}
