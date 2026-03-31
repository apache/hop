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

package org.apache.hop.pipeline;

import com.google.common.annotations.VisibleForTesting;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.ProgressNullMonitorListener;
import org.apache.hop.core.Result;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopMissingPluginsException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.parameters.INamedParameters;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.reflection.StringSearchResult;
import org.apache.hop.core.reflection.StringSearcher;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlFormatter;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.ITransformMetaChangeListener;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.pipeline.transforms.missing.Missing;
import org.apache.hop.resource.IResourceExport;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceReference;
import org.jspecify.annotations.NonNull;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * This class defines information about a pipeline and offers methods to save and load it from XML
 * as well as methods to alter a pipeline by adding/removing databases, transforms, hops, etc.
 */
public class PipelineMeta extends AbstractMeta
    implements IXml,
        Comparator<PipelineMeta>,
        Comparable<PipelineMeta>,
        Cloneable,
        IResourceExport,
        IHasFilename {

  public static final String PIPELINE_EXTENSION = ".hpl";
  private static final String CONST_ERROR_OPENING_OR_VALIDATING =
      "PipelineMeta.Exception.ErrorOpeningOrValidatingTheXMLFile";

  private static final Class<?> PKG = Pipeline.class;

  /** A constant specifying the tag value for the XML node of the pipeline. */
  public static final String XML_TAG = "pipeline";

  public static final int BORDER_INDENT = 20;

  @Getter
  @Setter
  @HopMetadataProperty(key = "info")
  protected PipelineMetaInfo info;

  /** The list of transforms associated with the pipeline. */
  @Getter
  @Setter
  @HopMetadataProperty(key = "transform")
  protected List<TransformMeta> transforms;

  /** The list of hops associated with the pipeline. */
  @Getter
  @Setter
  @HopMetadataProperty(groupKey = "order", key = "hop")
  protected List<PipelineHopMeta> hops;

  /** Indicators for changes in transforms, databases, hops, and notes. */
  protected boolean changedTransforms;

  protected boolean changedHops;

  /** The previous result. */
  protected Result previousResult;

  /** The transforms fields cache. */
  protected Map<String, IRowMeta> transformFieldsCache;

  /** The loop cache. */
  protected Map<String, Boolean> loopCache;

  /** The previous transform cache */
  protected Map<String, List<TransformMeta>> previousTransformCache;

  /** The list of TransformChangeListeners */
  protected List<ITransformMetaChangeListener> transformChangeListeners;

  private ArrayList<Missing> missingPipeline;

  // //////////////////////////////////////////////////////////////////////////

  /** A constant specifying the tag value for the XML node of the pipeline information. */
  protected static final String XML_TAG_INFO = "info";

  /** A constant specifying the tag value for the XML node of the order of transforms. */
  public static final String XML_TAG_ORDER = "order";

  /** A constant specifying the tag value for the XML node of the notes. */
  public static final String XML_TAG_NOTEPADS = "notepads";

  /** A constant specifying the tag value for the XML node of the pipeline parameters. */
  public static final String XML_TAG_PARAMETERS = "parameters";

  /**
   * A constant specifying the tag value for the XML node of the transforms' error-handling
   * information.
   */
  public static final String XML_TAG_TRANSFORM_ERROR_HANDLING = "transform_error_handling";

  /**
   * Builds a new empty pipeline. The pipeline will have default logging capability and no
   * variables, and all internal meta-data is cleared to defaults.
   */
  public PipelineMeta() {
    clear();
  }

  /**
   * Compares two pipelines on name and filename. The comparison algorithm is as follows:<br>
   *
   * <ol>
   *   <li>The first pipeline's filename is checked first; if it has none, the pipeline is generated
   *       If the second pipeline is not generated, -1 is returned.
   *   <li>If the pipelines are both generated, the pipelines' names are compared. If the first
   *       pipeline has no name and the second one does, a -1 is returned. If the opposite is true,
   *       1 is returned.
   *   <li>If they both have names they are compared as strings. If the result is non-zero it is
   *       returned.
   * </ol>
   *
   * @param t1 the first pipeline to compare
   * @param t2 the second pipeline to compare
   * @return 0 if the two pipelines are equal, 1 or -1 depending on the values (see description
   *     above)
   */
  @Override
  public int compare(PipelineMeta t1, PipelineMeta t2) {
    return super.compare(t1, t2);
  }

  /**
   * Compares this pipeline's meta-data to the specified pipeline's meta-data. This method simply
   * calls compare(this, pipelineMeta)
   *
   * @param pipelineMeta the pipelineMeta
   * @return the comparison value
   * @see #compare(PipelineMeta, PipelineMeta)
   * @see Comparable#compareTo(Object)
   */
  @Override
  public int compareTo(@NonNull PipelineMeta pipelineMeta) {
    return compare(this, pipelineMeta);
  }

  /**
   * Checks whether this pipeline's meta-data object is equal to the specified object. If the
   * specified object is not an instance of PipelineMeta, false is returned, otherwise the method
   * returns whether a call to compare() indicates equality (i.e. compare(this,
   * (PipelineMeta)obj)==0).
   *
   * @param obj the obj
   * @return true, if successful
   * @see #compare(PipelineMeta, PipelineMeta)
   * @see Object#equals(Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof PipelineMeta)) {
      return false;
    }
    return compare(this, (PipelineMeta) obj) == 0;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Clones the pipeline meta-data object.
   *
   * @return a clone of the pipeline meta-data object throws {@link HopRuntimeException} because
   *     this simple method doesn't have enough information to clone all this pipeline metadata.
   */
  @Override
  public PipelineMeta clone() {
    throw new HopRuntimeException(
        "A pipeline can't be cloned without building new external references like metadata resources."
            + "Consider serializing to XML and de-serializing using the loadXml() method");
  }

  @Override
  protected String getExtension() {
    return PIPELINE_EXTENSION;
  }

  /**
   * Clears the pipeline's meta-data, including the lists of databases, transforms, hops, notes,
   * dependencies, partition schemas, hop servers, and cluster schemas. Logging information and
   * timeouts are reset to defaults, and recent connection info is cleared.
   */
  @Override
  public void clear() {
    info = new PipelineMetaInfo();
    info.namedParams = new NamedParameters();
    info.pipelineStatus = -1;
    transforms = new ArrayList<>();
    hops = new ArrayList<>();
    transformChangeListeners = new ArrayList<>();
    undo = new ArrayList<>();
    maxUndo = Const.MAX_UNDO;
    undoPosition = -1;
    transformFieldsCache = new HashMap<>();
    loopCache = new HashMap<>();
    previousTransformCache = new HashMap<>();
    super.clear();
  }

  /**
   * Add a new transform to the pipeline. Also marks that the pipeline's transforms have changed.
   *
   * @param transformMeta The meta-data for the transform to be added.
   */
  public void addTransform(TransformMeta transformMeta) {
    transforms.add(transformMeta);
    transformMeta.setParentPipelineMeta(this);
    ITransformMeta iface = transformMeta.getTransform();
    if (iface instanceof ITransformMetaChangeListener iTransformMetaChangeListener) {
      addTransformChangeListener(iTransformMetaChangeListener);
    }
    changedTransforms = true;
    setChanged();
    clearCaches();
  }

  /**
   * Add a new transform to the pipeline if that transform didn't exist yet. Otherwise, replace the
   * transform. This method also marks that the pipeline's transforms have changed.
   *
   * @param transformMeta The meta-data for the transform to be added.
   */
  public void addOrReplaceTransform(TransformMeta transformMeta) {
    int index = transforms.indexOf(transformMeta);
    if (index < 0) {
      transforms.add(transformMeta);
      index = transforms.size() - 1;
    } else {
      TransformMeta previous = getTransform(index);
      previous.replaceMeta(transformMeta);
    }
    transformMeta.setParentPipelineMeta(this);
    ITransformMeta iface = transformMeta.getTransform();

    if (iface instanceof ITransformMetaChangeListener iTransformMetaChangeListener) {
      addTransformChangeListener(index, iTransformMetaChangeListener);
    }
    changedTransforms = true;
    clearCaches();
  }

  /**
   * Add a new hop to the pipeline. The hop information (source and target transforms, e.g.) should
   * be configured in the PipelineHopMeta object before calling addPipelineHop(). Also marks that
   * the pipeline's hops have changed.
   *
   * @param hi The hop meta-data to be added.
   */
  public void addPipelineHop(PipelineHopMeta hi) {
    hops.add(hi);
    changedHops = true;
    setChanged();
    clearCaches();
  }

  /**
   * Add a new transform to the pipeline at the specified index. This method sets the transform's
   * parent pipeline to the this pipeline, and marks that the pipelines' transforms have changed.
   *
   * @param p The index into the transform list
   * @param transformMeta The transform to be added.
   */
  public void addTransform(int p, TransformMeta transformMeta) {
    transforms.add(p, transformMeta);
    transformMeta.setParentPipelineMeta(this);
    changedTransforms = true;
    setChanged();
    ITransformMeta iface = transformMeta.getTransform();
    if (iface instanceof ITransformMetaChangeListener) {
      addTransformChangeListener(p, (ITransformMetaChangeListener) transformMeta.getTransform());
    }
    clearCaches();
  }

  /**
   * Add a new hop to the pipeline on a certain location (i.e. the specified index). Also marks that
   * the pipeline's hops have changed.
   *
   * @param p the index into the hop list
   * @param hi The hop to be added.
   */
  public void addPipelineHop(int p, PipelineHopMeta hi) {
    try {
      hops.add(p, hi);
    } catch (IndexOutOfBoundsException e) {
      hops.add(hi);
    }
    changedHops = true;
    setChanged();
    clearCaches();
  }

  /**
   * Retrieves a transform on a certain location (i.e. the specified index).
   *
   * @param i The index into the transforms list.
   * @return The desired transform's meta-data.
   */
  public TransformMeta getTransform(int i) {
    return transforms.get(i);
  }

  /**
   * Get a list of defined hops in this pipeline.
   *
   * @return a list of defined hops.
   */
  public List<PipelineHopMeta> getPipelineHops() {
    return Collections.unmodifiableList(hops);
  }

  /**
   * Retrieves a hop on a certain location (i.e. the specified index).
   *
   * @param i The index into the hops list.
   * @return The desired hop's meta-data.
   */
  public PipelineHopMeta getPipelineHop(int i) {
    return hops.get(i);
  }

  /**
   * Removes a transform from the pipeline on a certain location (i.e. the specified index). Also
   * marks that the pipeline's transforms have changed.
   *
   * @param i The index
   */
  public void removeTransform(int i) {
    if (i < 0 || i >= transforms.size()) {
      return;
    }

    TransformMeta removeTransform = transforms.get(i);
    ITransformMeta iface = removeTransform.getTransform();
    if (iface instanceof ITransformMetaChangeListener iTransformMetaChangeListener) {
      removeTransformChangeListener(iTransformMetaChangeListener);
    }

    transforms.remove(i);

    if (removeTransform.getTransform() instanceof Missing missingTransform) {
      removeMissingPipeline(missingTransform);
    }

    changedTransforms = true;
    setChanged();
    clearCaches();
  }

  /**
   * Removes a hop from the pipeline on a certain location (i.e. the specified index). Also marks
   * that the pipeline's hops have changed.
   *
   * @param i The index into the hops list
   */
  public void removePipelineHop(int i) {
    if (i < 0 || i >= hops.size()) {
      return;
    }

    hops.remove(i);
    changedHops = true;
    setChanged();
    clearCaches();
  }

  /**
   * Removes a hop from the pipeline. Also marks that the pipeline's hops have changed.
   *
   * @param hop The hop to remove from the list of hops
   */
  public void removePipelineHop(PipelineHopMeta hop) {
    hops.remove(hop);
    changedHops = true;
    setChanged();
    clearCaches();
  }

  /**
   * Gets the number of transforms in the pipeline.
   *
   * @return The number of transforms in the pipeline.
   */
  public int nrTransforms() {
    return transforms.size();
  }

  /**
   * Gets the number of hops in the pipeline.
   *
   * @return The number of hops in the pipeline.
   */
  public int nrPipelineHops() {
    return hops.size();
  }

  /**
   * Gets the number of transformChangeListeners in the pipeline.
   *
   * @return The number of transformChangeListeners in the pipeline.
   */
  public int nrTransformChangeListeners() {
    return transformChangeListeners.size();
  }

  /**
   * Changes the content of a transform on a certain position. This is accomplished by setting the
   * transform's metadata at the specified index to the specified meta-data object. The new
   * transform's parent pipeline is updated to be this pipeline.
   *
   * @param i The index into the transforms list
   * @param transformMeta The transform meta-data to set
   */
  public void setTransform(int i, TransformMeta transformMeta) {
    ITransformMeta iface = transformMeta.getTransform();
    if (iface instanceof ITransformMetaChangeListener) {
      addTransformChangeListener(i, (ITransformMetaChangeListener) transformMeta.getTransform());
    }
    transforms.set(i, transformMeta);
    transformMeta.setParentPipelineMeta(this);
    clearCaches();
  }

  /**
   * Changes the content of a hop on a certain position. This is accomplished by setting the hop's
   * metadata at the specified index to the specified meta-data object.
   *
   * @param i The index into the hops list
   * @param hop The hop meta-data to set
   */
  public void setPipelineHop(int i, PipelineHopMeta hop) {
    hops.set(i, hop);
    clearCaches();
  }

  /**
   * Gets the list of used transforms, which are the transforms that are connected by hops.
   *
   * @return a list with all the used transforms
   */
  public List<TransformMeta> getUsedTransforms() {
    List<TransformMeta> list = new ArrayList<>();

    for (TransformMeta transformMeta : transforms) {
      if (isTransformUsedInPipelineHops(transformMeta)) {
        list.add(transformMeta);
      }
    }
    if (list.isEmpty() && getTransforms().size() == 1) {
      list = getTransforms();
    }

    return list;
  }

  /**
   * Searches the list of transforms for a transform with a certain name.
   *
   * @param name The name of the transform to look for
   * @return The transform information or null if no nothing was found.
   */
  public TransformMeta findTransform(String name) {
    return findTransform(name, null);
  }

  /**
   * Searches the list of transforms for a transform with a certain name while excluding one
   * transform.
   *
   * @param name The name of the transform to look for
   * @param exclude The transform information to exclude.
   * @return The transform information or null if nothing was found.
   */
  public TransformMeta findTransform(String name, TransformMeta exclude) {
    if (name == null) {
      return null;
    }

    int excl = -1;
    if (exclude != null) {
      excl = indexOfTransform(exclude);
    }

    for (int i = 0; i < nrTransforms(); i++) {
      TransformMeta transformMeta = getTransform(i);
      if (i != excl && transformMeta.getName().equalsIgnoreCase(name)) {
        return transformMeta;
      }
    }
    return null;
  }

  /**
   * Searches the list of hops for a hop with a certain name.
   *
   * @param name The name of the hop to look for
   * @return The hop found or null if nothing was found.
   */
  public PipelineHopMeta findPipelineHop(String name) {
    for (PipelineHopMeta hop : hops) {
      if (hop.toString().equalsIgnoreCase(name)) {
        return hop;
      }
    }
    return null;
  }

  /**
   * Search all hops for a hop where a certain transform is at the start.
   *
   * @param fromTransform The transform at the start of the hop.
   * @return The first hop found or null if nothing was found.
   */
  public PipelineHopMeta findPipelineHopFrom(TransformMeta fromTransform) {
    if (fromTransform != null) {
      for (PipelineHopMeta hop : hops) {
        if (fromTransform.equals(hop.getFromTransform())) { // return the first
          return hop;
        }
      }
    }
    return null;
  }

  public List<PipelineHopMeta> findAllPipelineHopFrom(TransformMeta fromTransform) {
    return hops.stream()
        .filter(
            hop -> hop.getFromTransform() != null && hop.getFromTransform().equals(fromTransform))
        .toList();
  }

  /**
   * Find a certain hop in the pipeline.
   *
   * @param hop The hop information to look for.
   * @return The hop or null if no hop was found.
   */
  public PipelineHopMeta findPipelineHop(PipelineHopMeta hop) {
    return findPipelineHop(hop.getFromTransform(), hop.getToTransform());
  }

  /**
   * Search all hops for a hop where a certain transform is at the start and another is at the end.
   *
   * @param from The transform at the start of the hop.
   * @param to The transform at the end of the hop.
   * @return The hop or null if no hop was found.
   */
  public PipelineHopMeta findPipelineHop(TransformMeta from, TransformMeta to) {
    return findPipelineHop(from, to, false);
  }

  /**
   * Search all hops for a hop where a certain transform is at the start and another is at the end.
   *
   * @param from The transform at the start of the hop.
   * @param to The transform at the end of the hop.
   * @param includeDisabled include disabled hop
   * @return The hop or null if no hop was found.
   */
  public PipelineHopMeta findPipelineHop(
      TransformMeta from, TransformMeta to, boolean includeDisabled) {
    for (PipelineHopMeta hop : hops) {
      if ((hop.isEnabled() || includeDisabled)
          && hop.getFromTransform() != null
          && hop.getToTransform() != null
          && hop.getFromTransform().equals(from)
          && hop.getToTransform().equals(to)) {
        return hop;
      }
    }
    return null;
  }

  /**
   * Search all hops for a hop where a certain transform is at the end.
   *
   * @param toTransform The transform at the end of the hop.
   * @return The hop or null if nothing was found.
   */
  public PipelineHopMeta findPipelineHopTo(TransformMeta toTransform) {
    if (toTransform != null) {
      for (PipelineHopMeta hop : hops) {
        if (toTransform.equals(hop.getToTransform())) { // Return the first!
          return hop;
        }
      }
    }
    return null;
  }

  /**
   * Determines whether or not a certain transform is informative. This means that the previous
   * transform is sending information to this transform, but only informative. This means that this
   * transform is using the information to process the actual stream of data. We use this in
   * StreamLookup, TableInput and other types of transforms.
   *
   * @param thisTransform The transform that is receiving information.
   * @param prevTransform The transform that is sending information
   * @return true if prevTransform if informative for thisTransform.
   */
  public boolean isTransformInformative(TransformMeta thisTransform, TransformMeta prevTransform) {
    String[] infoTransforms =
        thisTransform.getTransform().getTransformIOMeta().getInfoTransformNames();
    if (infoTransforms == null) {
      return false;
    }
    for (String infoTransform : infoTransforms) {
      if (prevTransform.getName().equalsIgnoreCase(infoTransform)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Get the list of previous transforms for a certain reference transform. This includes the info
   * transforms.
   *
   * @param transformMeta The reference transform
   * @return The list of the preceding transforms, including the info transforms.
   */
  public List<TransformMeta> findPreviousTransforms(TransformMeta transformMeta) {
    return findPreviousTransforms(transformMeta, true);
  }

  /**
   * Get the previous transforms on a certain location taking into account the transforms being
   * informational or not.
   *
   * @param transformMeta The name of the transform
   * @param info true if we only want the informational transforms.
   * @return The list of the preceding transforms
   */
  public List<TransformMeta> findPreviousTransforms(TransformMeta transformMeta, boolean info) {
    if (transformMeta == null) {
      return new ArrayList<>();
    }

    String cacheKey = getTransformMetaCacheKey(transformMeta, info);
    List<TransformMeta> previousTransforms = previousTransformCache.get(cacheKey);
    if (previousTransforms == null) {
      previousTransforms = new ArrayList<>();
      for (PipelineHopMeta hi : hops) {
        if (hi.getToTransform() != null
            && hi.isEnabled()
            && hi.getToTransform().equals(transformMeta)
            && (info || !isTransformInformative(transformMeta, hi.getFromTransform()))) {
          // Check if this previous transform isn't informative (StreamValueLookup)
          // We don't want fields from this stream to show up!
          previousTransforms.add(hi.getFromTransform());
        }
      }
      previousTransformCache.put(cacheKey, previousTransforms);
    }
    return previousTransforms;
  }

  /**
   * Get the informational transforms for a certain transform. An informational transform is a
   * transform that provides information for lookups, etc.
   *
   * @param transformMeta The name of the transform
   * @return An array of the informational transforms found
   */
  public TransformMeta[] getInfoTransform(TransformMeta transformMeta) {
    String[] infoTransformName =
        transformMeta.getTransform().getTransformIOMeta().getInfoTransformNames();
    if (infoTransformName == null) {
      return new TransformMeta[0];
    }

    TransformMeta[] infoTransform = new TransformMeta[infoTransformName.length];
    for (int i = 0; i < infoTransform.length; i++) {
      infoTransform[i] = findTransform(infoTransformName[i]);
    }

    return infoTransform;
  }

  /**
   * Find the number of informational transforms for a certain transform.
   *
   * @param transformMeta The transform
   * @return The number of informational transforms found.
   */
  public int findNrInfoTransforms(TransformMeta transformMeta) {
    if (transformMeta == null) {
      return 0;
    }

    int count = 0;

    // Look at all the hops
    for (PipelineHopMeta hop : hops) {
      if (hop == null || hop.getToTransform() == null) {
        LogChannel.GENERAL.logError(
            BaseMessages.getString(PKG, "PipelineMeta.Log.DestinationOfHopCannotBeNull"));
      }
      if (hop != null
          && hop.getToTransform() != null
          && hop.isEnabled()
          && hop.getToTransform().equals(transformMeta)
          && isTransformInformative(transformMeta, hop.getFromTransform())) {
        // Check if this previous transform isn't informative (StreamValueLookup)
        // We don't want fields from this stream to show up!
        count++;
      }
    }
    return count;
  }

  /**
   * Find the informational fields coming from an informational transform into the transform
   * specified.
   *
   * @param transformName The name of the transform
   * @return A row containing fields with origin.
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getPrevInfoFields(IVariables variables, String transformName)
      throws HopTransformException {
    return getPrevInfoFields(variables, findTransform(transformName));
  }

  /**
   * Find the informational fields coming from an informational transform into the transform
   * specified.
   *
   * @param transformMeta The receiving transform
   * @return A row containing fields with origin.
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getPrevInfoFields(IVariables variables, TransformMeta transformMeta)
      throws HopTransformException {
    // Look at all the hops
    for (PipelineHopMeta hop : hops) {
      if (hop.isEnabled() && hop.getToTransform().equals(transformMeta)) {
        TransformMeta infoTransform = hop.getFromTransform();
        if (isTransformInformative(transformMeta, infoTransform)) {
          IRowMeta row = getPrevTransformFields(variables, infoTransform);
          return getThisTransformFields(variables, infoTransform, transformMeta, row);
        }
      }
    }
    return new RowMeta();
  }

  /**
   * Retrieve an array of preceding transforms for a certain destination transform. This includes
   * the info transforms.
   *
   * @param transformMeta The destination transform
   * @return An array containing the preceding transforms.
   */
  public TransformMeta[] getPrevTransforms(TransformMeta transformMeta) {
    List<TransformMeta> prevTransforms =
        previousTransformCache.get(getTransformMetaCacheKey(transformMeta, true));
    if (prevTransforms == null) {
      prevTransforms = new ArrayList<>();
      // Look at all the hops
      for (PipelineHopMeta hop : hops) {
        if (hop.isEnabled() && hop.getToTransform().equals(transformMeta)) {
          prevTransforms.add(hop.getFromTransform());
        }
      }
    }

    return prevTransforms.toArray(new TransformMeta[0]);
  }

  /**
   * Retrieve an array of succeeding transform names for a certain originating transform name.
   *
   * @param transformName The originating transform name
   * @return An array of succeeding transform names
   */
  public String[] getPrevTransformNames(String transformName) {
    return getPrevTransformNames(findTransform(transformName));
  }

  /**
   * Retrieve an array of preceding transforms for a certain destination transform.
   *
   * @param transformMeta The destination transform
   * @return an array of preceding transform names.
   */
  public String[] getPrevTransformNames(TransformMeta transformMeta) {
    TransformMeta[] prevTransformMetas = getPrevTransforms(transformMeta);
    String[] retval = new String[prevTransformMetas.length];
    for (int x = 0; x < prevTransformMetas.length; x++) {
      retval[x] = prevTransformMetas[x].getName();
    }

    return retval;
  }

  /**
   * Retrieve a list of succeeding transforms for a certain originating transform.
   *
   * @param transformMeta The originating transform
   * @return A list of succeeding transforms.
   */
  public List<TransformMeta> findNextTransforms(TransformMeta transformMeta) {
    return findNextTransforms(transformMeta, false);
  }

  /**
   * Retrieve a list of succeeding transforms for a certain originating transform.
   *
   * @param transformMeta The originating transform
   * @param includeDisabled if true, include the following transforms even if hops are disabled
   * @return A list of succeeding transforms.
   */
  public List<TransformMeta> findNextTransforms(
      TransformMeta transformMeta, boolean includeDisabled) {
    List<TransformMeta> nextTransforms = new ArrayList<>();
    // Look at all the hops
    for (PipelineHopMeta hop : hops) {
      if ((hop.isEnabled() || includeDisabled) && hop.getFromTransform().equals(transformMeta)) {
        nextTransforms.add(hop.getToTransform());
      }
    }

    return nextTransforms;
  }

  /**
   * Retrieve an array of succeeding transform names for a certain originating transform.
   *
   * @param transformMeta The originating transform
   * @return an array of succeeding transform names.
   */
  public String[] getNextTransformNames(TransformMeta transformMeta) {
    List<TransformMeta> nextTransformMeta = findNextTransforms(transformMeta);
    String[] retval = new String[nextTransformMeta.size()];
    for (int x = 0; x < nextTransformMeta.size(); x++) {
      retval[x] = nextTransformMeta.get(x).getName();
    }

    return retval;
  }

  /**
   * Find the transform that is located on a certain point on the canvas, taking into account the
   * icon size.
   *
   * @param x the x-coordinate of the point queried
   * @param y the y-coordinate of the point queried
   * @param iconsize the iconsize
   * @return The transform information if a transform is located at the point. Otherwise, if no
   *     transform was found: null.
   */
  public TransformMeta getTransform(int x, int y, int iconsize) {
    int s = transforms.size();
    for (int i = s - 1; i >= 0; i--) { // Back to front because drawing goes from start to end
      TransformMeta transformMeta = transforms.get(i);
      Point p = transformMeta.getLocation();
      if (p != null && x >= p.x && x <= p.x + iconsize && y >= p.y && y <= p.y + iconsize + 20) {
        return transformMeta;
      }
    }
    return null;
  }

  /**
   * Determines whether or not a certain transform is part of a hop.
   *
   * @param transformMeta The transform queried
   * @return true if the transform is part of a hop.
   */
  public boolean partOfPipelineHop(TransformMeta transformMeta) {
    for (PipelineHopMeta hop : hops) {
      if (hop.getFromTransform() == null || hop.getToTransform() == null) {
        return false;
      }
      if (hop.getFromTransform().equals(transformMeta)
          || hop.getToTransform().equals(transformMeta)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the fields that are emitted by a certain transform name.
   *
   * @param transformName The transformName of the transform to be queried.
   * @return A row containing the fields emitted.
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getTransformFields(IVariables variables, String transformName)
      throws HopTransformException {
    TransformMeta transformMeta = findTransform(transformName);
    if (transformMeta != null) {
      return getTransformFields(variables, transformMeta);
    } else {
      return null;
    }
  }

  /**
   * Returns the fields that are emitted by a certain transform.
   *
   * @param transformMeta The transform to be queried.
   * @return A row containing the fields emitted.
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getTransformFields(IVariables variables, TransformMeta transformMeta)
      throws HopTransformException {
    return getTransformFields(variables, transformMeta, null);
  }

  /**
   * Gets the fields for each of the specified transforms and merges them into a single set
   *
   * @param transformMeta the transform meta
   * @return an interface to the transform fields
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getTransformFields(IVariables variables, TransformMeta[] transformMeta)
      throws HopTransformException {
    IRowMeta fields = new RowMeta();

    for (TransformMeta meta : transformMeta) {
      IRowMeta flds = getTransformFields(variables, meta);
      if (flds != null && meta != null) {
        fields.mergeRowMeta(flds, meta.getName());
      }
    }
    return fields;
  }

  /**
   * Returns the fields that are emitted by a certain transform.
   *
   * @param transformMeta The transform to be queried.
   * @param monitor The progress monitor for progress dialog. (null if not used!)
   * @return A row containing the fields emitted.
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getTransformFields(
      IVariables variables, TransformMeta transformMeta, IProgressMonitor monitor)
      throws HopTransformException {
    return getTransformFields(variables, transformMeta, null, monitor);
  }

  /**
   * Returns the fields that are emitted by a certain transform.
   *
   * @param transformMeta The transform to be queried.
   * @param targetTransform the target transform
   * @param monitor The progress monitor for progress dialog. (null if not used!)
   * @return A row containing the fields emitted.
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getTransformFields(
      IVariables variables,
      TransformMeta transformMeta,
      TransformMeta targetTransform,
      IProgressMonitor monitor)
      throws HopTransformException {
    IRowMeta row = new RowMeta();

    if (transformMeta == null) {
      return row;
    }

    String fromToCacheEntry = calculateFieldsCacheEntryKey(transformMeta, targetTransform);
    IRowMeta rowMeta = transformFieldsCache.get(fromToCacheEntry);
    if (rowMeta != null) {
      return rowMeta;
    }

    // See if the transform is sending ERROR rows to the specified target transform.
    //
    if (targetTransform != null && transformMeta.isSendingErrorRowsToTransform(targetTransform)) {
      return getTransformFieldsErrorHandling(
          variables, transformMeta, targetTransform, monitor, fromToCacheEntry);
    }

    // Resume the regular program...
    //
    getTransformFieldsRegular(variables, transformMeta, monitor, row);

    // Finally, see if we need to add/modify/delete fields with this transform "name"
    rowMeta = getThisTransformFields(variables, transformMeta, targetTransform, row, monitor);

    // Store this row in the cache
    //
    transformFieldsCache.put(fromToCacheEntry, rowMeta);

    return rowMeta;
  }

  private void getTransformFieldsRegular(
      IVariables variables, TransformMeta transformMeta, IProgressMonitor monitor, IRowMeta row)
      throws HopTransformException {
    List<TransformMeta> prevTransforms = findPreviousTransforms(transformMeta, false);
    for (int i = 0; i < prevTransforms.size(); i++) {
      TransformMeta prevTransformMeta = prevTransforms.get(i);

      monitorSubTask(
          monitor, "PipelineMeta.Monitor.CheckingTransformTask.Title", prevTransformMeta.getName());

      IRowMeta add = getTransformFields(variables, prevTransformMeta, transformMeta, monitor);
      if (add == null) {
        add = new RowMeta();
      }
      if (i == 0) {
        row.addRowMeta(add);
      } else {
        // See if the add fields are not already in the row
        for (int x = 0; x < add.size(); x++) {
          IValueMeta v = add.getValueMeta(x);
          IValueMeta s = row.searchValueMeta(v.getName());
          if (s == null) {
            row.addValueMeta(v);
          }
        }
      }
    }
  }

  private static @NonNull String calculateFieldsCacheEntryKey(
      TransformMeta transformMeta, TransformMeta targetTransform) {
    return transformMeta.getName()
        + (targetTransform != null ? ("-" + targetTransform.getName()) : "");
  }

  private @NonNull IRowMeta getTransformFieldsErrorHandling(
      IVariables variables,
      TransformMeta transformMeta,
      TransformMeta targetTransform,
      IProgressMonitor monitor,
      String fromToCacheEntry)
      throws HopTransformException {
    IRowMeta row;
    // The error rows are the same as the input rows for
    // the transform but with the selected error fields added
    //
    row = getPrevTransformFields(variables, transformMeta);

    // Check if row object is null that means the transform could be an input transform. In this
    // case, get fields
    // out from transform itself
    if (row.isEmpty()) {
      row = getThisTransformFields(variables, transformMeta, targetTransform, row, monitor);
    }

    // Add to this the error fields...
    TransformErrorMeta transformErrorMeta = transformMeta.getTransformErrorMeta();
    row.addRowMeta(transformErrorMeta.getErrorRowMeta(variables));

    // Store this row in the cache
    //
    transformFieldsCache.put(fromToCacheEntry, row);

    return row;
  }

  /**
   * Find the fields that are entering a transform with a certain name.
   *
   * @param transformName The name of the transform queried
   * @return A row containing the fields (w/ origin) entering the transform
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getPrevTransformFields(IVariables variables, String transformName)
      throws HopTransformException {
    return getPrevTransformFields(variables, findTransform(transformName));
  }

  /**
   * Find the fields that are entering a certain transform.
   *
   * @param transformMeta The transform queried
   * @return A row containing the fields (w/ origin) entering the transform
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getPrevTransformFields(IVariables variables, TransformMeta transformMeta)
      throws HopTransformException {
    return getPrevTransformFields(variables, transformMeta, null);
  }

  /**
   * Find the fields that are entering a certain transform.
   *
   * @param transformMeta The transform queried
   * @param monitor The progress monitor for progress dialog. (null if not used!)
   * @return A row containing the fields (w/ origin) entering the transform
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getPrevTransformFields(
      IVariables variables, TransformMeta transformMeta, IProgressMonitor monitor)
      throws HopTransformException {
    return getPrevTransformFields(variables, transformMeta, null, monitor);
  }

  public IRowMeta getPrevTransformFields(
      IVariables variables,
      TransformMeta transformMeta,
      final String transformName,
      IProgressMonitor monitor)
      throws HopTransformException {
    clearTransformFieldsCache();

    IRowMeta row = new RowMeta();

    if (transformMeta == null) {
      return null;
    }
    List<TransformMeta> prevTransforms = findPreviousTransforms(transformMeta);
    for (int i = 0; i < prevTransforms.size(); i++) {
      TransformMeta prevTransformMeta = prevTransforms.get(i);
      if (transformName != null && !transformName.equalsIgnoreCase(prevTransformMeta.getName())) {
        continue;
      }

      monitorSubTask(
          monitor, "PipelineMeta.Monitor.CheckingTransformTask.Title", prevTransformMeta.getName());

      IRowMeta add = getTransformFields(variables, prevTransformMeta, transformMeta, monitor);

      if (i == 0) {
        // we expect all input streams to be of the same layout!
        row.addRowMeta(add); // recursive!
      } else {
        // See if the add fields are not already in the row
        for (int x = 0; x < add.size(); x++) {
          IValueMeta v = add.getValueMeta(x);
          IValueMeta s = row.searchValueMeta(v.getName());
          if (s == null) {
            row.addValueMeta(v);
          }
        }
      }
    }
    return row;
  }

  private void monitorBeginTask(
      IProgressMonitor monitor, int nrWorks, String messageString, String... arguments) {
    if (monitor != null) {
      monitor.beginTask(BaseMessages.getString(PKG, messageString, arguments), nrWorks);
    }
  }

  private void monitorSubTask(IProgressMonitor monitor, String messageString, String... arguments) {
    if (monitor != null) {
      monitor.subTask(BaseMessages.getString(PKG, messageString, arguments));
    }
  }

  /**
   * Return the fields that are emitted by a transform with a certain name.
   *
   * @param transformName The name of the transform that's being queried.
   * @param row A row containing the input fields or an empty row if no input is required.
   * @return A Row containing the output fields.
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getThisTransformFields(IVariables variables, String transformName, IRowMeta row)
      throws HopTransformException {
    return getThisTransformFields(variables, findTransform(transformName), null, row);
  }

  /**
   * Returns the fields that are emitted by a transform.
   *
   * @param transformMeta : The TransformMeta object that's being queried
   * @param nextTransform : if non-null this is the next transform that's call back to ask what's
   *     being sent
   * @param row : A row containing the input fields or an empty row if no input is required.
   * @return A Row containing the output fields.
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getThisTransformFields(
      IVariables variables, TransformMeta transformMeta, TransformMeta nextTransform, IRowMeta row)
      throws HopTransformException {
    return getThisTransformFields(variables, transformMeta, nextTransform, row, null);
  }

  /**
   * Returns the fields that are emitted by a transform.
   *
   * @param transformMeta : The TransformMeta object that's being queried
   * @param nextTransform : if non-null this is the next transform that's call back to ask what's
   *     being sent
   * @param row : A row containing the input fields or an empty row if no input is required.
   * @param monitor the monitor
   * @return A Row containing the output fields.
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getThisTransformFields(
      IVariables variables,
      TransformMeta transformMeta,
      TransformMeta nextTransform,
      IRowMeta row,
      IProgressMonitor monitor)
      throws HopTransformException {
    // Then this one.
    if (LogChannel.GENERAL.isDebug()) {
      LogChannel.GENERAL.logDebug(
          BaseMessages.getString(
              PKG,
              "PipelineMeta.Log.GettingFieldsFromTransform",
              transformMeta.getName(),
              transformMeta.getTransformPluginId()));
    }
    String name = transformMeta.getName();

    monitorSubTask(monitor, "PipelineMeta.Monitor.GettingFieldsFromTransformTask.Title", name);

    ITransformMeta iTransformMeta = transformMeta.getTransform();
    IRowMeta[] infoRowMeta;
    TransformMeta[] lu = getInfoTransform(transformMeta);
    if (Utils.isEmpty(lu)) {
      try {
        infoRowMeta =
            new IRowMeta[] {
              iTransformMeta.getTableFields(variables),
            };
      } catch (HopDatabaseException dbe) {
        throw new HopTransformException(
            "Error getting table fields in transform " + transformMeta.getName(), dbe);
      }
    } else {
      infoRowMeta = new IRowMeta[lu.length];
      for (int i = 0; i < lu.length; i++) {
        infoRowMeta[i] = getTransformFields(variables, lu[i]);
      }
    }

    // Go get the fields...
    //
    IRowMeta before = row.clone();
    IRowMeta[] clonedInfo = cloneRowMetaInterfaces(infoRowMeta);
    if (!isSomethingDifferentInRow(before, row)) {
      iTransformMeta.getFields(
          this, before, name, clonedInfo, nextTransform, variables, metadataProvider);
      // pass the clone object to prevent from spoiling data by other transforms
      row = before;
    }

    return row;
  }

  private boolean isSomethingDifferentInRow(IRowMeta before, IRowMeta after) {
    if (before.size() != after.size()) {
      return true;
    }
    for (int i = 0; i < before.size(); i++) {
      IValueMeta beforeValueMeta = before.getValueMeta(i);
      IValueMeta afterValueMeta = after.getValueMeta(i);
      if (stringsDifferent(beforeValueMeta.getName(), afterValueMeta.getName())) {
        return true;
      }
      if (beforeValueMeta.getType() != afterValueMeta.getType()) {
        return true;
      }
      if (beforeValueMeta.getLength() != afterValueMeta.getLength()) {
        return true;
      }
      if (beforeValueMeta.getPrecision() != afterValueMeta.getPrecision()) {
        return true;
      }
      if (stringsDifferent(beforeValueMeta.getOrigin(), afterValueMeta.getOrigin())) {
        return true;
      }
      if (stringsDifferent(beforeValueMeta.getComments(), afterValueMeta.getComments())) {
        return true;
      }
      if (stringsDifferent(
          beforeValueMeta.getConversionMask(), afterValueMeta.getConversionMask())) {
        return true;
      }
      if (stringsDifferent(
          beforeValueMeta.getStringEncoding(), afterValueMeta.getStringEncoding())) {
        return true;
      }
      if (stringsDifferent(beforeValueMeta.getDecimalSymbol(), afterValueMeta.getDecimalSymbol())) {
        return true;
      }
      if (stringsDifferent(
          beforeValueMeta.getGroupingSymbol(), afterValueMeta.getGroupingSymbol())) {
        return true;
      }
    }
    return false;
  }

  private boolean stringsDifferent(String one, String two) {
    if (one == null && two == null) {
      return false;
    }
    if (one == null && two != null) {
      return true;
    }
    if (one != null && two == null) {
      return true;
    }
    return !one.equals(two);
  }

  /**
   * Checks if the pipeline is using the specified partition schema.
   *
   * @param partitionSchema the partition schema
   * @return true if the pipeline is using the partition schema, false otherwise
   */
  public boolean isUsingPartitionSchema(PartitionSchema partitionSchema) {
    // Loop over all transforms and see if the partition schema is used.
    for (int i = 0; i < nrTransforms(); i++) {
      TransformPartitioningMeta transformPartitioningMeta =
          getTransform(i).getTransformPartitioningMeta();
      if (transformPartitioningMeta != null) {
        PartitionSchema check = transformPartitioningMeta.getPartitionSchema();
        if (check != null && check.equals(partitionSchema)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Finds the location (index) of the specified hop.
   *
   * @param hi The hop queried
   * @return The location of the hop, or -1 if nothing was found.
   */
  public int indexOfPipelineHop(PipelineHopMeta hi) {
    return hops.indexOf(hi);
  }

  /**
   * Finds the location (index) of the specified transform.
   *
   * @param transformMeta The transform queried
   * @return The location of the transform, or -1 if nothing was found.
   */
  public int indexOfTransform(TransformMeta transformMeta) {
    return transforms.indexOf(transformMeta);
  }

  /**
   * Gets the XML representation of this pipeline.
   *
   * @param variables The variables to look up whether we need to add a license header.
   * @return the XML representation of this pipeline
   * @throws HopException if any errors occur during generation of the XML
   * @see IXml#getXml(IVariables)
   */
  @Override
  public String getXml(IVariables variables) throws HopException {
    return XmlHandler.getLicenseHeader(variables)
        + XmlFormatter.format(
            XmlHandler.aroundTag(XML_TAG, XmlMetadataUtil.serializeObjectToXml(this)));
  }

  /**
   * Parses a file containing the XML that describes the pipeline.
   *
   * @param filename The filename
   * @param metadataProvider the metadata store to reference (or null if there is none)
   * @param parentVariableSpace the parent variable variables to use during PipelineMeta
   *     construction
   * @throws HopXmlException if any errors occur during parsing of the specified file
   * @throws HopMissingPluginsException in case missing plugins were found (details are in the
   *     exception in that case)
   */
  public PipelineMeta(
      String filename, IHopMetadataProvider metadataProvider, IVariables parentVariableSpace)
      throws HopXmlException, HopMissingPluginsException {
    // if filename is not provided, there's not much we can do, throw an exception
    if (StringUtils.isBlank(filename)) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "PipelineMeta.Exception.MissingXMLFilePath"));
    }
    if (metadataProvider == null) {
      throw new HopXmlException(
          "API error: metadata provider can't be null. When loading a pipeline Hop needs to be able to reference external metadata objects");
    }
    this.metadataProvider = metadataProvider;

    loadXml(filename, metadataProvider, parentVariableSpace);
  }

  public void loadXml(
      String filename, IHopMetadataProvider metadataProvider, IVariables parentVariableSpace)
      throws HopXmlException, HopMissingPluginsException {
    // OK, try to load using the VFS stuff...
    Document doc;
    try {
      final FileObject pipelineFile = HopVfs.getFileObject(filename);
      if (!pipelineFile.exists()) {
        throw new HopXmlException(
            BaseMessages.getString(PKG, "PipelineMeta.Exception.InvalidXMLPath", filename));
      }
      doc = XmlHandler.loadXmlFile(pipelineFile);
    } catch (HopXmlException ke) {
      // if we have a HopXmlException, simply re-throw it
      throw ke;
    } catch (HopException | FileSystemException e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, CONST_ERROR_OPENING_OR_VALIDATING, filename), e);
    }

    if (doc != null) {
      // Root node:
      Node pipelineNode = XmlHandler.getSubNode(doc, XML_TAG);

      if (pipelineNode == null) {
        throw new HopXmlException(
            BaseMessages.getString(PKG, "PipelineMeta.Exception.NotValidPipelineXML", filename));
      }

      // Load from this node...
      loadXml(pipelineNode, filename, metadataProvider, parentVariableSpace);
    } else {
      throw new HopXmlException(
          BaseMessages.getString(PKG, CONST_ERROR_OPENING_OR_VALIDATING, filename));
    }
  }

  /**
   * Instantiates a new pipeline meta-data object.
   *
   * @param xmlStream the XML input stream from which to read the pipeline definition
   * @param parentVariableSpace the parent variable variables
   * @throws HopXmlException if any errors occur during parsing of the specified stream
   * @throws HopMissingPluginsException in case missing plugins were found (details are in the
   *     exception in that case)
   */
  public PipelineMeta(
      InputStream xmlStream, IHopMetadataProvider metadataProvider, IVariables parentVariableSpace)
      throws HopXmlException, HopMissingPluginsException {
    Document doc = XmlHandler.loadXmlFile(xmlStream, null, false, false);
    Node pipelineNode = XmlHandler.getSubNode(doc, XML_TAG);
    loadXml(pipelineNode, null, metadataProvider, parentVariableSpace);
  }

  /**
   * Parse a file containing the XML that describes the pipeline.
   *
   * @param pipelineNode The XML node to load from
   * @throws HopXmlException if any errors occur during parsing of the specified file
   * @throws HopMissingPluginsException in case missing plugins were found (details are in the
   *     exception in that case)
   */
  public PipelineMeta(Node pipelineNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException, HopMissingPluginsException {
    loadXml(pipelineNode, null, metadataProvider, null);
  }

  /**
   * Parses an XML DOM (starting at the specified Node) that describes the pipeline.
   *
   * @param pipelineNode The XML node to load from
   * @param filename The filename
   * @param variables the parent variable variables to use during PipelineMeta construction
   * @throws HopXmlException if any errors occur during parsing of the specified file
   * @throws HopMissingPluginsException in case missing plugins were found (details are in the
   *     exception in that case)
   */
  public void loadXml(
      Node pipelineNode,
      String filename,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopXmlException, HopMissingPluginsException {

    HopMissingPluginsException missingPluginsException =
        new HopMissingPluginsException(
            BaseMessages.getString(
                PKG, "PipelineMeta.MissingPluginsFoundWhileLoadingPipeline.Exception"));

    this.metadataProvider = metadataProvider; // Remember this as the primary meta store.

    try {
      deSerializeXml(pipelineNode, filename, metadataProvider, variables);
    } catch (Exception e) {
      // See if we have missing plugins to report, those take precedence!
      //
      if (!missingPluginsException.getMissingPluginDetailsList().isEmpty()) {
        throw missingPluginsException;
      } else {
        throw new HopXmlException(
            BaseMessages.getString(PKG, "PipelineMeta.Exception.ErrorReadingPipeline"), e);
      }
    } finally {
      if (!missingPluginsException.getMissingPluginDetailsList().isEmpty()) {
        throw missingPluginsException;
      }
    }
    clearChanged();
  }

  private void deSerializeXml(
      Node pipelineNode,
      String filename,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopException {
    try {
      // Clear the pipeline
      clear();

      // Set the filename here, so it can be used in variables for ALL aspects of the pipeline.
      //
      setFilename(filename);

      // De-serialize the XML using @HopMetadataProperty annotations
      //
      XmlMetadataUtil.deSerializeFromXml(
          null, null, pipelineNode, PipelineMeta.class, this, metadataProvider);

      lookupReferencesAfterLoading();
    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "PipelineMeta.Exception.ErrorReadingPipeline"), xe);
    } catch (Exception e) {
      throw new HopXmlException(e);
    } finally {
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.PipelineMetaLoaded.id, this);
    }
  }

  /**
   * After loading there can still be some references to other transforms or indeed this pipeline
   * that need to be set. This is happening here.
   */
  public void lookupReferencesAfterLoading() {
    for (TransformMeta transformMeta : transforms) {
      ITransformMeta iTransform = transformMeta.getTransform();

      // Search info and target transforms.
      iTransform.searchInfoAndTargetTransforms(transforms);

      // Also set the parent pipeline to which this transform belongs.
      // This is rarely used, for example in getTableFields.getTableFields()
      transformMeta.setParentPipelineMeta(this);
    }
  }

  /**
   * Gets a List of all the transforms that are used in at least one active hop. These transforms
   * will be used to execute the pipeline. The others will not be executed.<br>
   * Update 3.0 : we also add those transforms that are not linked to another hop, but have at least
   * one remote input or output transform defined.
   *
   * @param all true if you want to get ALL the transforms from the pipeline, false otherwise
   * @return A List of transforms
   */
  public List<TransformMeta> getPipelineHopTransforms(boolean all) {
    List<TransformMeta> st = new ArrayList<>();
    int idx;

    for (int x = 0; x < nrPipelineHops(); x++) {
      PipelineHopMeta hi = getPipelineHop(x);
      if (hi.isEnabled() || all) {
        idx = st.indexOf(hi.getFromTransform()); // FROM
        if (idx < 0) {
          st.add(hi.getFromTransform());
        }

        idx = st.indexOf(hi.getToTransform()); // TO
        if (idx < 0) {
          st.add(hi.getToTransform());
        }
      }
    }

    // Also, add the transforms that need to be painted, but are not part of a hop
    for (int x = 0; x < nrTransforms(); x++) {
      TransformMeta transformMeta = getTransform(x);
      if (!isTransformUsedInPipelineHops(transformMeta)) {
        st.add(transformMeta);
      }
    }

    return st;
  }

  /**
   * Checks if a transform has been used in a hop or not.
   *
   * @param transformMeta The transform queried.
   * @return true if a transform is used in a hop (active or not), false otherwise
   */
  public boolean isTransformUsedInPipelineHops(TransformMeta transformMeta) {
    PipelineHopMeta fr = findPipelineHopFrom(transformMeta);
    PipelineHopMeta to = findPipelineHopTo(transformMeta);
    return fr != null || to != null;
  }

  /**
   * Checks if any selected transform has been used in a hop or not.
   *
   * @return true if a transform is used in a hop (active or not), false otherwise
   */
  public boolean isAnySelectedTransformUsedInPipelineHops() {
    List<TransformMeta> selectedTransforms = getSelectedTransforms();
    int i = 0;
    while (i < selectedTransforms.size()) {
      TransformMeta transformMeta = selectedTransforms.get(i);
      if (isTransformUsedInPipelineHops(transformMeta)) {
        return true;
      }
      i++;
    }
    return false;
  }

  /** Clears the different changed flags of the pipeline. */
  @Override
  public void clearChanged() {
    changedTransforms = false;
    changedHops = false;

    for (TransformMeta transform : transforms) {
      transform.setChanged(false);
      if (transform.getTransformPartitioningMeta() != null) {
        transform.getTransformPartitioningMeta().hasChanged(false);
      }
    }
    for (PipelineHopMeta hop : hops) {
      hop.setChanged(false);
    }

    super.clearChanged();
  }

  /**
   * Checks whether or not the transforms have changed.
   *
   * @return true if the transforms have been changed, false otherwise
   */
  @Override
  public boolean hasChanged() {
    return super.hasChanged() || haveTransformsChanged() || haveHopsChanged();
  }

  public boolean haveTransformsChanged() {
    if (changedTransforms) {
      return true;
    }

    for (TransformMeta transform : transforms) {
      if (transform.hasChanged()) {
        return true;
      }
      if (transform.getTransformPartitioningMeta() != null
          && transform.getTransformPartitioningMeta().hasChanged()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether or not any of the hops have been changed.
   *
   * @return true if a hop has been changed, false otherwise
   */
  public boolean haveHopsChanged() {
    if (changedHops) {
      return true;
    }

    for (PipelineHopMeta hop : hops) {
      if (hop.hasChanged()) {
        return true;
      }
    }
    return false;
  }

  /**
   * See if there are any loops in the pipeline, starting at the indicated transform. This works by
   * looking at all the previous transforms. If you keep going backward and find the transform,
   * there is a loop. Both the informational and the normal transforms need to be checked for loops!
   *
   * @param transformMeta The transform position to start looking
   * @return true if a loop has been found, false if no loop is found.
   */
  public boolean hasLoop(TransformMeta transformMeta) {
    clearLoopCache();
    return hasLoop(transformMeta, null);
  }

  /**
   * Checks for loop.
   *
   * @param transformMeta the transformmeta
   * @param lookup the lookup
   * @return true, if successful
   */
  public boolean hasLoop(TransformMeta transformMeta, TransformMeta lookup) {
    return hasLoop(transformMeta, lookup, new HashSet<>());
  }

  /**
   * See if there are any loops in the pipeline, starting at the indicated transform. This works by
   * looking at all the previous transforms. If you keep going backward and find the original
   * transform again, there is a loop.
   *
   * @param transformMeta The transform position to start looking
   * @param lookup The original transform when wandering around the pipeline.
   * @param checkedEntries Already checked entries
   * @return true if a loop has been found, false if no loop is found.
   */
  private boolean hasLoop(
      TransformMeta transformMeta, TransformMeta lookup, HashSet<TransformMeta> checkedEntries) {
    String cacheKey = transformMeta.getName() + " - " + (lookup != null ? lookup.getName() : "");

    Boolean hasLoop = loopCache.get(cacheKey);

    if (hasLoop != null) {
      return hasLoop;
    }

    hasLoop = false;

    checkedEntries.add(transformMeta);

    List<TransformMeta> prevTransforms = findPreviousTransforms(transformMeta, true);
    for (TransformMeta prevTransformMeta : prevTransforms) {
      if (prevTransformMeta != null
          && (prevTransformMeta.equals(lookup)
              || (!checkedEntries.contains(prevTransformMeta)
                  && hasLoop(
                      prevTransformMeta,
                      lookup == null ? transformMeta : lookup,
                      checkedEntries)))) {
        hasLoop = true;
        break;
      }
    }

    loopCache.put(cacheKey, hasLoop);
    return hasLoop;
  }

  /** Mark all transforms in the pipeline as selected. */
  public void selectAll() {
    for (TransformMeta transform : transforms) {
      transform.setSelected(true);
    }
    for (NotePadMeta note : getNotes()) {
      note.setSelected(true);
    }

    setChanged();
    notifyObservers("refreshGraph");
  }

  /** Clear the selection of all transforms. */
  public void unselectAll() {
    for (TransformMeta transform : transforms) {
      transform.setSelected(false);
    }
    for (NotePadMeta note : getNotes()) {
      note.setSelected(false);
    }
  }

  /**
   * Get an array of all the selected transform locations.
   *
   * @return The selected transform locations.
   */
  public Point[] getSelectedTransformLocations() {
    List<Point> points = new ArrayList<>();

    for (TransformMeta transformMeta : getSelectedTransforms()) {
      Point p = transformMeta.getLocation();
      points.add(new Point(p.x, p.y)); // explicit copy of location
    }

    return points.toArray(new Point[0]);
  }

  /**
   * Get an array of all the selected note locations.
   *
   * @return The selected note locations.
   */
  public Point[] getSelectedNoteLocations() {
    List<Point> points = new ArrayList<>();

    for (NotePadMeta ni : getSelectedNotes()) {
      Point p = ni.getLocation();
      points.add(new Point(p.x, p.y)); // explicit copy of location
    }

    return points.toArray(new Point[0]);
  }

  /**
   * Gets a list of the selected transforms.
   *
   * @return A list of all the selected transforms.
   */
  public List<TransformMeta> getSelectedTransforms() {
    List<TransformMeta> selection = new ArrayList<>();
    for (TransformMeta transformMeta : transforms) {
      if (transformMeta.isSelected()) {
        selection.add(transformMeta);
      }
    }
    return selection;
  }

  /**
   * Gets an array of all the selected transform names.
   *
   * @return An array of all the selected transform names.
   */
  public String[] getSelectedTransformNames() {
    List<TransformMeta> selection = getSelectedTransforms();
    String[] retval = new String[selection.size()];
    for (int i = 0; i < retval.length; i++) {
      TransformMeta transformMeta = selection.get(i);
      retval[i] = transformMeta.getName();
    }
    return retval;
  }

  /**
   * Gets an array of the locations of an array of transforms.
   *
   * @param transforms An array of transforms
   * @return an array of the locations of an array of transforms
   */
  public int[] getTransformIndexes(List<TransformMeta> transforms) {
    int[] retval = new int[transforms.size()];

    for (int i = 0; i < transforms.size(); i++) {
      retval[i] = indexOfTransform(transforms.get(i));
    }

    return retval;
  }

  /**
   * Gets the maximum size of the canvas by calculating the maximum location of a transform.
   *
   * @return Maximum coordinate of a transform in the pipeline + (100,100) for safety.
   */
  public Point getMaximum() {
    int maxx = 0;
    int maxy = 0;
    for (TransformMeta transformMeta : getTransforms()) {
      Point loc = transformMeta.getLocation();
      if (loc.x > maxx) {
        maxx = loc.x;
      }
      if (loc.y > maxy) {
        maxy = loc.y;
      }
    }
    for (NotePadMeta notePadMeta : getNotes()) {
      Point loc = notePadMeta.getLocation();
      if (loc.x + notePadMeta.width > maxx) {
        maxx = loc.x + notePadMeta.width;
      }
      if (loc.y + notePadMeta.height > maxy) {
        maxy = loc.y + notePadMeta.height;
      }
    }

    return new Point(maxx + 100, maxy + 100);
  }

  /**
   * Gets the minimum point on the canvas of a pipeline.
   *
   * @return Minimum coordinate of a transform in the pipeline
   */
  public Point getMinimum() {
    int minx = Integer.MAX_VALUE;
    int miny = Integer.MAX_VALUE;
    for (TransformMeta transformMeta : getTransforms()) {
      Point loc = transformMeta.getLocation();
      if (loc.x < minx) {
        minx = loc.x;
      }
      if (loc.y < miny) {
        miny = loc.y;
      }
    }
    for (NotePadMeta notePadMeta : getNotes()) {
      Point loc = notePadMeta.getLocation();
      if (loc.x < minx) {
        minx = loc.x;
      }
      if (loc.y < miny) {
        miny = loc.y;
      }
    }

    if (minx > BORDER_INDENT && minx != Integer.MAX_VALUE) {
      minx -= BORDER_INDENT;
    } else {
      minx = 0;
    }
    if (miny > BORDER_INDENT && miny != Integer.MAX_VALUE) {
      miny -= BORDER_INDENT;
    } else {
      miny = 0;
    }

    return new Point(minx, miny);
  }

  /**
   * Gets the names of all the transforms.
   *
   * @return An array of transform names.
   */
  public String[] getTransformNames() {
    String[] transformNames = new String[nrTransforms()];
    for (int i = 0; i < nrTransforms(); i++) {
      transformNames[i] = getTransform(i).getName();
    }
    return transformNames;
  }

  /**
   * Gets all the transforms as an array.
   *
   * @return An array of all the transforms in the pipeline.
   */
  public TransformMeta[] getTransformsArray() {
    TransformMeta[] retval = new TransformMeta[nrTransforms()];

    for (int i = 0; i < nrTransforms(); i++) {
      retval[i] = getTransform(i);
    }

    return retval;
  }

  /**
   * Looks in the pipeline to find a transform in a previous location starting somewhere.
   *
   * @param startTransform The starting transform
   * @param transformToFind The transform to look for backward in the pipeline
   * @return true if we can find the transform in an earlier location in the pipeline.
   */
  public boolean findPrevious(TransformMeta startTransform, TransformMeta transformToFind) {
    String key = startTransform.getName() + " - " + transformToFind.getName();
    Boolean result = loopCache.get(key);
    if (result != null) {
      return result;
    }

    // Normal transforms
    //
    List<TransformMeta> previousTransforms = findPreviousTransforms(startTransform, false);
    for (TransformMeta transformMeta : previousTransforms) {
      if (transformMeta.equals(transformToFind)) {
        loopCache.put(key, true);
        return true;
      }

      boolean found =
          findPrevious(transformMeta, transformToFind); // Look further back in the tree.
      if (found) {
        loopCache.put(key, true);
        return true;
      }
    }

    // Info transforms
    List<TransformMeta> infoTransforms = findPreviousTransforms(startTransform, true);
    for (TransformMeta transformMeta : infoTransforms) {
      if (transformMeta.equals(transformToFind)) {
        loopCache.put(key, true);
        return true;
      }

      boolean found =
          findPrevious(transformMeta, transformToFind); // Look further back in the tree.
      if (found) {
        loopCache.put(key, true);
        return true;
      }
    }

    loopCache.put(key, false);
    return false;
  }

  /** The previous count. */
  private long prevCount;

  /**
   * Puts the transforms in a more natural order: from start to finish. For the moment, we ignore
   * splits and joins. Splits and joins can't be listed sequentially in any case!
   *
   * @return a map containing all the previous transforms per transform
   */
  public Map<TransformMeta, Map<TransformMeta, Boolean>> sortTransformsNatural() {
    long startTime = System.currentTimeMillis();

    prevCount = 0;

    // First create a map where all the previous transforms of another transform are kept...
    //
    final Map<TransformMeta, Map<TransformMeta, Boolean>> transformMap = new HashMap<>();

    // Also cache the previous transforms
    //
    final Map<TransformMeta, List<TransformMeta>> previousCache = new HashMap<>();

    // Cache calculation of transforms before another
    //
    Map<TransformMeta, Map<TransformMeta, Boolean>> beforeCache = new HashMap<>();

    for (TransformMeta transformMeta : transforms) {
      // What are the previous transforms? (cached version for performance)
      //
      List<TransformMeta> prevTransforms =
          previousCache.computeIfAbsent(
              transformMeta,
              f -> {
                prevCount++;
                return findPreviousTransforms(transformMeta);
              });

      // Now get the previous transforms recursively, store them in the transform map
      //
      for (TransformMeta prev : prevTransforms) {
        Map<TransformMeta, Boolean> beforePrevMap =
            updateFillTransformMap(previousCache, beforeCache, transformMeta, prev);
        transformMap.put(transformMeta, beforePrevMap);

        // Store it also in the beforeCache...
        //
        beforeCache.put(prev, beforePrevMap);
      }
    }

    transforms.sort(
        (o1, o2) -> {
          Map<TransformMeta, Boolean> beforeMap = transformMap.get(o1);
          if (beforeMap != null) {
            if (beforeMap.get(o2) == null) {
              return -1;
            } else {
              return 1;
            }
          } else {
            return o1.getName().compareToIgnoreCase(o2.getName());
          }
        });

    long endTime = System.currentTimeMillis();
    LogChannel.GENERAL.logBasic(
        BaseMessages.getString(
            PKG, "PipelineMeta.Log.TimeExecutionTransformSort", (endTime - startTime), prevCount));

    return transformMap;
  }

  /**
   * Fills a map with all transforms previous to the given transform. This method uses a caching
   * technique, so if a map is provided that contains the specified previous transform, it is
   * immediately returned to avoid unnecessary processing. Otherwise, the previous transforms are
   * determined and added to the map recursively, and a cache is constructed for later use.
   *
   * @param previousCache the previous cache, must be non-null
   * @param beforeCache the before cache, must be non-null
   * @param originTransformMeta the origin transform meta
   * @param previousTransformMeta the previous transform meta
   * @return the map
   */
  private Map<TransformMeta, Boolean> updateFillTransformMap(
      Map<TransformMeta, List<TransformMeta>> previousCache,
      Map<TransformMeta, Map<TransformMeta, Boolean>> beforeCache,
      TransformMeta originTransformMeta,
      TransformMeta previousTransformMeta) {

    // See if we have a hash map to store transform occurrence (located before the transform)
    //
    Map<TransformMeta, Boolean> beforeMap = beforeCache.get(previousTransformMeta);
    if (beforeMap == null) {
      beforeMap = new HashMap<>();
    } else {
      return beforeMap; // Nothing left to do here!
    }

    // Store the current previous transform in the map
    //
    beforeMap.put(previousTransformMeta, Boolean.TRUE);

    // Figure out all the previous transforms as well, they all need to go in there...
    //
    List<TransformMeta> prevTransforms =
        previousCache.computeIfAbsent(
            previousTransformMeta,
            e -> {
              prevCount++;
              return findPreviousTransforms(previousTransformMeta);
            });

    // Now, get the previous transforms for transformMeta recursively...
    // We only do this when the beforeMap is not known yet...
    //
    for (TransformMeta prev : prevTransforms) {
      Map<TransformMeta, Boolean> beforePrevMap =
          updateFillTransformMap(previousCache, beforeCache, originTransformMeta, prev);

      // Keep a copy in the cache...
      //
      beforeCache.put(prev, beforePrevMap);

      // Also add it to the new map for this transform...
      //
      beforeMap.putAll(beforePrevMap);
    }

    return beforeMap;
  }

  /** Sorts the hops in a natural way: from beginning to end. */
  public void sortHopsNatural() {
    // Loop over the hops...
    for (int j = 0; j < nrPipelineHops(); j++) {
      // Buble sort: we need to do this several times...
      for (int i = 0; i < nrPipelineHops() - 1; i++) {
        PipelineHopMeta one = getPipelineHop(i);
        PipelineHopMeta two = getPipelineHop(i + 1);

        TransformMeta a = two.getFromTransform();
        TransformMeta b = one.getToTransform();

        if (!findPrevious(a, b) && !a.equals(b)) {
          setPipelineHop(i + 1, one);
          setPipelineHop(i, two);
        }
      }
    }
  }

  /**
   * Determines the impact of the different transforms in a pipeline on databases, tables and field.
   *
   * @param impact An ArrayList of DatabaseImpact objects.
   * @param monitor a progress monitor listener to be updated as the pipeline is analyzed
   * @throws HopTransformException if any errors occur during analysis
   */
  public void analyseImpact(
      IVariables variables, List<DatabaseImpact> impact, IProgressMonitor monitor)
      throws HopTransformException {
    monitorBeginTask(
        monitor, nrTransforms(), "PipelineMeta.Monitor.GettingFieldsFromTransformTask.Title");

    boolean stop = false;
    for (int i = 0; i < nrTransforms() && !stop; i++) {
      monitorSubTask(
          monitor,
          "PipelineMeta.Monitor.LookingAtTransformTask.Title",
          (i + 1) + "/" + nrTransforms());

      TransformMeta transformMeta = getTransform(i);

      IRowMeta prev = getPrevTransformFields(variables, transformMeta);
      ITransformMeta iTransformMeta = transformMeta.getTransform();
      IRowMeta infoRowMeta;
      TransformMeta[] lu = getInfoTransform(transformMeta);
      if (lu != null) {
        infoRowMeta = getTransformFields(variables, lu);
      } else {
        try {
          infoRowMeta = iTransformMeta.getTableFields(variables);
        } catch (HopDatabaseException dbe) {
          throw new HopTransformException(
              "Error getting table fields from in transform " + transformMeta.getName(), dbe);
        }
      }

      iTransformMeta.analyseImpact(
          variables, impact, this, transformMeta, prev, null, null, infoRowMeta, metadataProvider);

      if (monitor != null) {
        monitor.worked(1);
        stop = monitor.isCanceled();
      }
    }

    if (monitor != null) {
      monitor.done();
    }
  }

  /**
   * Proposes an alternative transformName when the original already exists.
   *
   * @param transformName The transformName to find an alternative for
   * @return The suggested alternative transformName.
   */
  public String getAlternativeTransformName(String transformName) {
    String newname = transformName;
    TransformMeta transformMeta = findTransform(newname);
    int nr = 1;
    while (transformMeta != null) {
      nr++;
      newname = transformName + " " + nr;
      transformMeta = findTransform(newname);
    }

    return newname;
  }

  /**
   * Builds a list of all the Sql statements that this pipeline needs in order to work properly.
   *
   * @param variables the variables to resolve variable expressions with
   * @return An ArrayList of SqlStatement objects.
   * @throws HopTransformException if any errors occur during Sql statement generation
   */
  public List<SqlStatement> getSqlStatements(IVariables variables) throws HopTransformException {
    return getSqlStatements(variables, null);
  }

  /**
   * Builds a list of all the Sql statements that this pipeline needs in order to work properly.
   *
   * @param variables the variables to resolve variable expressions with
   * @param monitor a progress monitor listener to be updated as the Sql statements are generated
   * @return An ArrayList of SqlStatement objects.
   * @throws HopTransformException if any errors occur during Sql statement generation
   */
  public List<SqlStatement> getSqlStatements(IVariables variables, IProgressMonitor monitor)
      throws HopTransformException {
    monitorBeginTask(
        monitor, nrTransforms() + 1, "PipelineMeta.Monitor.GettingTheSQLForPipelineTask.Title");
    List<SqlStatement> stats = new ArrayList<>();

    for (int i = 0; i < nrTransforms(); i++) {
      TransformMeta transformMeta = getTransform(i);
      monitorSubTask(
          monitor,
          "PipelineMeta.Monitor.GettingTheSQLForTransformTask.Title",
          transformMeta.toString());

      IRowMeta prev = getPrevTransformFields(variables, transformMeta);
      SqlStatement sql =
          transformMeta
              .getTransform()
              .getSqlStatements(variables, this, transformMeta, prev, metadataProvider);
      if (sql.getSql() != null || sql.hasError()) {
        stats.add(sql);
      }
      if (monitor != null) {
        monitor.worked(1);
      }
    }

    if (monitor != null) {
      monitor.done();
    }

    return stats;
  }

  /**
   * Get the SQL statements (needed to run this pipeline) as a single String.
   *
   * @return the SQL statements needed to run this pipeline
   * @throws HopTransformException if any errors occur during SQL statement generation
   */
  public String getSqlStatementsString(IVariables variables) throws HopTransformException {
    StringBuilder sql = new StringBuilder();
    List<SqlStatement> statements = getSqlStatements(variables);
    for (SqlStatement statement : statements) {
      if (!statement.hasError() && statement.hasSql()) {
        sql.append(statement.getSql());
      }
    }

    return sql.toString();
  }

  /**
   * Checks all the transforms and fills a List with CheckResult remarks.
   *
   * @param remarks The remarks list to add to.
   * @param onlySelected true to check only the selected transforms, false for all transforms
   * @param monitor a progress monitor listener to be updated as the SQL statements are generated
   */
  public void checkTransforms(
      List<ICheckResult> remarks,
      boolean onlySelected,
      IProgressMonitor monitor,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    try {
      // Start with a clean slate...
      remarks.clear();

      if (monitor == null) {
        monitor = new ProgressNullMonitorListener();
      }

      Map<IValueMeta, String> values = new Hashtable<>();

      List<TransformMeta> transformsToCheck =
          (onlySelected) ? getSelectedTransforms() : getTransforms();

      TransformMeta[] transformArray = transformsToCheck.toArray(new TransformMeta[0]);

      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL,
          variables,
          HopExtensionPoint.BeforeCheckTransforms.id,
          new CheckTransformsExtension(remarks, variables, this, transformArray, metadataProvider));

      boolean stopChecking = false;

      monitor.beginTask(
          BaseMessages.getString(PKG, "PipelineMeta.Monitor.VerifyingThisPipelineTask.Title"),
          transformsToCheck.size());

      int worked = 1;
      for (TransformMeta transformMeta : transformsToCheck) {

        if (stopChecking) {
          break;
        }

        monitor.subTask(
            BaseMessages.getString(
                PKG, "PipelineMeta.Monitor.VerifyingTransformTask.Title", transformMeta.getName()));

        // Check missing transform plugin
        if (transformMeta.getTransform() instanceof Missing missingTransform) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG,
                      "PipelineMeta.CheckResult.TypeResultError.TransformPluginNotFound.Description",
                      missingTransform.getMissingPluginId()),
                  transformMeta));
        }

        // Check deprecated transform plugin
        if (transformMeta.isDeprecated()) {
          remarks.add(
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(
                      PKG,
                      "PipelineMeta.CheckResult.TypeResultWarning.DeprecatedTransformPlugin.Description"),
                  transformMeta));
        }

        int nrInfoTransforms = findNrInfoTransforms(transformMeta);
        TransformMeta[] infoTransform = null;
        if (nrInfoTransforms > 0) {
          infoTransform = getInfoTransform(transformMeta);
        }

        IRowMeta infoRowMeta = null;
        if (infoTransform != null) {
          try {
            infoRowMeta = getTransformFields(variables, infoTransform);
          } catch (HopTransformException kse) {
            CheckResult cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG,
                        "PipelineMeta.CheckResult.TypeResultError.ErrorOccurredGettingTransformMetaFields.Description",
                        "" + transformMeta,
                        Const.CR + kse.getMessage()),
                    transformMeta);
            remarks.add(cr);
          }
        }

        // The previous fields from non-informative transformsToCheck:
        IRowMeta prev = null;
        try {
          prev = getPrevTransformFields(variables, transformMeta);
        } catch (HopTransformException kse) {
          CheckResult cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_ERROR,
                  BaseMessages.getString(
                      PKG,
                      "PipelineMeta.CheckResult.TypeResultError.ErrorOccurredGettingInputFields.Description",
                      "" + transformMeta,
                      Const.CR + kse.getMessage()),
                  transformMeta);
          remarks.add(cr);
          // This is a severe error: stop checking...
          // Otherwise we wind up checking time & time again because nothing gets put in the
          // database
          // cache, the timeout of certain databases is very long... (Oracle)
          stopChecking = true;
        }

        if (isTransformUsedInPipelineHops(transformMeta) || getTransforms().size() == 1) {
          // Get the input & output transformsToCheck!
          // Copy to arrays:
          String[] input = getPrevTransformNames(transformMeta);
          String[] output = getNextTransformNames(transformMeta);

          // Check transform specific info...
          ExtensionPointHandler.callExtensionPoint(
              LogChannel.GENERAL,
              variables,
              HopExtensionPoint.BeforeCheckTransform.id,
              new CheckTransformsExtension(
                  remarks, variables, this, new TransformMeta[] {transformMeta}, metadataProvider));
          transformMeta.check(
              remarks, this, prev, input, output, infoRowMeta, variables, metadataProvider);
          ExtensionPointHandler.callExtensionPoint(
              LogChannel.GENERAL,
              variables,
              HopExtensionPoint.AfterCheckTransform.id,
              new CheckTransformsExtension(
                  remarks, variables, this, new TransformMeta[] {transformMeta}, metadataProvider));

          // See if illegal characters etc. were used in field-names...
          if (prev != null) {
            for (int x = 0; x < prev.size(); x++) {
              IValueMeta valueMeta = prev.getValueMeta(x);
              String name = valueMeta.getName();
              if (name == null) {
                values.put(
                    valueMeta,
                    BaseMessages.getString(
                        PKG, "PipelineMeta.Value.CheckingFieldName.FieldNameIsEmpty.Description"));
              } else if (name.indexOf(' ') >= 0) {
                values.put(
                    valueMeta,
                    BaseMessages.getString(
                        PKG,
                        "PipelineMeta.Value.CheckingFieldName.FieldNameContainsSpaces.Description",
                        valueMeta.getName()));
              } else {
                char[] list =
                    new char[] {
                      '.', ',', '-', '/', '+', '*', '\'', '\t', '"', '|', '@', '(', ')', '{', '}',
                      '!', '^'
                    };
                for (char value : list) {
                  if (name.indexOf(value) >= 0) {
                    values.put(
                        valueMeta,
                        BaseMessages.getString(
                            PKG,
                            "PipelineMeta.Value.CheckingFieldName.FieldNameContainsUnfriendlyCodes.Description",
                            valueMeta.getName(),
                            String.valueOf(value)));
                  }
                }
              }
            }

            // Check if 2 transformsToCheck with the same name are entering the transform...
            if (prev.size() > 1) {
              String[] fieldNames = prev.getFieldNames();
              String[] sortedNames = Const.sortStrings(fieldNames);

              String prevName = sortedNames[0];
              for (int x = 1; x < sortedNames.length; x++) {
                // Checking for doubles
                if (prevName.equalsIgnoreCase(sortedNames[x])) {
                  // Give a warning!!
                  CheckResult cr =
                      new CheckResult(
                          ICheckResult.TYPE_RESULT_ERROR,
                          BaseMessages.getString(
                              PKG,
                              "PipelineMeta.CheckResult.TypeResultWarning.HaveTheSameNameField.Description",
                              prevName),
                          transformMeta);
                  remarks.add(cr);
                } else {
                  prevName = sortedNames[x];
                }
              }
            }
          } else {
            CheckResult cr =
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                            PKG,
                            "PipelineMeta.CheckResult.TypeResultError.CannotFindPreviousFields.Description")
                        + transformMeta.getName(),
                    transformMeta);
            remarks.add(cr);
          }
        } else {
          CheckResult cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(
                      PKG,
                      "PipelineMeta.CheckResult.TypeResultWarning.TransformIsNotUsed.Description"),
                  transformMeta);
          remarks.add(cr);
        }

        // Also check for mixing rows...
        try {
          checkRowMixingStatically(variables, transformMeta, null);
        } catch (HopRowException e) {
          CheckResult cr =
              new CheckResult(ICheckResult.TYPE_RESULT_ERROR, e.getMessage(), transformMeta);
          remarks.add(cr);
        }

        monitor.worked(worked++); // progress bar...
        if (monitor.isCanceled()) {
          stopChecking = true;
        }
      }

      monitor.subTask(
          BaseMessages.getString(
              PKG,
              "PipelineMeta.Monitor.CheckingForDatabaseUnfriendlyCharactersInFieldNamesTask.Title"));

      if (!values.isEmpty()) {
        for (Map.Entry<IValueMeta, String> entry : values.entrySet()) {
          IValueMeta valueMeta = entry.getKey();
          String message = entry.getValue();
          CheckResult cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING, message, findTransform(valueMeta.getOrigin()));
          remarks.add(cr);
        }
      } else {
        CheckResult cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "PipelineMeta.CheckResult.TypeResultOK.Description"),
                null);
        remarks.add(cr);
      }

      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL,
          variables,
          HopExtensionPoint.AfterCheckTransforms.id,
          new CheckTransformsExtension(remarks, variables, this, transformArray, metadataProvider));

      monitor.done();
    } catch (Exception e) {
      throw new HopRuntimeException("Error checking pipeline", e);
    }
  }

  /**
   * Gets the version of the pipeline.
   *
   * @return The version of the pipeline
   */
  public String getPipelineVersion() {
    return info.getPipelineVersion();
  }

  /**
   * Sets the version of the pipeline.
   *
   * @param pipelineVersion The new version description of the pipeline
   */
  public void setPipelineVersion(String pipelineVersion) {
    info.setPipelineVersion(pipelineVersion);
  }

  /**
   * Gets a textual representation of the pipeline. If its name has been set, it will be returned,
   * otherwise the classname is returned.
   *
   * @return the textual representation of the pipeline.
   */
  @Override
  public String toString() {
    if (!Utils.isEmpty(filename)) {
      if (Utils.isEmpty(getName())) {
        return filename;
      } else {
        return filename + " : " + getName();
      }
    }

    if (getName() != null) {
      return getName();
    } else {
      return PipelineMeta.class.getName();
    }
  }

  /**
   * Cancels queries opened for checking &amp; fieldprediction.
   *
   * @throws HopDatabaseException if any errors occur during query cancellation
   */
  public void cancelQueries() throws HopDatabaseException {
    for (int i = 0; i < nrTransforms(); i++) {
      getTransform(i).getTransform().cancelQueries();
    }
  }

  /**
   * Gets a list of all the strings used in this pipeline. The parameters indicate which collections
   * to search and which to exclude.
   *
   * @param searchTransforms true if transforms should be searched, false otherwise
   * @param searchDatabases true if databases should be searched, false otherwise
   * @param searchNotes true if notes should be searched, false otherwise
   * @param includePasswords true if passwords should be searched, false otherwise
   * @return a list of search results for strings used in the pipeline.
   */
  public List<StringSearchResult> getStringList(
      boolean searchTransforms,
      boolean searchDatabases,
      boolean searchNotes,
      boolean includePasswords) {
    List<StringSearchResult> stringList = new ArrayList<>();

    if (searchTransforms) {
      // Loop over all transforms in the pipeline and see what the used vars are...
      for (int i = 0; i < nrTransforms(); i++) {
        TransformMeta transformMeta = getTransform(i);
        stringList.add(
            new StringSearchResult(
                transformMeta.getName(),
                transformMeta,
                this,
                BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.TransformName")));
        if (transformMeta.getDescription() != null) {
          stringList.add(
              new StringSearchResult(
                  transformMeta.getDescription(),
                  transformMeta,
                  this,
                  BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.TransformDescription")));
        }
        ITransformMeta metaInterface = transformMeta.getTransform();
        StringSearcher.findMetaData(metaInterface, 1, stringList, transformMeta, this);
      }
    }

    // Loop over all transforms in the pipeline and see what the used vars are...
    if (searchDatabases) {
      for (DatabaseMeta meta : getDatabases()) {
        stringList.add(
            new StringSearchResult(
                meta.getName(),
                meta,
                this,
                BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.DatabaseConnectionName")));
        if (meta.getHostname() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getHostname(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.DatabaseHostName")));
        }
        if (meta.getDatabaseName() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getDatabaseName(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.DatabaseName")));
        }
        if (meta.getUsername() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getUsername(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.DatabaseUsername")));
        }
        if (meta.getPluginId() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getPluginId(),
                  meta,
                  this,
                  BaseMessages.getString(
                      PKG, "PipelineMeta.SearchMetadata.DatabaseTypeDescription")));
        }
        if (meta.getPort() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getPort(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.DatabasePort")));
        }
        if (meta.getServername() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getServername(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.DatabaseServer")));
        }
        if (includePasswords && meta.getPassword() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getPassword(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.DatabasePassword")));
        }
      }
    }

    // Loop over all transforms in the pipeline and see what the used vars are...
    if (searchNotes) {
      for (int i = 0; i < nrNotes(); i++) {
        NotePadMeta meta = getNote(i);
        if (meta.getNote() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getNote(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.NotepadText")));
        }
      }
    }

    return stringList;
  }

  /**
   * Get a list of all the strings used in this pipeline. The parameters indicate which collections
   * to search and which to exclude.
   *
   * @param searchTransforms true if transforms should be searched, false otherwise
   * @param searchDatabases true if databases should be searched, false otherwise
   * @param searchNotes true if notes should be searched, false otherwise
   * @return a list of search results for strings used in the pipeline.
   */
  public List<StringSearchResult> getStringList(
      boolean searchTransforms, boolean searchDatabases, boolean searchNotes) {
    return getStringList(searchTransforms, searchDatabases, searchNotes, false);
  }

  /**
   * Gets a list of the used variables in this pipeline.
   *
   * @return a list of the used variables in this pipeline.
   */
  public List<String> getUsedVariables() {
    // Get the list of Strings.
    List<StringSearchResult> stringList = getStringList(true, true, false, true);

    List<String> varList = new ArrayList<>();

    // Look around in the strings, see what we find...
    for (StringSearchResult result : stringList) {
      StringUtil.getUsedVariables(result.getString(), varList, false);
    }

    return varList;
  }

  /**
   * Check a transform to see if there are no multiple transforms to read from. If so, check to see
   * if the receiving rows are all the same in layout. We only want to ONLY use the DbCache for this
   * to prevent GUI stalls.
   *
   * @param transformMeta the transform to check
   * @param monitor the monitor
   * @throws HopRowException in case we detect a row mixing violation
   */
  public void checkRowMixingStatically(
      IVariables variables, TransformMeta transformMeta, IProgressMonitor monitor)
      throws HopRowException {
    List<TransformMeta> prevTransforms = findPreviousTransforms(transformMeta);
    int nrPrevious = prevTransforms.size();
    if (nrPrevious > 1) {
      IRowMeta referenceRow = null;
      // See if all previous transforms send out the same rows...
      for (TransformMeta previousTransform : prevTransforms) {
        try {
          IRowMeta row =
              getTransformFields(
                  variables, previousTransform, monitor); // Throws HopTransformException
          if (referenceRow == null) {
            referenceRow = row;
          } else if (!transformMeta.getTransform().excludeFromRowLayoutVerification()) {
            BaseTransform.safeModeChecking(referenceRow, row);
          }
        } catch (HopTransformException e) {
          // We ignore this one because we are in the process of designing the pipeline, anything
          // intermediate can
          // go wrong.
        }
      }
    }
  }

  /**
   * Sets the internal hop variables.
   *
   * @param variables the new internal hop variables
   */
  @Override
  public void setInternalHopVariables(IVariables variables) {
    setInternalFilenameHopVariables(variables);
    setInternalNameHopVariable(variables);
    setInternalEntryCurrentDirectory(variables);

    HopVersionProvider versionProvider = new HopVersionProvider();
    variables.setVariable(Const.HOP_VERSION, versionProvider.getVersion()[0]);
  }

  /**
   * Sets the internal name hop variable.
   *
   * @param variables the new internal name hop variable
   */
  @Override
  protected void setInternalNameHopVariable(IVariables variables) {
    // The name of the pipeline
    //
    variables.setVariable(Const.INTERNAL_VARIABLE_PIPELINE_NAME, Const.NVL(getName(), ""));
  }

  /**
   * Sets the internal filename hop variables.
   *
   * @param variables the new internal filename hop variables
   */
  @Override
  protected void setInternalFilenameHopVariables(IVariables variables) {
    // If we have a filename that's defined, set variables. If not, clear them.
    //
    if (!Utils.isEmpty(filename)) {
      try {
        FileObject fileObject = HopVfs.getFileObject(filename);
        FileName fileName = fileObject.getName();

        // The filename of the pipeline
        variables.setVariable(
            Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_NAME, fileName.getBaseName());

        // The directory of the pipeline
        FileName fileDir = fileName.getParent();
        variables.setVariable(
            Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, fileDir.getURI());
      } catch (HopFileException e) {
        LogChannel.GENERAL.logError("Unexpected error setting internal filename variables!", e);

        variables.setVariable(Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "");
        variables.setVariable(Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_NAME, "");
      }
    } else {
      variables.setVariable(Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY, "");
      variables.setVariable(Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_NAME, "");
    }

    setInternalEntryCurrentDirectory(variables);
  }

  protected void setInternalEntryCurrentDirectory(IVariables variables) {
    variables.setVariable(
        Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER,
        variables.getVariable(
            StringUtils.isNotEmpty(filename)
                ? Const.INTERNAL_VARIABLE_PIPELINE_FILENAME_DIRECTORY
                : Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  /**
   * Gets a list of the resource dependencies.
   *
   * @return a list of ResourceReferences
   */
  public List<ResourceReference> getResourceDependencies(IVariables variables) {
    return transforms.stream()
        .flatMap(
            (TransformMeta transformMeta) ->
                transformMeta.getResourceDependencies(variables).stream())
        .toList();
  }

  /**
   * Exports the specified objects to a flat-file system, adding content with filename keys to a set
   * of definitions. The supplied resource naming interface allows the object to name appropriately
   * without worrying about those parts of the implementation specific details.
   *
   * @param variables the variable variables to use
   * @param definitions The definitions to use
   * @param iResourceNaming The resource naming to use
   * @param metadataProvider the metadataProvider in which non-hop metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming iResourceNaming,
      IHopMetadataProvider metadataProvider)
      throws HopException {

    String exportFileName = null;
    try {
      // Handle naming for XML bases resources...
      //
      String baseName;
      String originalPath;
      String fullName;
      String extension = "hpl";
      if (StringUtils.isNotEmpty(getFilename())) {
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(getFilename()));
        originalPath = fileObject.getParent().getURL().toString();
        baseName = fileObject.getName().getBaseName();
        fullName = fileObject.getURL().toString();

        exportFileName =
            iResourceNaming.nameResource(
                baseName, originalPath, extension, IResourceNaming.FileNamingType.PIPELINE);
        ResourceDefinition definition = definitions.get(exportFileName);
        if (definition == null) {
          // Create a copy of this pipeline metadata by serializing to and de-serializing from XML.
          //
          String xml = getXml(variables);
          Node node = XmlHandler.loadXmlString(xml, XML_TAG);
          PipelineMeta pipelineMeta = new PipelineMeta();
          pipelineMeta.loadXml(node, exportFileName, metadataProvider, variables);

          // Set an appropriate name
          //
          pipelineMeta.setNameSynchronizedWithFilename(false);
          pipelineMeta.setName(getName());

          // Add used resources, modify pipelineMeta accordingly
          // Go through the list of transforms, etc.
          // These critters change the transforms in the cloned PipelineMeta
          // At the end we make a new XML version of it in "exported"
          // format...

          // loop over transforms, databases will be exported to XML anyway.
          //
          for (TransformMeta transformMeta : pipelineMeta.getTransforms()) {
            transformMeta.exportResources(
                variables, definitions, iResourceNaming, metadataProvider);
          }

          // Change the filename, calling this sets internal variables
          // inside of the pipeline.
          //
          pipelineMeta.setFilename(exportFileName);

          // At the end, add ourselves to the map...
          //
          String pipelineMetaContent = pipelineMeta.getXml(variables);

          definition = new ResourceDefinition(exportFileName, pipelineMetaContent);

          // Also remember the original filename (if any), including variables etc.
          //
          if (Utils.isEmpty(this.getFilename())) { // Generated
            definition.setOrigin(fullName);
          } else {
            definition.setOrigin(this.getFilename());
          }

          definitions.put(fullName, definition);
        }
      }

      return exportFileName;
    } catch (FileSystemException | HopFileException e) {
      throw new HopException(
          BaseMessages.getString(PKG, CONST_ERROR_OPENING_OR_VALIDATING, getFilename()), e);
    }
  }

  /**
   * Checks whether the pipeline is capturing transform performance snapshots.
   *
   * @return true if the pipeline is capturing transform performance snapshots, false otherwise
   */
  public boolean isCapturingTransformPerformanceSnapShots() {
    return info.isCapturingTransformPerformanceSnapShots();
  }

  /**
   * Sets whether the pipeline is capturing transform performance snapshots.
   *
   * @param capturingTransformPerformanceSnapShots true if the pipeline is capturing transform
   *     performance snapshots, false otherwise
   */
  public void setCapturingTransformPerformanceSnapShots(
      boolean capturingTransformPerformanceSnapShots) {
    this.info.setCapturingTransformPerformanceSnapShots(capturingTransformPerformanceSnapShots);
  }

  /**
   * Gets the transform performance capturing delay.
   *
   * @return the transform performance capturing delay
   */
  public long getTransformPerformanceCapturingDelay() {
    return info.getTransformPerformanceCapturingDelay();
  }

  /**
   * Sets the transform performance capturing delay.
   *
   * @param transformPerformanceCapturingDelay the transformPerformanceCapturingDelay to set
   */
  public void setTransformPerformanceCapturingDelay(long transformPerformanceCapturingDelay) {
    this.info.setTransformPerformanceCapturingDelay(transformPerformanceCapturingDelay);
  }

  /**
   * Gets the transform performance capturing size limit.
   *
   * @return the transform performance capturing size limit
   */
  public String getTransformPerformanceCapturingSizeLimit() {
    return info.getTransformPerformanceCapturingSizeLimit();
  }

  /**
   * Sets the transform performance capturing size limit.
   *
   * @param transformPerformanceCapturingSizeLimit the transform performance capturing size limit to
   *     set
   */
  public void setTransformPerformanceCapturingSizeLimit(
      String transformPerformanceCapturingSizeLimit) {
    this.info.setTransformPerformanceCapturingSizeLimit(transformPerformanceCapturingSizeLimit);
  }

  /** Clears the transform fields and loop caches. */
  public void clearCaches() {
    clearTransformFieldsCache();
    clearLoopCache();
    clearPreviousTransformCache();
  }

  /** Clears the transform fields cache. */
  private void clearTransformFieldsCache() {
    transformFieldsCache.clear();
  }

  /** Clears the loop cache. */
  private void clearLoopCache() {
    loopCache.clear();
  }

  @VisibleForTesting
  void clearPreviousTransformCache() {
    previousTransformCache.clear();
  }

  /**
   * Gets the pipeline type.
   *
   * @return the pipelineType
   */
  public PipelineType getPipelineType() {
    return info.getPipelineType();
  }

  /**
   * Sets the pipeline type.
   *
   * @param pipelineType the pipelineType to set
   */
  public void setPipelineType(PipelineType pipelineType) {
    this.info.setPipelineType(pipelineType);
  }

  public void addTransformChangeListener(ITransformMetaChangeListener listener) {
    transformChangeListeners.add(listener);
  }

  public void addTransformChangeListener(
      int transformIndex, ITransformMetaChangeListener listener) {
    TransformMeta rewriteTransform = transforms.get(transformIndex);
    ITransformMeta iTransformMeta = rewriteTransform.getTransform();
    if (iTransformMeta instanceof ITransformMetaChangeListener changeListener) {
      int index = transformChangeListeners.indexOf(changeListener);
      if (index >= 0) {
        transformChangeListeners.set(index, listener);
      } else {
        transformChangeListeners.add(listener);
      }
    }
  }

  public void removeTransformChangeListener(ITransformMetaChangeListener list) {
    transformChangeListeners.remove(list);
  }

  public void notifyAllListeners(TransformMeta oldMeta, TransformMeta newMeta) {
    for (ITransformMetaChangeListener listener : transformChangeListeners) {
      listener.onTransformChange(this, oldMeta, newMeta);
    }
  }

  public boolean containsTransformMeta(TransformMeta transformMeta) {
    return transforms.contains(transformMeta);
  }

  public List<Missing> getMissingPipeline() {
    return missingPipeline;
  }

  public void addMissingPipeline(Missing pipeline) {
    if (missingPipeline == null) {
      missingPipeline = new ArrayList<>();
    }
    missingPipeline.add(pipeline);
  }

  public void removeMissingPipeline(Missing pipeline) {
    if (missingPipeline != null && pipeline != null) {
      missingPipeline.remove(pipeline);
    }
  }

  @Override
  public boolean hasMissingPlugins() {
    return !Utils.isEmpty(missingPipeline);
  }

  private static String getTransformMetaCacheKey(TransformMeta transformMeta, boolean info) {
    return String.format(
        "%1$b-%2$s-%3$s", info, transformMeta.getTransformPluginId(), transformMeta);
  }

  private static IRowMeta[] cloneRowMetaInterfaces(IRowMeta[] inform) {
    IRowMeta[] cloned = inform.clone();
    for (int i = 0; i < cloned.length; i++) {
      if (cloned[i] != null) {
        cloned[i] = cloned[i].clone();
      }
    }
    return cloned;
  }

  public boolean isEmpty() {
    return nrTransforms() == 0 && nrNotes() == 0;
  }

  /**
   * The PipelineType enum describes the various types of pipelines in terms of execution, including
   * Normal, Serial Single-Threaded, and Single-Threaded.
   */
  @SuppressWarnings("java:S115")
  @Getter
  public enum PipelineType implements IEnumHasCodeAndDescription {
    /** A normal pipeline. */
    Normal("Normal", BaseMessages.getString(PKG, "PipelineMeta.PipelineType.Normal")),

    /** A single-threaded pipeline. */
    SingleThreaded(
        "SingleThreaded", BaseMessages.getString(PKG, "PipelineMeta.PipelineType.SingleThreaded"));

    /** The code corresponding to the pipeline type. */
    private final String code;

    /** The description of the pipeline type. */
    private final String description;

    /**
     * Instantiates a new pipeline type.
     *
     * @param code the code
     * @param description the description
     */
    PipelineType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    /**
     * Gets the pipeline type by code.
     *
     * @param pipelineTypeCode the pipeline type code
     * @return the pipeline type by code
     */
    public static PipelineType getPipelineTypeByCode(String pipelineTypeCode) {
      if (pipelineTypeCode != null) {
        for (PipelineType type : values()) {
          if (type.code.equalsIgnoreCase(pipelineTypeCode)) {
            return type;
          }
        }
      }
      return Normal;
    }

    /**
     * Gets the pipeline types descriptions.
     *
     * @return the pipeline types descriptions
     */
    public static String[] getPipelineTypesDescriptions() {
      String[] desc = new String[values().length];
      for (int i = 0; i < values().length; i++) {
        desc[i] = values()[i].getDescription();
      }
      return desc;
    }
  }

  /**
   * Get the name of the pipeline. If the name is synchronized with the filename, we return the base
   * filename.
   *
   * @return The name of the pipeline
   */
  @Override
  public String getName() {
    return extractNameFromFilename(
        isNameSynchronizedWithFilename(), info.getName(), filename, getExtension());
  }

  /**
   * Set the name.
   *
   * @param newName The new name
   */
  @Override
  public void setName(String newName) {
    fireNameChangedListeners(getName(), newName);
    this.info.setName(newName);
  }

  @Override
  public boolean isNameSynchronizedWithFilename() {
    return info.isNameSynchronizedWithFilename();
  }

  @Override
  public void setNameSynchronizedWithFilename(boolean nameSynchronizedWithFilename) {
    info.setNameSynchronizedWithFilename(nameSynchronizedWithFilename);
  }

  /**
   * Gets the description of the workflow.
   *
   * @return The description of the workflow
   */
  public String getDescription() {
    return info.getDescription();
  }

  /**
   * Set the description of the workflow.
   *
   * @param description The new description of the workflow
   */
  public void setDescription(String description) {
    this.info.setDescription(description);
  }

  /**
   * Gets the extended description of the workflow.
   *
   * @return The extended description of the workflow
   */
  public String getExtendedDescription() {
    return info.getExtendedDescription();
  }

  /**
   * Set the description of the workflow.
   *
   * @param extendedDescription The new extended description of the workflow
   */
  public void setExtendedDescription(String extendedDescription) {
    info.setExtendedDescription(extendedDescription);
  }

  /**
   * Gets the date the pipeline was created.
   *
   * @return the date the pipeline was created.
   */
  @Override
  public Date getCreatedDate() {
    return info.getCreatedDate();
  }

  /**
   * Sets the date the pipeline was created.
   *
   * @param createdDate The creation date to set.
   */
  @Override
  public void setCreatedDate(Date createdDate) {
    info.setCreatedDate(createdDate);
  }

  /**
   * Sets the user by whom the pipeline was created.
   *
   * @param createdUser The user to set.
   */
  @Override
  public void setCreatedUser(String createdUser) {
    info.setCreatedUser(createdUser);
  }

  /**
   * Gets the user by whom the pipeline was created.
   *
   * @return the user by whom the pipeline was created.
   */
  @Override
  public String getCreatedUser() {
    return info.getCreatedUser();
  }

  /**
   * Sets the date the pipeline was modified.
   *
   * @param modifiedDate The modified date to set.
   */
  @Override
  public void setModifiedDate(Date modifiedDate) {
    info.setModifiedDate(modifiedDate);
  }

  /**
   * Gets the date the pipeline was modified.
   *
   * @return the date the pipeline was modified.
   */
  @Override
  public Date getModifiedDate() {
    return info.getModifiedDate();
  }

  /**
   * Sets the user who last modified the pipeline.
   *
   * @param modifiedUser The username to set.
   */
  @Override
  public void setModifiedUser(String modifiedUser) {
    info.setModifiedUser(modifiedUser);
  }

  /**
   * Gets the user who last modified the pipeline.
   *
   * @return the user who last modified the pipeline.
   */
  @Override
  public String getModifiedUser() {
    return info.getModifiedUser();
  }

  @Override
  protected INamedParameters getNamedParameters() {
    return info.namedParams;
  }

  @HopMetadataProperty(
      groupKey = "transform_error_handling",
      key = "error",
      listItemClass = TransformErrorMeta.class)
  public List<TransformErrorMeta> getTransformErrorMetas() {
    List<TransformErrorMeta> errorMetas = new ArrayList<>();
    for (TransformMeta transformMeta : transforms) {
      if (transformMeta.getTransformErrorMeta() != null) {
        errorMetas.add(transformMeta.getTransformErrorMeta());
      }
    }
    return errorMetas;
  }

  @HopMetadataProperty(
      groupKey = "transform_error_handling",
      key = "error",
      listItemClass = TransformErrorMeta.class)
  public void setTransformErrorMetas(List<TransformErrorMeta> errorMetas) {
    for (TransformErrorMeta errorMeta : errorMetas) {
      TransformMeta transformMeta = errorMeta.getSourceTransform();
      if (transformMeta != null) {
        transformMeta.setTransformErrorMeta(errorMeta);
      }
    }
  }
}
