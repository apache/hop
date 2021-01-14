// CHECKSTYLE:FileLength:OFF
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

package org.apache.hop.pipeline;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.Counter;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Result;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.attributes.AttributesUtil;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopMissingPluginsException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObjectType;
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
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.ITransformMetaChangeListener;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformPartitioningMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transforms.missing.Missing;
import org.apache.hop.pipeline.transforms.pipelineexecutor.PipelineExecutorMeta;
import org.apache.hop.pipeline.transforms.workflowexecutor.WorkflowExecutorMeta;
import org.apache.hop.resource.IResourceExport;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class defines information about a pipeline and offers methods to save and load it from XML
 * as well as methods to alter a pipeline by adding/removing databases, transforms, hops, etc.
 *
 * @author Matt Casters
 * @since 20-jun-2003
 */
public class PipelineMeta extends AbstractMeta
    implements IXml,
        Comparator<PipelineMeta>,
        Comparable<PipelineMeta>,
        Cloneable,
        IResourceExport,
        ILoggingObject,
        IHasFilename {

  public static final String PIPELINE_EXTENSION = ".hpl";

  private static final Class<?> PKG = Pipeline.class; // For Translator

  /** A constant specifying the tag value for the XML node of the pipeline. */
  public static final String XML_TAG = "pipeline";

  public static final int BORDER_INDENT = 20;
  /** The list of transforms associated with the pipeline. */
  protected List<TransformMeta> transforms;

  /** The list of hops associated with the pipeline. */
  protected List<PipelineHopMeta> hops;

  /** The version string for the pipeline. */
  protected String pipelineVersion;

  /** The status of the pipeline. */
  protected int pipelineStatus;

  /**
   * A table of named counters.
   *
   * @deprecated Moved to Pipeline
   */
  @Deprecated protected Hashtable<String, Counter> counters;

  /** Indicators for changes in transforms, databases, hops, and notes. */
  protected boolean changedTransforms, changedHops;

  /** The previous result. */
  protected Result previousResult;

  /** Whether the pipeline is capturing transform performance snap shots. */
  protected boolean capturingTransformPerformanceSnapShots;

  /** The transform performance capturing delay. */
  protected long transformPerformanceCapturingDelay;

  /** The transform performance capturing size limit. */
  protected String transformPerformanceCapturingSizeLimit;

  /** The transforms fields cache. */
  protected Map<String, IRowMeta> transformFieldsCache;

  /** The loop cache. */
  protected Map<String, Boolean> loopCache;

  /** The previous transform cache */
  protected Map<String, List<TransformMeta>> previousTransformCache;

  /** The log channel interface. */
  protected ILogChannel log;

  /** The list of TransformChangeListeners */
  protected List<ITransformMetaChangeListener> transformChangeListeners;

  protected byte[] keyForSessionKey;
  boolean isKeyPrivate;
  private ArrayList<Missing> missingPipeline;

  /**
   * The PipelineType enum describes the various types of pipelines in terms of execution, including
   * Normal, Serial Single-Threaded, and Single-Threaded.
   */
  public enum PipelineType {

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
     * Gets the code corresponding to the pipeline type.
     *
     * @return the code
     */
    public String getCode() {
      return code;
    }

    /**
     * Gets the description of the pipeline type.
     *
     * @return the description
     */
    public String getDescription() {
      return description;
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

  /** The pipeline type. */
  protected PipelineType pipelineType;

  // //////////////////////////////////////////////////////////////////////////

  /** A list of localized strings corresponding to string descriptions of the undo/redo actions. */
  public static final String[] descTypeUndo = {
    "",
    BaseMessages.getString(PKG, "PipelineMeta.UndoTypeDesc.UndoChange"),
    BaseMessages.getString(PKG, "PipelineMeta.UndoTypeDesc.UndoNew"),
    BaseMessages.getString(PKG, "PipelineMeta.UndoTypeDesc.UndoDelete"),
    BaseMessages.getString(PKG, "PipelineMeta.UndoTypeDesc.UndoPosition")
  };

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
   * Compares two pipeline on name and filename. The comparison algorithm is as follows:<br>
   *
   * <ol>
   *   <li>The first pipeline's filename is checked first; if it has none, the pipeline is generated
   *       If the second pipeline is not generated, -1 is returned.
   *   <li>If the pipelines are both generated, the pipelines' names are compared. If the first
   *       pipeline has no name and the second one does, a -1 is returned. If the opposite is true,
   *       1 is returned.
   *   <li>If they both have names they are compared as strings. If the result is non-zero it is
   *       returned. Otherwise the repository directories are compared using the same technique of
   *       checking empty values and then performing a string comparison, returning any non-zero
   *       result.
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
   * calls compare(this, o)
   *
   * @param o the o
   * @return the int
   * @see #compare(PipelineMeta, PipelineMeta)
   * @see Comparable#compareTo(Object)
   */
  @Override
  public int compareTo(PipelineMeta o) {
    return compare(this, o);
  }

  /**
   * Checks whether this pipeline's meta-data object is equal to the specified object. If the
   * specified object is not an instance of PipelineMeta, false is returned. Otherwise the method
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

  /**
   * Clones the pipeline meta-data object.
   *
   * @return a clone of the pipeline meta-data object
   * @see Object#clone()
   */
  @Override
  public Object clone() {
    return realClone(true);
  }

  /**
   * Perform a real clone of the pipeline meta-data object, including cloning all lists and copying
   * all values. If the doClear parameter is true, the clone will be cleared of ALL values before
   * the copy. If false, only the copied fields will be cleared.
   *
   * @param doClear Whether to clear all of the clone's data before copying from the source object
   * @return a real clone of the calling object
   */
  public Object realClone(boolean doClear) {

    try {
      PipelineMeta pipelineMeta = (PipelineMeta) super.clone();
      if (doClear) {
        pipelineMeta.clear();
      } else {
        // Clear out the things we're replacing below
        pipelineMeta.transforms = new ArrayList<>();
        pipelineMeta.hops = new ArrayList<>();
        pipelineMeta.notes = new ArrayList<>();
        pipelineMeta.namedParams = new NamedParameters();
        pipelineMeta.transformChangeListeners = new ArrayList<>();
      }
      for (TransformMeta transform : transforms) {
        pipelineMeta.addTransform((TransformMeta) transform.clone());
      }
      // PDI-15799: Transform references are original yet. Set them to the clones.
      for (TransformMeta transform : pipelineMeta.getTransforms()) {
        final ITransformMeta transformMetaInterface = transform.getTransform();
        if (transformMetaInterface != null) {
          final ITransformIOMeta transformIOMeta = transformMetaInterface.getTransformIOMeta();
          if (transformIOMeta != null) {
            for (IStream stream : transformIOMeta.getInfoStreams()) {
              String streamTransformName = stream.getTransformName();
              if (streamTransformName != null) {
                TransformMeta streamTransformMeta = pipelineMeta.findTransform(streamTransformName);
                stream.setTransformMeta(streamTransformMeta);
              }
            }
          }
        }
      }
      for (PipelineHopMeta hop : hops) {
        pipelineMeta.addPipelineHop((PipelineHopMeta) hop.clone());
      }
      for (NotePadMeta note : notes) {
        pipelineMeta.addNote(note.clone());
      }
      for (String key : listParameters()) {
        pipelineMeta.addParameterDefinition(
            key, getParameterDefault(key), getParameterDescription(key));
      }

      return pipelineMeta;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

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
    nameSynchronizedWithFilename = true;

    transforms = new ArrayList<>();
    hops = new ArrayList<>();
    namedParams = new NamedParameters();
    transformChangeListeners = new ArrayList<>();

    pipelineStatus = -1;
    pipelineVersion = null;

    undo = new ArrayList<>();
    maxUndo = Const.MAX_UNDO;
    undoPosition = -1;

    super.clear();

    // The performance monitoring options
    //
    capturingTransformPerformanceSnapShots = false;
    transformPerformanceCapturingDelay = 1000; // every 1 seconds
    transformPerformanceCapturingSizeLimit = "100"; // maximum 100 data points

    transformFieldsCache = new HashMap<>();
    loopCache = new HashMap<>();
    previousTransformCache = new HashMap<>();
    pipelineType = PipelineType.Normal;

    log = LogChannel.GENERAL;
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
    if (iface instanceof ITransformMetaChangeListener) {
      addTransformChangeListener((ITransformMetaChangeListener) iface);
    }
    changedTransforms = true;
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
      index = transforms.add(transformMeta) ? 0 : index;
    } else {
      TransformMeta previous = getTransform(index);
      previous.replaceMeta(transformMeta);
    }
    transformMeta.setParentPipelineMeta(this);
    ITransformMeta iface = transformMeta.getTransform();
    if (index != -1 && iface instanceof ITransformMetaChangeListener) {
      addTransformChangeListener(index, (ITransformMetaChangeListener) iface);
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
    clearCaches();
  }

  /**
   * Get a list of defined transforms in this pipeline.
   *
   * @return an ArrayList of defined transforms.
   */
  public List<TransformMeta> getTransforms() {
    return transforms;
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
    if (iface instanceof ITransformMetaChangeListener) {
      removeTransformChangeListener((ITransformMetaChangeListener) iface);
    }

    transforms.remove(i);

    if (removeTransform.getTransform() instanceof Missing) {
      removeMissingPipeline((Missing) removeTransform.getTransform());
    }

    changedTransforms = true;
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
   * @param hi The hop meta-data to set
   */
  public void setPipelineHop(int i, PipelineHopMeta hi) {
    hops.set(i, hi);
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
   * @return The hop information or null if nothing was found.
   */
  public PipelineHopMeta findPipelineHop(String name) {
    int i;

    for (i = 0; i < nrPipelineHops(); i++) {
      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.toString().equalsIgnoreCase(name)) {
        return hi;
      }
    }
    return null;
  }

  /**
   * Search all hops for a hop where a certain transform is at the start.
   *
   * @param fromTransform The transform at the start of the hop.
   * @return The hop or null if no hop was found.
   */
  public PipelineHopMeta findPipelineHopFrom(TransformMeta fromTransform) {
    int i;
    for (i = 0; i < nrPipelineHops(); i++) {
      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.getFromTransform() != null
          && hi.getFromTransform().equals(fromTransform)) { // return the first
        return hi;
      }
    }
    return null;
  }

  public List<PipelineHopMeta> findAllPipelineHopFrom(TransformMeta fromTransform) {
    return hops.stream()
        .filter(
            hop -> hop.getFromTransform() != null && hop.getFromTransform().equals(fromTransform))
        .collect(Collectors.toList());
  }

  /**
   * Find a certain hop in the pipeline.
   *
   * @param hi The hop information to look for.
   * @return The hop or null if no hop was found.
   */
  public PipelineHopMeta findPipelineHop(PipelineHopMeta hi) {
    return findPipelineHop(hi.getFromTransform(), hi.getToTransform());
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
   * @param disabledToo the disabled too
   * @return The hop or null if no hop was found.
   */
  public PipelineHopMeta findPipelineHop(
      TransformMeta from, TransformMeta to, boolean disabledToo) {
    for (int i = 0; i < nrPipelineHops(); i++) {
      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.isEnabled() || disabledToo) {
        if (hi.getFromTransform() != null
            && hi.getToTransform() != null
            && hi.getFromTransform().equals(from)
            && hi.getToTransform().equals(to)) {
          return hi;
        }
      }
    }
    return null;
  }

  /**
   * Search all hops for a hop where a certain transform is at the end.
   *
   * @param toTransform The transform at the end of the hop.
   * @return The hop or null if no hop was found.
   */
  public PipelineHopMeta findPipelineHopTo(TransformMeta toTransform) {
    int i;
    for (i = 0; i < nrPipelineHops(); i++) {
      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.getToTransform() != null
          && hi.getToTransform().equals(toTransform)) { // Return the first!
        return hi;
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
  public boolean isTransformMetarmative(TransformMeta thisTransform, TransformMeta prevTransform) {
    String[] infoTransforms =
        thisTransform.getTransform().getTransformIOMeta().getInfoTransformNames();
    if (infoTransforms == null) {
      return false;
    }
    for (int i = 0; i < infoTransforms.length; i++) {
      if (prevTransform.getName().equalsIgnoreCase(infoTransforms[i])) {
        return true;
      }
    }

    return false;
  }

  /**
   * Counts the number of previous transforms for a transform name.
   *
   * @param transformName The name of the transform to start from
   * @return The number of preceding transforms.
   * @deprecated
   */
  @Deprecated
  public int findNrPrevTransforms(String transformName) {
    return findNrPrevTransforms(findTransform(transformName), false);
  }

  /**
   * Counts the number of previous transforms for a transform name taking into account whether or
   * not they are informational.
   *
   * @param transformName The name of the transform to start from
   * @param info true if only the informational transforms are desired, false otherwise
   * @return The number of preceding transforms.
   * @deprecated
   */
  @Deprecated
  public int findNrPrevTransforms(String transformName, boolean info) {
    return findNrPrevTransforms(findTransform(transformName), info);
  }

  /**
   * Find the number of transforms that precede the indicated transform.
   *
   * @param transformMeta The source transform
   * @return The number of preceding transforms found.
   */
  public int findNrPrevTransforms(TransformMeta transformMeta) {
    return findNrPrevTransforms(transformMeta, false);
  }

  /**
   * Find the previous transform on a certain location (i.e. the specified index).
   *
   * @param transformName The source transform name
   * @param nr the index into the transform list
   * @return The preceding transform found.
   * @deprecated
   */
  @Deprecated
  public TransformMeta findPrevTransform(String transformName, int nr) {
    return findPrevTransform(findTransform(transformName), nr);
  }

  /**
   * Find the previous transform on a certain location taking into account the transforms being
   * informational or not.
   *
   * @param transformName The name of the transform
   * @param nr The index into the transform list
   * @param info true if only the informational transforms are desired, false otherwise
   * @return The transform information
   * @deprecated
   */
  @Deprecated
  public TransformMeta findPrevTransform(String transformName, int nr, boolean info) {
    return findPrevTransform(findTransform(transformName), nr, info);
  }

  /**
   * Find the previous transform on a certain location (i.e. the specified index).
   *
   * @param transformMeta The source transform information
   * @param nr the index into the hops list
   * @return The preceding transform found.
   */
  public TransformMeta findPrevTransform(TransformMeta transformMeta, int nr) {
    return findPrevTransform(transformMeta, nr, false);
  }

  /**
   * Count the number of previous transforms on a certain location taking into account the
   * transforms being informational or not.
   *
   * @param transformMeta The name of the transform
   * @param info true if only the informational transforms are desired, false otherwise
   * @return The number of preceding transforms
   * @deprecated please use method findPreviousTransforms
   */
  @Deprecated
  public int findNrPrevTransforms(TransformMeta transformMeta, boolean info) {
    int count = 0;
    int i;

    for (i = 0; i < nrPipelineHops(); i++) { // Look at all the hops;
      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.getToTransform() != null
          && hi.isEnabled()
          && hi.getToTransform().equals(transformMeta)) {
        // Check if this previous transform isn't informative (StreamValueLookup)
        // We don't want fields from this stream to show up!
        if (info || !isTransformMetarmative(transformMeta, hi.getFromTransform())) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * Find the previous transform on a certain location taking into account the transforms being
   * informational or not.
   *
   * @param transformMeta The transform
   * @param nr The index into the hops list
   * @param info true if we only want the informational transforms.
   * @return The preceding transform information
   * @deprecated please use method findPreviousTransforms
   */
  @Deprecated
  public TransformMeta findPrevTransform(TransformMeta transformMeta, int nr, boolean info) {
    int count = 0;
    int i;

    for (i = 0; i < nrPipelineHops(); i++) { // Look at all the hops;

      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.getToTransform() != null
          && hi.isEnabled()
          && hi.getToTransform().equals(transformMeta)) {
        if (info || !isTransformMetarmative(transformMeta, hi.getFromTransform())) {
          if (count == nr) {
            return hi.getFromTransform();
          }
          count++;
        }
      }
    }
    return null;
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
            && hi.getToTransform().equals(transformMeta)) {
          // Check if this previous transform isn't informative (StreamValueLookup)
          // We don't want fields from this stream to show up!
          if (info || !isTransformMetarmative(transformMeta, hi.getFromTransform())) {
            previousTransforms.add(hi.getFromTransform());
          }
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
      return null;
    }

    TransformMeta[] infoTransform = new TransformMeta[infoTransformName.length];
    for (int i = 0; i < infoTransform.length; i++) {
      infoTransform[i] = findTransform(infoTransformName[i]);
    }

    return infoTransform;
  }

  /**
   * Find the the number of informational transforms for a certain transform.
   *
   * @param transformMeta The transform
   * @return The number of informational transforms found.
   */
  public int findNrInfoTransforms(TransformMeta transformMeta) {
    if (transformMeta == null) {
      return 0;
    }

    int count = 0;

    for (int i = 0; i < nrPipelineHops(); i++) { // Look at all the hops;

      PipelineHopMeta hi = getPipelineHop(i);
      if (hi == null || hi.getToTransform() == null) {
        log.logError(BaseMessages.getString(PKG, "PipelineMeta.Log.DestinationOfHopCannotBeNull"));
      }
      if (hi != null
          && hi.getToTransform() != null
          && hi.isEnabled()
          && hi.getToTransform().equals(transformMeta)) {
        // Check if this previous transform isn't informative (StreamValueLookup)
        // We don't want fields from this stream to show up!
        if (isTransformMetarmative(transformMeta, hi.getFromTransform())) {
          count++;
        }
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
    for (int i = 0; i < nrPipelineHops(); i++) { // Look at all the hops;
      PipelineHopMeta hi = getPipelineHop(i);

      if (hi.isEnabled() && hi.getToTransform().equals(transformMeta)) {
        TransformMeta infoTransform = hi.getFromTransform();
        if (isTransformMetarmative(transformMeta, infoTransform)) {
          IRowMeta row = getPrevTransformFields(variables, infoTransform);
          return getThisTransformFields(variables, infoTransform, transformMeta, row);
        }
      }
    }
    return new RowMeta();
  }

  /**
   * Find the number of succeeding transforms for a certain originating transform.
   *
   * @param transformMeta The originating transform
   * @return The number of succeeding transforms.
   * @deprecated use {@link #getNextTransforms(TransformMeta)}
   */
  @Deprecated
  public int findNrNextTransforms(TransformMeta transformMeta) {
    int count = 0;
    int i;
    for (i = 0; i < nrPipelineHops(); i++) { // Look at all the hops;

      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.isEnabled() && hi.getFromTransform().equals(transformMeta)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Find the succeeding transform at a location for an originating transform.
   *
   * @param transformMeta The originating transform
   * @param nr The location
   * @return The transform found.
   * @deprecated use {@link #getNextTransforms(TransformMeta)}
   */
  @Deprecated
  public TransformMeta findNextTransform(TransformMeta transformMeta, int nr) {
    int count = 0;
    int i;

    for (i = 0; i < nrPipelineHops(); i++) { // Look at all the hops;

      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.isEnabled() && hi.getFromTransform().equals(transformMeta)) {
        if (count == nr) {
          return hi.getToTransform();
        }
        count++;
      }
    }
    return null;
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
      for (int i = 0; i < nrPipelineHops(); i++) { // Look at all the hops;
        PipelineHopMeta hopMeta = getPipelineHop(i);
        if (hopMeta.isEnabled() && hopMeta.getToTransform().equals(transformMeta)) {
          prevTransforms.add(hopMeta.getFromTransform());
        }
      }
    }

    return prevTransforms.toArray(new TransformMeta[prevTransforms.size()]);
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
   * Retrieve an array of succeeding transforms for a certain originating transform.
   *
   * @param transformMeta The originating transform
   * @return an array of succeeding transforms.
   * @deprecated use findNextTransforms instead
   */
  @Deprecated
  public TransformMeta[] getNextTransforms(TransformMeta transformMeta) {
    List<TransformMeta> nextTransforms = new ArrayList<>();
    for (int i = 0; i < nrPipelineHops(); i++) { // Look at all the hops;

      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.isEnabled() && hi.getFromTransform().equals(transformMeta)) {
        nextTransforms.add(hi.getToTransform());
      }
    }

    return nextTransforms.toArray(new TransformMeta[nextTransforms.size()]);
  }

  /**
   * Retrieve a list of succeeding transforms for a certain originating transform.
   *
   * @param transformMeta The originating transform
   * @return an array of succeeding transforms.
   */
  public List<TransformMeta> findNextTransforms(TransformMeta transformMeta) {
    List<TransformMeta> nextTransforms = new ArrayList<>();
    for (int i = 0; i < nrPipelineHops(); i++) { // Look at all the hops;

      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.isEnabled() && hi.getFromTransform().equals(transformMeta)) {
        nextTransforms.add(hi.getToTransform());
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
    TransformMeta[] nextTransformMeta = getNextTransforms(transformMeta);
    String[] retval = new String[nextTransformMeta.length];
    for (int x = 0; x < nextTransformMeta.length; x++) {
      retval[x] = nextTransformMeta[x].getName();
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
    int i, s;
    s = transforms.size();
    for (i = s - 1; i >= 0; i--) { // Back to front because drawing goes from start to end
      TransformMeta transformMeta = transforms.get(i);
      Point p = transformMeta.getLocation();
      if (p != null) {
        if (x >= p.x && x <= p.x + iconsize && y >= p.y && y <= p.y + iconsize + 20) {
          return transformMeta;
        }
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
    int i;
    for (i = 0; i < nrPipelineHops(); i++) {
      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.getFromTransform() == null || hi.getToTransform() == null) {
        return false;
      }
      if (hi.getFromTransform().equals(transformMeta)
          || hi.getToTransform().equals(transformMeta)) {
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

    for (int i = 0; i < transformMeta.length; i++) {
      IRowMeta flds = getTransformFields(variables, transformMeta[i]);
      if (flds != null) {
        fields.mergeRowMeta(flds, transformMeta[i].getName());
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
    setMetaStoreOnMappingTransforms();
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

    String fromToCacheEntry =
        transformMeta.getName()
            + (targetTransform != null ? ("-" + targetTransform.getName()) : "");
    IRowMeta rowMeta = transformFieldsCache.get(fromToCacheEntry);
    if (rowMeta != null) {
      return rowMeta;
    }

    // See if the transform is sending ERROR rows to the specified target transform.
    //
    if (targetTransform != null && transformMeta.isSendingErrorRowsToTransform(targetTransform)) {
      // The error rows are the same as the input rows for
      // the transform but with the selected error fields added
      //
      row = getPrevTransformFields(variables, transformMeta);

      // Add to this the error fields...
      TransformErrorMeta transformErrorMeta = transformMeta.getTransformErrorMeta();
      row.addRowMeta(transformErrorMeta.getErrorRowMeta(variables));

      // Store this row in the cache
      //
      transformFieldsCache.put(fromToCacheEntry, row);

      return row;
    }

    // Resume the regular program...

    List<TransformMeta> prevTransforms = findPreviousTransforms(transformMeta, false);

    int nrPrevious = prevTransforms.size();

    if (log.isDebug()) {
      log.logDebug(
          BaseMessages.getString(
              PKG,
              "PipelineMeta.Log.FromTransformALookingAtPreviousTransform",
              transformMeta.getName(),
              String.valueOf(nrPrevious)));
    }
    for (int i = 0; i < prevTransforms.size(); i++) {
      TransformMeta prevTransformMeta = prevTransforms.get(i);

      if (monitor != null) {
        monitor.subTask(
            BaseMessages.getString(
                PKG,
                "PipelineMeta.Monitor.CheckingTransformTask.Title",
                prevTransformMeta.getName()));
      }

      IRowMeta add = getTransformFields(variables, prevTransformMeta, transformMeta, monitor);
      if (add == null) {
        add = new RowMeta();
      }
      if (log.isDebug()) {
        log.logDebug(
            BaseMessages.getString(PKG, "PipelineMeta.Log.FoundFieldsToAdd") + add.toString());
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

    // Finally, see if we need to add/modify/delete fields with this transform "name"
    rowMeta = getThisTransformFields(variables, transformMeta, targetTransform, row, monitor);

    // Store this row in the cache
    //
    transformFieldsCache.put(fromToCacheEntry, rowMeta);

    return rowMeta;
  }

  /**
   * Find the fields that are entering a transform with a certain name.
   *
   * @param transformName The name of the transform queried
   * @return A row containing the fields (w/ origin) entering the transform
   * @throws HopTransformException the hop transform exception
   */
  public IRowMeta getPrevTransformFields(IVariables variables, String transformName) throws HopTransformException {
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
    int nrPrevTransforms = prevTransforms.size();
    if (log.isDebug()) {
      log.logDebug(
          BaseMessages.getString(
              PKG,
              "PipelineMeta.Log.FromTransformALookingAtPreviousTransform",
              transformMeta.getName(),
              String.valueOf(nrPrevTransforms)));
    }
    TransformMeta prevTransformMeta;
    for (int i = 0; i < nrPrevTransforms; i++) {
      prevTransformMeta = prevTransforms.get(i);
      if (transformName != null && !transformName.equalsIgnoreCase(prevTransformMeta.getName())) {
        continue;
      }

      if (monitor != null) {
        monitor.subTask(
            BaseMessages.getString(
                PKG,
                "PipelineMeta.Monitor.CheckingTransformTask.Title",
                prevTransformMeta.getName()));
      }

      IRowMeta add = getTransformFields(variables, prevTransformMeta, transformMeta, monitor);

      if (log.isDebug()) {
        log.logDebug(
            BaseMessages.getString(PKG, "PipelineMeta.Log.FoundFieldsToAdd2") + add.toString());
      }
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
    if (log.isDebug()) {
      log.logDebug(
          BaseMessages.getString(
              PKG,
              "PipelineMeta.Log.GettingFieldsFromTransform",
              transformMeta.getName(),
              transformMeta.getTransformPluginId()));
    }
    String name = transformMeta.getName();

    if (monitor != null) {
      monitor.subTask(
          BaseMessages.getString(
              PKG, "PipelineMeta.Monitor.GettingFieldsFromTransformTask.Title", name));
    }

    ITransformMeta iTransformMeta = transformMeta.getTransform();
    IRowMeta[] infoRowMeta;
    TransformMeta[] lu = getInfoTransform(transformMeta);
    if (Utils.isEmpty(lu)) {
      infoRowMeta =
          new IRowMeta[] {
            iTransformMeta.getTableFields(variables),
          };
    } else {
      infoRowMeta = new IRowMeta[lu.length];
      for (int i = 0; i < lu.length; i++) {
        infoRowMeta[i] = getTransformFields(variables, lu[i]);
      }
    }

    setMetaStoreOnMappingTransforms();

    // Go get the fields...
    //
    IRowMeta before = row.clone();
    IRowMeta[] clonedInfo = cloneRowMetaInterfaces(infoRowMeta);
    if (!isSomethingDifferentInRow(before, row)) {
      iTransformMeta.getFields(before, name, clonedInfo, nextTransform, variables, metadataProvider);
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
   * Set the MetaStore on the Mapping transform. That way the mapping transform can determine the
   * output fields for metadata referencing mappings... This is the exception to the rule so we
   * don't pass this through the getFields() method. TODO: figure out a way to make this more
   * generic.
   */
  private void setMetaStoreOnMappingTransforms() {

    for (TransformMeta transform : transforms) {
      if (transform.getTransform() instanceof WorkflowExecutorMeta) {
        ((WorkflowExecutorMeta) transform.getTransform()).setMetadataProvider(metadataProvider);
      }
      if (transform.getTransform() instanceof PipelineExecutorMeta) {
        ((PipelineExecutorMeta) transform.getTransform()).setMetadataProvider(metadataProvider);
      }
    }
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
   * @return the XML representation of this pipeline
   * @throws HopException if any errors occur during generation of the XML
   * @see IXml#getXml()
   */
  @Override
  public String getXml() throws HopException {
    return getXml(true, true, true, true);
  }

  /**
   * Gets the XML representation of this pipeline, including or excluding transform, database, hop
   * server, cluster, or partition information as specified by the parameters
   *
   * @param includeTransforms whether to include transform data
   * @param includeNamedParameters whether to include named parameters data
   * @param includeNotePads whether to include notepads data
   * @param includeAttributeGroups whether to include attributes map data
   * @return the XML representation of this pipeline
   * @throws HopException if any errors occur during generation of the XML
   */
  public String getXml(
      boolean includeTransforms,
      boolean includeNamedParameters,
      boolean includeNotePads,
      boolean includeAttributeGroups)
      throws HopException {

    StringBuilder xml = new StringBuilder(800);

    xml.append(XmlHandler.openTag(XML_TAG)).append(Const.CR);

    xml.append("  ").append(XmlHandler.openTag(XML_TAG_INFO)).append(Const.CR);

    xml.append("    ")
        .append(
            XmlHandler.addTagValue("name", getName())); // lossy if name is sync'ed with filename
    xml.append("    ")
        .append(XmlHandler.addTagValue("name_sync_with_filename", nameSynchronizedWithFilename));
    xml.append("    ").append(XmlHandler.addTagValue("description", description));
    xml.append("    ").append(XmlHandler.addTagValue("extended_description", extendedDescription));
    xml.append("    ").append(XmlHandler.addTagValue("pipeline_version", pipelineVersion));
    xml.append("    ").append(XmlHandler.addTagValue("pipeline_type", pipelineType.getCode()));

    if (pipelineStatus >= 0) {
      xml.append("    ").append(XmlHandler.addTagValue("pipeline_status", pipelineStatus));
    }

    if (includeNamedParameters) {
      xml.append("    ").append(XmlHandler.openTag(XML_TAG_PARAMETERS)).append(Const.CR);
      String[] parameters = listParameters();
      for (int idx = 0; idx < parameters.length; idx++) {
        xml.append("      ").append(XmlHandler.openTag("parameter")).append(Const.CR);
        xml.append("        ").append(XmlHandler.addTagValue("name", parameters[idx]));
        xml.append("        ")
            .append(XmlHandler.addTagValue("default_value", getParameterDefault(parameters[idx])));
        xml.append("        ")
            .append(
                XmlHandler.addTagValue("description", getParameterDescription(parameters[idx])));
        xml.append("      ").append(XmlHandler.closeTag("parameter")).append(Const.CR);
      }
      xml.append("    ").append(XmlHandler.closeTag(XML_TAG_PARAMETERS)).append(Const.CR);
    }

    // Performance monitoring
    //
    xml.append("    ")
        .append(
            XmlHandler.addTagValue(
                "capture_transform_performance", capturingTransformPerformanceSnapShots));
    xml.append("    ")
        .append(
            XmlHandler.addTagValue(
                "transform_performance_capturing_delay", transformPerformanceCapturingDelay));
    xml.append("    ")
        .append(
            XmlHandler.addTagValue(
                "transform_performance_capturing_size_limit",
                transformPerformanceCapturingSizeLimit));

    xml.append("    ").append(XmlHandler.addTagValue("created_user", createdUser));
    xml.append("    ")
        .append(XmlHandler.addTagValue("created_date", XmlHandler.date2string(createdDate)));
    xml.append("    ").append(XmlHandler.addTagValue("modified_user", modifiedUser));
    xml.append("    ")
        .append(XmlHandler.addTagValue("modified_date", XmlHandler.date2string(modifiedDate)));

    try {
      xml.append("    ").append(XmlHandler.addTagValue("key_for_session_key", keyForSessionKey));
    } catch (Exception ex) {
      log.logError("Unable to decode key", ex);
    }
    xml.append("    ").append(XmlHandler.addTagValue("is_key_private", isKeyPrivate));

    xml.append("  ").append(XmlHandler.closeTag(XML_TAG_INFO)).append(Const.CR);

    if (includeNotePads) {
      xml.append("  ").append(XmlHandler.openTag(XML_TAG_NOTEPADS)).append(Const.CR);
      if (notes != null) {
        for (int i = 0; i < nrNotes(); i++) {
          NotePadMeta ni = getNote(i);
          xml.append(ni.getXml());
        }
      }
      xml.append("  ").append(XmlHandler.closeTag(XML_TAG_NOTEPADS)).append(Const.CR);
    }

    if (includeTransforms) {
      xml.append("  ").append(XmlHandler.openTag(XML_TAG_ORDER)).append(Const.CR);
      for (int i = 0; i < nrPipelineHops(); i++) {
        PipelineHopMeta pipelineHopMeta = getPipelineHop(i);
        xml.append(pipelineHopMeta.getXml());
      }
      xml.append("  ").append(XmlHandler.closeTag(XML_TAG_ORDER)).append(Const.CR);

      /* The transforms... */
      for (int i = 0; i < nrTransforms(); i++) {
        TransformMeta transformMeta = getTransform(i);
        xml.append(transformMeta.getXml());
      }

      /* The error handling metadata on the transforms */
      xml.append("  ")
          .append(XmlHandler.openTag(XML_TAG_TRANSFORM_ERROR_HANDLING))
          .append(Const.CR);
      for (int i = 0; i < nrTransforms(); i++) {
        TransformMeta transformMeta = getTransform(i);

        if (transformMeta.getTransformErrorMeta() != null) {
          xml.append(transformMeta.getTransformErrorMeta().getXml());
        }
      }
      xml.append("  ")
          .append(XmlHandler.closeTag(XML_TAG_TRANSFORM_ERROR_HANDLING))
          .append(Const.CR);
    }

    // Also store the attribute groups
    //
    if (includeAttributeGroups) {
      xml.append(AttributesUtil.getAttributesXml(attributesMap));
    }
    xml.append(XmlHandler.closeTag(XML_TAG)).append(Const.CR);

    return XmlFormatter.format(xml.toString());
  }

  /**
   * Parses a file containing the XML that describes the pipeline.
   *
   * @param fname The filename
   * @param metadataProvider the metadata store to reference (or null if there is none)
   * @param setInternalVariables true if you want to set the internal variables based on this
   *     pipeline information
   * @param parentVariableSpace the parent variable variables to use during PipelineMeta construction
   * @throws HopXmlException if any errors occur during parsing of the specified file
   * @throws HopMissingPluginsException in case missing plugins were found (details are in the
   *     exception in that case)
   */
  public PipelineMeta(
      String fname,
      IHopMetadataProvider metadataProvider,
      boolean setInternalVariables,
      IVariables parentVariableSpace)
      throws HopXmlException, HopMissingPluginsException {
    // if fname is not provided, there's not much we can do, throw an exception
    if (StringUtils.isBlank(fname)) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "PipelineMeta.Exception.MissingXMLFilePath"));
    }

    if (metadataProvider == null) {
      throw new HopXmlException(
          "API error: metadata provider can't be null. When loading a pipeline Hop needs to be able to reference external metadata objects");
    }

    this.metadataProvider = metadataProvider;

    // OK, try to load using the VFS stuff...
    Document doc = null;
    try {
      final FileObject pipelineFile = HopVfs.getFileObject(fname);
      if (!pipelineFile.exists()) {
        throw new HopXmlException(
            BaseMessages.getString(PKG, "PipelineMeta.Exception.InvalidXMLPath", fname));
      }
      doc = XmlHandler.loadXmlFile(pipelineFile);
    } catch (HopXmlException ke) {
      // if we have a HopXmlException, simply re-throw it
      throw ke;
    } catch (HopException | FileSystemException e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "PipelineMeta.Exception.ErrorOpeningOrValidatingTheXMLFile", fname),
          e);
    }

    if (doc != null) {
      // Root node:
      Node pipelineNode = XmlHandler.getSubNode(doc, XML_TAG);

      if (pipelineNode == null) {
        throw new HopXmlException(
            BaseMessages.getString(PKG, "PipelineMeta.Exception.NotValidPipelineXML", fname));
      }

      // Load from this node...
      loadXml(pipelineNode, fname, metadataProvider, setInternalVariables, parentVariableSpace);

    } else {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "PipelineMeta.Exception.ErrorOpeningOrValidatingTheXMLFile", fname));
    }
  }

  /**
   * Instantiates a new pipeline meta-data object.
   *
   * @param xmlStream the XML input stream from which to read the pipeline definition
   * @param setInternalVariables whether to set internal variables as a result of the creation
   * @param parentVariableSpace the parent variable variables
   * @throws HopXmlException if any errors occur during parsing of the specified stream
   * @throws HopMissingPluginsException in case missing plugins were found (details are in the
   *     exception in that case)
   */
  public PipelineMeta(
      InputStream xmlStream,
      IHopMetadataProvider metadataProvider,
      boolean setInternalVariables,
      IVariables parentVariableSpace)
      throws HopXmlException, HopMissingPluginsException {
    Document doc = XmlHandler.loadXmlFile(xmlStream, null, false, false);
    Node pipelineNode = XmlHandler.getSubNode(doc, XML_TAG);
    loadXml(pipelineNode, null, metadataProvider, setInternalVariables, parentVariableSpace);
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
    loadXml(pipelineNode, null, metadataProvider, false, null);
  }

  /**
   * Parses an XML DOM (starting at the specified Node) that describes the pipeline.
   *
   * @param pipelineNode The XML node to load from
   * @param filename The filename
   * @param setInternalVariables true if you want to set the internal variables based on this
   *     pipeline information
   * @param variables the parent variable variables to use during PipelineMeta construction
   * @throws HopXmlException if any errors occur during parsing of the specified file
   * @throws HopMissingPluginsException in case missing plugins were found (details are in the
   *     exception in that case)
   */
  public void loadXml(
      Node pipelineNode,
      String filename,
      IHopMetadataProvider metadataProvider,
      boolean setInternalVariables,
      IVariables variables)
      throws HopXmlException, HopMissingPluginsException {

    HopMissingPluginsException missingPluginsException =
        new HopMissingPluginsException(
            BaseMessages.getString(
                PKG, "PipelineMeta.MissingPluginsFoundWhileLoadingPipeline.Exception"));

    this.metadataProvider = metadataProvider; // Remember this as the primary meta store.

    try {
      try {
        // Clear the pipeline
        clear();

        // Set the filename here so it can be used in variables for ALL aspects of the pipeline FIX:
        // PDI-8890
        //
        setFilename(filename);

        // Read the notes...
        Node notepadsNode = XmlHandler.getSubNode(pipelineNode, XML_TAG_NOTEPADS);
        List<Node> notepadNodes = XmlHandler.getNodes(notepadsNode, NotePadMeta.XML_TAG);
        for (Node notepadNode : notepadNodes) {
          NotePadMeta ni = new NotePadMeta(notepadNode);
          notes.add(ni);
        }

        // Handle Transforms
        //
        List<Node> transformNodes = XmlHandler.getNodes(pipelineNode, TransformMeta.XML_TAG);

        if (log.isDebug()) {
          log.logDebug(
              BaseMessages.getString(PKG, "PipelineMeta.Log.ReadingTransforms")
                  + transformNodes.size()
                  + " transforms...");
        }
        for (Node transformNode : transformNodes) {
          TransformMeta transformMeta = new TransformMeta(transformNode, metadataProvider);
          transformMeta.setParentPipelineMeta(this); // for tracing, retain hierarchy

          if (transformMeta.isMissing()) {
            addMissingPipeline((Missing) transformMeta.getTransform());
          }
          addOrReplaceTransform(transformMeta);
        }

        // Read the error handling code of the transforms...
        //
        Node errorHandlingNode =
            XmlHandler.getSubNode(pipelineNode, XML_TAG_TRANSFORM_ERROR_HANDLING);
        int nrErrorHandlers =
            XmlHandler.countNodes(errorHandlingNode, TransformErrorMeta.XML_ERROR_TAG);
        for (int i = 0; i < nrErrorHandlers; i++) {
          Node transformErrorMetaNode =
              XmlHandler.getSubNodeByNr(errorHandlingNode, TransformErrorMeta.XML_ERROR_TAG, i);
          TransformErrorMeta transformErrorMeta =
              new TransformErrorMeta(transformErrorMetaNode, transforms);
          if (transformErrorMeta.getSourceTransform() != null) {
            transformErrorMeta
                .getSourceTransform()
                .setTransformErrorMeta(transformErrorMeta); // a bit of a trick, I know.
          }
        }

        // Have all StreamValueLookups, etc. reference the correct source transforms...
        //
        for (int i = 0; i < nrTransforms(); i++) {
          TransformMeta transformMeta = getTransform(i);
          ITransformMeta sii = transformMeta.getTransform();
          if (sii != null) {
            sii.searchInfoAndTargetTransforms(transforms);
          }
        }

        // Handle Hops
        //
        Node orderNode = XmlHandler.getSubNode(pipelineNode, XML_TAG_ORDER);
        List<Node> hopNodes = XmlHandler.getNodes(orderNode, PipelineHopMeta.XML_HOP_TAG);

        if (log.isDebug()) {
          log.logDebug(
              BaseMessages.getString(PKG, "PipelineMeta.Log.WeHaveHops")
                  + hopNodes.size()
                  + " hops...");
        }

        for (Node hopNode : hopNodes) {
          PipelineHopMeta hopinf = new PipelineHopMeta(hopNode, transforms);
          hopinf.setErrorHop(isErrorNode(errorHandlingNode, hopNode));
          addPipelineHop(hopinf);
        }

        //
        // get pipeline info:
        //
        Node infoNode = XmlHandler.getSubNode(pipelineNode, XML_TAG_INFO);

        // Name
        //
        this.name = XmlHandler.getTagValue(infoNode, "name");

        nameSynchronizedWithFilename =
            "Y".equalsIgnoreCase(XmlHandler.getTagValue(infoNode, "name_sync_with_filename"));

        // description
        //
        description = XmlHandler.getTagValue(infoNode, "description");

        // extended description
        //
        extendedDescription = XmlHandler.getTagValue(infoNode, "extended_description");

        // pipeline version
        //
        pipelineVersion = XmlHandler.getTagValue(infoNode, "pipeline_version");

        // pipeline status
        //
        pipelineStatus = Const.toInt(XmlHandler.getTagValue(infoNode, "pipeline_status"), -1);

        String pipelineTypeCode = XmlHandler.getTagValue(infoNode, "pipeline_type");
        pipelineType = PipelineType.getPipelineTypeByCode(pipelineTypeCode);

        // Read the named parameters.
        Node paramsNode = XmlHandler.getSubNode(infoNode, XML_TAG_PARAMETERS);
        int nrParams = XmlHandler.countNodes(paramsNode, "parameter");

        for (int i = 0; i < nrParams; i++) {
          Node paramNode = XmlHandler.getSubNodeByNr(paramsNode, "parameter", i);

          String paramName = XmlHandler.getTagValue(paramNode, "name");
          String defaultValue = XmlHandler.getTagValue(paramNode, "default_value");
          String descr = XmlHandler.getTagValue(paramNode, "description");

          addParameterDefinition(paramName, defaultValue, descr);
        }

        // Performance monitoring for transforms...
        //
        capturingTransformPerformanceSnapShots =
            "Y".equalsIgnoreCase(XmlHandler.getTagValue(infoNode, "capture_transform_performance"));
        transformPerformanceCapturingDelay =
            Const.toLong(
                XmlHandler.getTagValue(infoNode, "transform_performance_capturing_delay"), 1000);
        transformPerformanceCapturingSizeLimit =
            XmlHandler.getTagValue(infoNode, "transform_performance_capturing_size_limit");

        // Created user/date
        createdUser = XmlHandler.getTagValue(infoNode, "created_user");
        String createDate = XmlHandler.getTagValue(infoNode, "created_date");
        if (createDate != null) {
          createdDate = XmlHandler.stringToDate(createDate);
        }

        // Changed user/date
        modifiedUser = XmlHandler.getTagValue(infoNode, "modified_user");
        String modDate = XmlHandler.getTagValue(infoNode, "modified_date");
        if (modDate != null) {
          modifiedDate = XmlHandler.stringToDate(modDate);
        }

        if (log.isDebug()) {
          log.logDebug(
              BaseMessages.getString(PKG, "PipelineMeta.Log.NumberOfTransformReaded")
                  + nrTransforms());
          log.logDebug(
              BaseMessages.getString(PKG, "PipelineMeta.Log.NumberOfHopsReaded")
                  + nrPipelineHops());
        }
        sortTransforms();

        // Load the attribute groups map
        //
        attributesMap =
            AttributesUtil.loadAttributes(
                XmlHandler.getSubNode(pipelineNode, AttributesUtil.XML_TAG));

        keyForSessionKey =
            XmlHandler.stringToBinary(XmlHandler.getTagValue(infoNode, "key_for_session_key"));
        isKeyPrivate = "Y".equals(XmlHandler.getTagValue(infoNode, "is_key_private"));

      } catch (HopXmlException xe) {
        throw new HopXmlException(
            BaseMessages.getString(PKG, "PipelineMeta.Exception.ErrorReadingPipeline"), xe);
      } catch (HopException e) {
        throw new HopXmlException(e);
      } finally {
        ExtensionPointHandler.callExtensionPoint(
            log, variables, HopExtensionPoint.PipelineMetaLoaded.id, this );
      }
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

  public byte[] getKey() {
    return keyForSessionKey;
  }

  public void setKey(byte[] key) {
    this.keyForSessionKey = key;
  }

  public boolean isPrivateKey() {
    return isKeyPrivate;
  }

  public void setPrivateKey(boolean privateKey) {
    this.isKeyPrivate = privateKey;
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

    for (int i = 0; i < nrTransforms(); i++) {
      getTransform(i).setChanged(false);
      if (getTransform(i).getTransformPartitioningMeta() != null) {
        getTransform(i).getTransformPartitioningMeta().hasChanged(false);
      }
    }
    for (int i = 0; i < nrPipelineHops(); i++) {
      getPipelineHop(i).setChanged(false);
    }

    super.clearChanged();
  }

  /**
   * Checks whether or not the transforms have changed.
   *
   * @return true if the transforms have been changed, false otherwise
   */
  public boolean haveTransformsChanged() {
    if (changedTransforms) {
      return true;
    }

    for (int i = 0; i < nrTransforms(); i++) {
      TransformMeta transformMeta = getTransform(i);
      if (transformMeta.hasChanged()) {
        return true;
      }
      if (transformMeta.getTransformPartitioningMeta() != null
          && transformMeta.getTransformPartitioningMeta().hasChanged()) {
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

    for (int i = 0; i < nrPipelineHops(); i++) {
      PipelineHopMeta hi = getPipelineHop(i);
      if (hi.hasChanged()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether or not the pipeline has changed.
   *
   * @return true if the pipeline has changed, false otherwise
   */
  @Override
  public boolean hasChanged() {
    return super.hasChanged() || haveTransformsChanged() || haveHopsChanged();
  }

  private boolean isErrorNode(Node errorHandingNode, Node checkNode) {
    if (errorHandingNode != null) {
      NodeList errors = errorHandingNode.getChildNodes();

      Node nodeHopFrom = XmlHandler.getSubNode(checkNode, PipelineHopMeta.XML_FROM_TAG);
      Node nodeHopTo = XmlHandler.getSubNode(checkNode, PipelineHopMeta.XML_TO_TAG);

      int i = 0;
      while (i < errors.getLength()) {

        Node errorNode = errors.item(i);

        if (!TransformErrorMeta.XML_ERROR_TAG.equals(errorNode.getNodeName())) {
          i++;
          continue;
        }

        Node errorSourceNode =
            XmlHandler.getSubNode(errorNode, TransformErrorMeta.XML_SOURCE_TRANSFORM_TAG);
        Node errorTagetNode =
            XmlHandler.getSubNode(errorNode, TransformErrorMeta.XML_TARGET_TRANSFORM_TAG);

        String sourceContent = errorSourceNode.getTextContent().trim();
        String tagetContent = errorTagetNode.getTextContent().trim();

        if (sourceContent.equals(nodeHopFrom.getTextContent().trim())
            && tagetContent.equals(nodeHopTo.getTextContent().trim())) {
          return true;
        }
        i++;
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

  /** @deprecated use {@link #hasLoop(TransformMeta, TransformMeta)}} */
  @Deprecated
  public boolean hasLoop(TransformMeta transformMeta, TransformMeta lookup, boolean info) {
    return hasLoop(transformMeta, lookup, new HashSet<>());
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
    int nr = prevTransforms.size();
    for (int i = 0; i < nr; i++) {
      TransformMeta prevTransformMeta = prevTransforms.get(i);
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
    int i;
    for (i = 0; i < nrTransforms(); i++) {
      TransformMeta transformMeta = getTransform(i);
      transformMeta.setSelected(true);
    }
    for (i = 0; i < nrNotes(); i++) {
      NotePadMeta ni = getNote(i);
      ni.setSelected(true);
    }

    setChanged();
    notifyObservers("refreshGraph");
  }

  /** Clear the selection of all transforms. */
  public void unselectAll() {
    int i;
    for (i = 0; i < nrTransforms(); i++) {
      TransformMeta transformMeta = getTransform(i);
      transformMeta.setSelected(false);
    }
    for (i = 0; i < nrNotes(); i++) {
      NotePadMeta ni = getNote(i);
      ni.setSelected(false);
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

    return points.toArray(new Point[points.size()]);
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

    return points.toArray(new Point[points.size()]);
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
    int maxx = 0, maxy = 0;
    for (int i = 0; i < nrTransforms(); i++) {
      TransformMeta transformMeta = getTransform(i);
      Point loc = transformMeta.getLocation();
      if (loc.x > maxx) {
        maxx = loc.x;
      }
      if (loc.y > maxy) {
        maxy = loc.y;
      }
    }
    for (int i = 0; i < nrNotes(); i++) {
      NotePadMeta notePadMeta = getNote(i);
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
    int minx = Integer.MAX_VALUE, miny = Integer.MAX_VALUE;
    for (int i = 0; i < nrTransforms(); i++) {
      TransformMeta transformMeta = getTransform(i);
      Point loc = transformMeta.getLocation();
      if (loc.x < minx) {
        minx = loc.x;
      }
      if (loc.y < miny) {
        miny = loc.y;
      }
    }
    for (int i = 0; i < nrNotes(); i++) {
      NotePadMeta notePadMeta = getNote(i);
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
    String[] retval = new String[nrTransforms()];

    for (int i = 0; i < nrTransforms(); i++) {
      retval[i] = getTransform(i).getName();
    }

    return retval;
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
    for (int i = 0; i < previousTransforms.size(); i++) {
      TransformMeta transformMeta = previousTransforms.get(i);
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
    for (int i = 0; i < infoTransforms.size(); i++) {
      TransformMeta transformMeta = infoTransforms.get(i);
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

  /** Puts the transforms in alphabetical order. */
  public void sortTransforms() {
    try {
      Collections.sort(transforms);
    } catch (Exception e) {
      log.logError(
          BaseMessages.getString(PKG, "PipelineMeta.Exception.ErrorOfSortingTransforms") + e);
      log.logError(Const.getStackTracker(e));
    }
  }

  /** Sorts all the hops in the pipeline. */
  public void sortHops() {
    Collections.sort(hops);
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
      List<TransformMeta> prevTransforms = previousCache.get(transformMeta);
      if (prevTransforms == null) {
        prevTransforms = findPreviousTransforms(transformMeta);
        prevCount++;
        previousCache.put(transformMeta, prevTransforms);
      }

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

    Collections.sort(
        transforms,
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
    log.logBasic(
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
    List<TransformMeta> prevTransforms = previousCache.get(previousTransformMeta);
    if (prevTransforms == null) {
      prevTransforms = findPreviousTransforms(previousTransformMeta);
      prevCount++;
      previousCache.put(previousTransformMeta, prevTransforms);
    }

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
  public void analyseImpact(IVariables variables, List<DatabaseImpact> impact, IProgressMonitor monitor)
      throws HopTransformException {
    if (monitor != null) {
      monitor.beginTask(
          BaseMessages.getString(PKG, "PipelineMeta.Monitor.DeterminingImpactTask.Title"),
          nrTransforms());
    }
    boolean stop = false;
    for (int i = 0; i < nrTransforms() && !stop; i++) {
      if (monitor != null) {
        monitor.subTask(
            BaseMessages.getString(PKG, "PipelineMeta.Monitor.LookingAtTransformTask.Title")
                + (i + 1)
                + "/"
                + nrTransforms());
      }
      TransformMeta transformMeta = getTransform(i);

      IRowMeta prev = getPrevTransformFields(variables, transformMeta);
      ITransformMeta iTransformMeta = transformMeta.getTransform();
      IRowMeta infoRowMeta;
      TransformMeta[] lu = getInfoTransform(transformMeta);
      if (lu != null) {
        infoRowMeta = getTransformFields(variables, lu);
      } else {
        infoRowMeta = iTransformMeta.getTableFields(variables);
      }

      iTransformMeta.analyseImpact( variables,
          impact, this, transformMeta, prev, null, null, infoRowMeta, metadataProvider);

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
   * Builds a list of all the SQL statements that this pipeline needs in order to work properly.
   *
   * @param variables the variables to resolve variable expressions with
   * @return An ArrayList of SqlStatement objects.
   * @throws HopTransformException if any errors occur during SQL statement generation
   */
  public List<SqlStatement> getSqlStatements(IVariables variables) throws HopTransformException {
    return getSqlStatements(variables, null);
  }

  /**
   * Builds a list of all the SQL statements that this pipeline needs in order to work properly.
   *
   * @param variables the variables to resolve variable expressions with
   * @param monitor a progress monitor listener to be updated as the SQL statements are generated
   * @return An ArrayList of SqlStatement objects.
   * @throws HopTransformException if any errors occur during SQL statement generation
   */
  public List<SqlStatement> getSqlStatements(IVariables variables, IProgressMonitor monitor)
      throws HopTransformException {
    if (monitor != null) {
      monitor.beginTask(
          BaseMessages.getString(PKG, "PipelineMeta.Monitor.GettingTheSQLForPipelineTask.Title"),
          nrTransforms() + 1);
    }
    List<SqlStatement> stats = new ArrayList<>();

    for (int i = 0; i < nrTransforms(); i++) {
      TransformMeta transformMeta = getTransform(i);
      if (monitor != null) {
        monitor.subTask(
            BaseMessages.getString(
                PKG,
                "PipelineMeta.Monitor.GettingTheSQLForTransformTask.Title",
                "" + transformMeta));
      }
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
    String sql = "";
    List<SqlStatement> stats = getSqlStatements(variables);
    for (int i = 0; i < stats.size(); i++) {
      SqlStatement stat = stats.get(i);
      if (!stat.hasError() && stat.hasSql()) {
        sql += stat.getSql();
      }
    }

    return sql;
  }

  /**
   * Checks all the transforms and fills a List of (CheckResult) remarks.
   *
   * @param remarks The remarks list to add to.
   * @param only_selected true to check only the selected transforms, false for all transforms
   * @param monitor a progress monitor listener to be updated as the SQL statements are generated
   */
  public void checkTransforms(
      List<ICheckResult> remarks,
      boolean only_selected,
      IProgressMonitor monitor,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    try {
      remarks.clear(); // Start with a clean slate...

      Map<IValueMeta, String> values = new Hashtable<>();
      String[] transformnames;
      TransformMeta[] transforms;
      List<TransformMeta> selectedTransforms = getSelectedTransforms();
      if (!only_selected || selectedTransforms.isEmpty()) {
        transformnames = getTransformNames();
        transforms = getTransformsArray();
      } else {
        transformnames = getSelectedTransformNames();
        transforms = selectedTransforms.toArray(new TransformMeta[selectedTransforms.size()]);
      }

      ExtensionPointHandler.callExtensionPoint(
          getLogChannel(), variables,
        HopExtensionPoint.BeforeCheckTransforms.id, new CheckTransformsExtension(remarks, variables, this, transforms, metadataProvider) );

      boolean stop_checking = false;

      if (monitor != null) {
        monitor.beginTask(
            BaseMessages.getString(PKG, "PipelineMeta.Monitor.VerifyingThisPipelineTask.Title"),
            transforms.length + 2);
      }

      for (int i = 0; i < transforms.length && !stop_checking; i++) {
        if (monitor != null) {
          monitor.subTask(
              BaseMessages.getString(
                  PKG, "PipelineMeta.Monitor.VerifyingTransformTask.Title", transformnames[i]));
        }

        TransformMeta transformMeta = transforms[i];

        int nrinfo = findNrInfoTransforms(transformMeta);
        TransformMeta[] infoTransform = null;
        if (nrinfo > 0) {
          infoTransform = getInfoTransform(transformMeta);
        }

        IRowMeta info = null;
        if (infoTransform != null) {
          try {
            info = getTransformFields(variables, infoTransform);
          } catch (HopTransformException kse) {
            info = null;
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

        // The previous fields from non-informative transforms:
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
          stop_checking = true;
        }

        if (isTransformUsedInPipelineHops(transformMeta) || getTransforms().size() == 1) {
          // Get the input & output transforms!
          // Copy to arrays:
          String[] input = getPrevTransformNames(transformMeta);
          String[] output = getNextTransformNames(transformMeta);

          // Check transform specific info...
          ExtensionPointHandler.callExtensionPoint(
              getLogChannel(), variables,
            HopExtensionPoint.BeforeCheckTransform.id, new CheckTransformsExtension(
                remarks, variables, this, new TransformMeta[] {transformMeta}, metadataProvider) );
          transformMeta.check(
              remarks, this, prev, input, output, info, variables, metadataProvider);
          ExtensionPointHandler.callExtensionPoint(
              getLogChannel(), variables,
            HopExtensionPoint.AfterCheckTransform.id, new CheckTransformsExtension(
                remarks, variables, this, new TransformMeta[] {transformMeta}, metadataProvider) );

          // See if illegal characters etc. were used in field-names...
          if (prev != null) {
            for (int x = 0; x < prev.size(); x++) {
              IValueMeta v = prev.getValueMeta(x);
              String name = v.getName();
              if (name == null) {
                values.put(
                    v,
                    BaseMessages.getString(
                        PKG, "PipelineMeta.Value.CheckingFieldName.FieldNameIsEmpty.Description"));
              } else if (name.indexOf(' ') >= 0) {
                values.put(
                    v,
                    BaseMessages.getString(
                        PKG,
                        "PipelineMeta.Value.CheckingFieldName.FieldNameContainsSpaces.Description"));
              } else {
                char[] list =
                    new char[] {
                      '.', ',', '-', '/', '+', '*', '\'', '\t', '"', '|', '@', '(', ')', '{', '}',
                      '!', '^'
                    };
                for (int c = 0; c < list.length; c++) {
                  if (name.indexOf(list[c]) >= 0) {
                    values.put(
                        v,
                        BaseMessages.getString(
                            PKG,
                            "PipelineMeta.Value.CheckingFieldName.FieldNameContainsUnfriendlyCodes.Description",
                            String.valueOf(list[c])));
                  }
                }
              }
            }

            // Check if 2 transforms with the same name are entering the transform...
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

        if (monitor != null) {
          monitor.worked(1); // progress bar...
          if (monitor.isCanceled()) {
            stop_checking = true;
          }
        }
      }

      if (monitor != null) {
        monitor.subTask(
            BaseMessages.getString(
                PKG,
                "PipelineMeta.Monitor.CheckingForDatabaseUnfriendlyCharactersInFieldNamesTask.Title"));
      }
      if (values.size() > 0) {
        for (IValueMeta v : values.keySet()) {
          String message = values.get(v);
          CheckResult cr =
              new CheckResult(
                  ICheckResult.TYPE_RESULT_WARNING,
                  BaseMessages.getString(
                      PKG,
                      "PipelineMeta.CheckResult.TypeResultWarning.Description",
                      v.getName(),
                      message,
                      v.getOrigin()),
                  findTransform(v.getOrigin()));
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
      if (monitor != null) {
        monitor.worked(1);
      }
      ExtensionPointHandler.callExtensionPoint(
          getLogChannel(), variables,
        HopExtensionPoint.AfterCheckTransforms.id, new CheckTransformsExtension(remarks, variables, this, transforms, metadataProvider) );
    } catch (Exception e) {
      log.logError(Const.getStackTracker(e));
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the version of the pipeline.
   *
   * @return The version of the pipeline
   */
  public String getPipelineVersion() {
    return pipelineVersion;
  }

  /**
   * Sets the version of the pipeline.
   *
   * @param n The new version description of the pipeline
   */
  public void setPipelineVersion(String n) {
    pipelineVersion = n;
  }

  /**
   * Sets the status of the pipeline.
   *
   * @param n The new status description of the pipeline
   */
  public void setPipelineStatus(int n) {
    pipelineStatus = n;
  }

  /**
   * Gets the status of the pipeline.
   *
   * @return The status of the pipeline
   */
  public int getPipelineStatus() {
    return pipelineStatus;
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
      if (Utils.isEmpty(name)) {
        return filename;
      } else {
        return filename + " : " + name;
      }
    }

    if (name != null) {
      return name;
    } else {
      return PipelineMeta.class.getName();
    }
  }

  /**
   * Cancels queries opened for checking & fieldprediction.
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
        if (includePasswords) {
          if (meta.getPassword() != null) {
            stringList.add(
                new StringSearchResult(
                    meta.getPassword(),
                    meta,
                    this,
                    BaseMessages.getString(PKG, "PipelineMeta.SearchMetadata.DatabasePassword")));
          }
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
    for (int i = 0; i < stringList.size(); i++) {
      StringSearchResult result = stringList.get(i);
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
  public void checkRowMixingStatically(IVariables variables, TransformMeta transformMeta, IProgressMonitor monitor)
      throws HopRowException {
    List<TransformMeta> prevTransforms = findPreviousTransforms(transformMeta);
    int nrPrevious = prevTransforms.size();
    if (nrPrevious > 1) {
      IRowMeta referenceRow = null;
      // See if all previous transforms send out the same rows...
      for (int i = 0; i < nrPrevious; i++) {
        TransformMeta previousTransform = prevTransforms.get(i);
        try {
          IRowMeta row =
              getTransformFields(variables, previousTransform, monitor); // Throws HopTransformException
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
    variables.setVariable(Const.INTERNAL_VARIABLE_PIPELINE_NAME, Const.NVL(name, ""));
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
        log.logError("Unexpected error setting internal filename variables!", e);

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
   * Finds the mapping input transform with the specified name. If no mapping input transform is
   * found, null is returned
   *
   * @param transformName the name to search for
   * @return the transform meta-data corresponding to the desired mapping input transform, or null
   *     if no transform was found
   * @throws HopTransformException if any errors occur during the search
   */
  public TransformMeta findMappingInputTransform(String transformName)
      throws HopTransformException {
    if (!Utils.isEmpty(transformName)) {
      TransformMeta transformMeta =
          findTransform(transformName); // TODO verify that it's a mapping input!!
      if (transformMeta == null) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "PipelineMeta.Exception.TransformNameNotFound", transformName));
      }
      return transformMeta;
    } else {
      // Find the first mapping input transform that fits the bill.
      TransformMeta transformMeta = null;
      for (TransformMeta mappingTransform : transforms) {
        if (mappingTransform.getTransformPluginId().equals("MappingInput")) {
          if (transformMeta == null) {
            transformMeta = mappingTransform;
          } else if (transformMeta != null) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG, "PipelineMeta.Exception.OnlyOneMappingInputTransformAllowed", "2"));
          }
        }
      }
      if (transformMeta == null) {
        throw new HopTransformException(
            BaseMessages.getString(PKG, "PipelineMeta.Exception.OneMappingInputTransformRequired"));
      }
      return transformMeta;
    }
  }

  /**
   * Finds the mapping output transform with the specified name. If no mapping output transform is
   * found, null is returned.
   *
   * @param transformName the name to search for
   * @return the transform meta-data corresponding to the desired mapping input transform, or null
   *     if no transform was found
   * @throws HopTransformException if any errors occur during the search
   */
  public TransformMeta findMappingOutputTransform(String transformName)
      throws HopTransformException {
    if (!Utils.isEmpty(transformName)) {
      TransformMeta transformMeta =
          findTransform(transformName); // TODO verify that it's a mapping output transform.
      if (transformMeta == null) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "PipelineMeta.Exception.TransformNameNotFound", transformName));
      }
      return transformMeta;
    } else {
      // Find the first mapping output transform that fits the bill.
      TransformMeta transformMeta = null;
      for (TransformMeta mappingTransform : transforms) {
        if (mappingTransform.getTransformPluginId().equals("MappingOutput")) {
          if (transformMeta == null) {
            transformMeta = mappingTransform;
          } else if (transformMeta != null) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG, "PipelineMeta.Exception.OnlyOneMappingOutputTransformAllowed", "2"));
          }
        }
      }
      if (transformMeta == null) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "PipelineMeta.Exception.OneMappingOutputTransformRequired"));
      }
      return transformMeta;
    }
  }

  /**
   * Gets a list of the resource dependencies.
   *
   * @return a list of ResourceReferences
   */
  public List<ResourceReference> getResourceDependencies(IVariables variables) {
    return transforms.stream()
        .flatMap(
            (TransformMeta transformMeta) -> transformMeta.getResourceDependencies(variables).stream())
        .collect(Collectors.toList());
  }

  /**
   * Exports the specified objects to a flat-file system, adding content with filename keys to a set
   * of definitions. The supplied resource naming interface allows the object to name appropriately
   * without worrying about those parts of the implementation specific details.
   *
   * @param variables the variable variables to use
   * @param definitions
   * @param iResourceNaming
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
      String fullname;
      String extension = "ktr";
      if (StringUtils.isNotEmpty(getFilename())) {
        FileObject fileObject =
            HopVfs.getFileObject(variables.resolve(getFilename()));
        originalPath = fileObject.getParent().getURL().toString();
        baseName = fileObject.getName().getBaseName();
        fullname = fileObject.getURL().toString();

        exportFileName =
            iResourceNaming.nameResource(
                baseName, originalPath, extension, IResourceNaming.FileNamingType.PIPELINE);
        ResourceDefinition definition = definitions.get(exportFileName);
        if (definition == null) {
          // If we do this once, it will be plenty :-)
          //
          PipelineMeta pipelineMeta = (PipelineMeta) this.realClone(false);
          // pipelineMeta.copyVariablesFrom(variables);

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

          // Set a number of parameters for all the data files referenced so far...
          //
          Map<String, String> directoryMap = iResourceNaming.getDirectoryMap();
          if (directoryMap != null) {
            for (String directory : directoryMap.keySet()) {
              String parameterName = directoryMap.get(directory);
              pipelineMeta.addParameterDefinition(
                  parameterName, directory, "Data file path discovered during export");
            }
          }

          // At the end, add ourselves to the map...
          //
          String pipelineMetaContent = pipelineMeta.getXml();

          definition = new ResourceDefinition(exportFileName, pipelineMetaContent);

          // Also remember the original filename (if any), including variables etc.
          //
          if (Utils.isEmpty(this.getFilename())) { // Generated
            definition.setOrigin(fullname);
          } else {
            definition.setOrigin(this.getFilename());
          }

          definitions.put(fullname, definition);
        }
      }

      return exportFileName;
    } catch (FileSystemException e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "PipelineMeta.Exception.ErrorOpeningOrValidatingTheXMLFile", getFilename()),
          e);
    } catch (HopFileException e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "PipelineMeta.Exception.ErrorOpeningOrValidatingTheXMLFile", getFilename()),
          e);
    }
  }

  /**
   * Checks whether the pipeline is capturing transform performance snapshots.
   *
   * @return true if the pipeline is capturing transform performance snapshots, false otherwise
   */
  public boolean isCapturingTransformPerformanceSnapShots() {
    return capturingTransformPerformanceSnapShots;
  }

  /**
   * Sets whether the pipeline is capturing transform performance snapshots.
   *
   * @param capturingTransformPerformanceSnapShots true if the pipeline is capturing transform
   *     performance snapshots, false otherwise
   */
  public void setCapturingTransformPerformanceSnapShots(
      boolean capturingTransformPerformanceSnapShots) {
    this.capturingTransformPerformanceSnapShots = capturingTransformPerformanceSnapShots;
  }

  /**
   * Gets the transform performance capturing delay.
   *
   * @return the transform performance capturing delay
   */
  public long getTransformPerformanceCapturingDelay() {
    return transformPerformanceCapturingDelay;
  }

  /**
   * Sets the transform performance capturing delay.
   *
   * @param transformPerformanceCapturingDelay the transformPerformanceCapturingDelay to set
   */
  public void setTransformPerformanceCapturingDelay(long transformPerformanceCapturingDelay) {
    this.transformPerformanceCapturingDelay = transformPerformanceCapturingDelay;
  }

  /**
   * Gets the transform performance capturing size limit.
   *
   * @return the transform performance capturing size limit
   */
  public String getTransformPerformanceCapturingSizeLimit() {
    return transformPerformanceCapturingSizeLimit;
  }

  /**
   * Sets the transform performance capturing size limit.
   *
   * @param transformPerformanceCapturingSizeLimit the transform performance capturing size limit to
   *     set
   */
  public void setTransformPerformanceCapturingSizeLimit(
      String transformPerformanceCapturingSizeLimit) {
    this.transformPerformanceCapturingSizeLimit = transformPerformanceCapturingSizeLimit;
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
   * Gets the log channel.
   *
   * @return the log channel
   */
  public ILogChannel getLogChannel() {
    return log;
  }

  /**
   * Gets the log channel ID.
   *
   * @return the log channel ID
   * @see ILoggingObject#getLogChannelId()
   */
  @Override
  public String getLogChannelId() {
    return log.getLogChannelId();
  }

  /**
   * Gets the object type.
   *
   * @return the object type
   * @see ILoggingObject#getObjectType()
   */
  @Override
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.PIPELINE_META;
  }

  /**
   * Gets the pipeline type.
   *
   * @return the pipelineType
   */
  public PipelineType getPipelineType() {
    return pipelineType;
  }

  /**
   * Sets the pipeline type.
   *
   * @param pipelineType the pipelineType to set
   */
  public void setPipelineType(PipelineType pipelineType) {
    this.pipelineType = pipelineType;
  }

  /**
   * Utility method to write the XML of this pipeline to a file, mostly for testing purposes.
   *
   * @param filename The filename to save to
   * @throws HopXmlException in case something goes wrong.
   */
  public void writeXml(String filename) throws HopXmlException {
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(filename);
      fos.write(XmlHandler.getXmlHeader().getBytes(Const.XML_ENCODING));
      fos.write(getXml().getBytes(Const.XML_ENCODING));
    } catch (Exception e) {
      throw new HopXmlException("Unable to save to XML file '" + filename + "'", e);
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          throw new HopXmlException("Unable to close file '" + filename + "'", e);
        }
      }
    }
  }

  @Override
  public boolean isGatheringMetrics() {
    return log.isGatheringMetrics();
  }

  @Override
  public void setGatheringMetrics(boolean gatheringMetrics) {
    log.setGatheringMetrics(gatheringMetrics);
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return log.isForcingSeparateLogging();
  }

  @Override
  public void setForcingSeparateLogging(boolean forcingSeparateLogging) {
    log.setForcingSeparateLogging(forcingSeparateLogging);
  }

  public void addTransformChangeListener(ITransformMetaChangeListener listener) {
    transformChangeListeners.add(listener);
  }

  public void addTransformChangeListener(int p, ITransformMetaChangeListener list) {
    int indexListener = -1;
    int indexListenerRemove = -1;
    TransformMeta rewriteTransform = transforms.get(p);
    ITransformMeta iface = rewriteTransform.getTransform();
    if (iface instanceof ITransformMetaChangeListener) {
      for (ITransformMetaChangeListener listener : transformChangeListeners) {
        indexListener++;
        if (listener.equals(iface)) {
          indexListenerRemove = indexListener;
        }
      }
      if (indexListenerRemove >= 0) {
        transformChangeListeners.add(indexListenerRemove, list);
      } else if (transformChangeListeners.size() == 0 && p == 0) {
        transformChangeListeners.add(list);
      }
    }
  }

  public void removeTransformChangeListener(ITransformMetaChangeListener list) {
    int indexListener = -1;
    int indexListenerRemove = -1;
    for (ITransformMetaChangeListener listener : transformChangeListeners) {
      indexListener++;
      if (listener.equals(list)) {
        indexListenerRemove = indexListener;
      }
    }
    if (indexListenerRemove >= 0) {
      transformChangeListeners.remove(indexListenerRemove);
    }
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
    if (missingPipeline != null && pipeline != null && missingPipeline.contains(pipeline)) {
      missingPipeline.remove(pipeline);
    }
  }

  @Override
  public boolean hasMissingPlugins() {
    return missingPipeline != null && !missingPipeline.isEmpty();
  }

  private static String getTransformMetaCacheKey(TransformMeta transformMeta, boolean info) {
    return String.format(
        "%1$b-%2$s-%3$s", info, transformMeta.getTransformPluginId(), transformMeta.toString());
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
}
