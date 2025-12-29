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

package org.apache.hop.workflow;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.base.BaseHopMeta;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.ProgressNullMonitorListener;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.attributes.AttributesUtil;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.parameters.NamedParameters;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.reflection.StringSearchResult;
import org.apache.hop.core.reflection.StringSearcher;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlFormatter;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.IResourceExport;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.actions.missing.MissingAction;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * The definition of a Hop workflow is represented by a WorkflowMeta object. It is typically loaded
 * from a .hwf file or it is generated dynamically. The declared parameters of the workflow
 * definition are then queried using listParameters() and assigned values using calls to
 * setParameterValue(..). WorkflowMeta provides methods to load, save, verify, etc.
 */
public class WorkflowMeta extends AbstractMeta
    implements Cloneable, Comparable<WorkflowMeta>, IXml, IResourceExport, IHasFilename {
  public static final String WORKFLOW_EXTENSION = ".hwf";
  public static final String XML_TAG = "workflow";
  public static final int BORDER_INDENT = 20;

  /** A constant specifying the tag value for the XML node of the actions. */
  public static final String XML_TAG_ACTIONS = "actions";

  /** A constant specifying the tag value for the XML node of the notes. */
  public static final String XML_TAG_NOTEPADS = "notepads";

  /** A constant specifying the tag value for the XML node of the hops. */
  public static final String XML_TAG_HOPS = "hops";

  /** A constant specifying the tag value for the XML node of the workflow parameters. */
  protected static final String XML_TAG_PARAMETERS = "parameters";

  private static final Class<?> PKG = WorkflowMeta.class;
  private static final String CONST_DESCRIPTION = "description";
  private static final String CONST_PARAMETER = "parameter";
  private static final String CONST_SPACE = "        ";

  /**
   * List of booleans indicating whether or not to remember the size and position of the different
   * windows...
   */
  public boolean[] max = new boolean[1];

  @HopMetadataProperty(inline = true)
  protected WorkflowMetaInfo info;

  protected String workflowVersion;
  protected int workflowStatus;
  protected List<ActionMeta> workflowActions;
  protected List<WorkflowHopMeta> workflowHops;
  protected String[] arguments;
  protected boolean changedActions;
  protected boolean changedHops;
  protected String startActionName;
  protected boolean expandingRemoteWorkflow;

  /** The loop cache. */
  protected Map<String, Boolean> loopCache;

  private List<MissingAction> missingActions;

  /** Instantiates a new workflow meta. */
  public WorkflowMeta() {
    clear();
  }

  /**
   * Instantiates a new workflow meta.
   *
   * @param fname the fname
   * @throws HopXmlException the hop xml exception
   */
  public WorkflowMeta(String fname) throws HopXmlException {
    this(null, fname, null);
  }

  /**
   * Load the workflow from the XML file specified
   *
   * @param variables
   * @param fname
   * @param metadataProvider
   * @throws HopXmlException
   */
  public WorkflowMeta(IVariables variables, String fname, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    this.metadataProvider = metadataProvider;
    loadXml(variables, fname, metadataProvider);
  }

  /**
   * Instantiates a new workflow meta.
   *
   * @param inputStream the input stream
   * @param variables
   * @throws HopXmlException the hop xml exception
   */
  public WorkflowMeta(
      InputStream inputStream, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    this();
    this.metadataProvider = metadataProvider;
    Document doc = XmlHandler.loadXmlFile(inputStream, null, false, false);
    Node subNode = XmlHandler.getSubNode(doc, WorkflowMeta.XML_TAG);
    loadXml(subNode, null, metadataProvider, variables);
  }

  /**
   * Create a new WorkflowMeta object by loading it from a a DOM node.
   *
   * @param workflowNode The node to load from
   * @param variables
   * @throws HopXmlException
   */
  public WorkflowMeta(
      Node workflowNode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    this();
    this.metadataProvider = metadataProvider;
    loadXml(workflowNode, null, metadataProvider, variables);
  }

  /** Clears or reinitializes many of the WorkflowMeta properties. */
  @Override
  public void clear() {
    workflowActions = new ArrayList<>();
    workflowHops = new ArrayList<>();
    namedParams = new NamedParameters();

    info = new WorkflowMetaInfo();
    arguments = null;

    super.clear();
    loopCache = new HashMap<>();
    addDefaults();
    workflowStatus = -1;
    workflowVersion = null;
  }

  /** Adds the defaults. */
  public void addDefaults() {
    clearChanged();
  }

  /**
   * Gets the start.
   *
   * @return the start
   */
  public ActionMeta getStart() {
    for (ActionMeta action : workflowActions) {
      if (action.isStart()) {
        return action;
      }
    }
    return null;
  }

  /**
   * Compares two workflows on name, filename, etc. The comparison algorithm is as follows:<br>
   *
   * <ol>
   *   <li>The first workflow's filename is checked first; if it has none, the workflow is created.
   *       If the second workflow does not come from a repository, -1 is returned.
   *   <li>If the workflows are both created, the workflows' names are compared. If the first
   *       workflow has no name and the second one does, a -1 is returned. If the opposite is true,
   *       a 1 is returned.
   *   <li>If they both have names they are compared as strings. If the result is non-zero it is
   *       returned.
   * </ol>
   *
   * @param workflow1 the first workflow to compare
   * @param workflow2 the second workflow to compare
   * @return 0 if the two workflows are equal, 1 or -1 depending on the values (see description
   *     above)
   */
  public int compare(WorkflowMeta workflow1, WorkflowMeta workflow2) {
    return super.compare(workflow1, workflow2);
  }

  /**
   * Compares this workflow's meta-data to the specified workflow's meta-data. This method simply
   * calls compare(this, o)
   *
   * @param o the o
   * @return the int
   * @see #compare(WorkflowMeta, WorkflowMeta)
   * @see Comparable#compareTo(Object)
   */
  @Override
  public int compareTo(WorkflowMeta o) {
    return compare(this, o);
  }

  /**
   * Checks whether this workflow's meta-data object is equal to the specified object. If the
   * specified object is not an instance of WorkflowMeta, false is returned. Otherwise the method
   * returns whether a call to compare() indicates equality (i.e. compare(this,
   * (WorkflowMeta)obj)==0).
   *
   * @param obj the obj
   * @return true, if successful
   * @see #compare(WorkflowMeta, WorkflowMeta)
   * @see Object#equals(Object)
   */
  public boolean equals(Object obj) {
    if (!(obj instanceof WorkflowMeta)) {
      return false;
    }

    return compare(this, (WorkflowMeta) obj) == 0;
  }

  /**
   * Clones the workflow meta-data object.
   *
   * @return a clone of the workflow meta-data object
   * @see Object#clone()
   */
  @Override
  public Object clone() {
    return realClone(true);
  }

  /**
   * Perform a real clone of the workflow meta-data object, including cloning all lists and copying
   * all values. If the doClear parameter is true, the clone will be cleared of ALL values before
   * the copy. If false, only the copied fields will be cleared.
   *
   * @param doClear Whether to clear all of the clone's data before copying from the source object
   * @return a real clone of the calling object
   */
  public Object realClone(boolean doClear) {
    try {
      WorkflowMeta workflowMeta = (WorkflowMeta) super.clone();
      if (doClear) {
        workflowMeta.clear();
      } else {
        workflowMeta.workflowActions = new ArrayList<>();
        workflowMeta.workflowHops = new ArrayList<>();
        workflowMeta.notes = new ArrayList<>();
        workflowMeta.namedParams = new NamedParameters();
      }

      for (ActionMeta action : workflowActions) {
        workflowMeta.workflowActions.add((ActionMeta) action.cloneDeep());
      }
      for (WorkflowHopMeta hop : workflowHops) {
        workflowMeta.workflowHops.add(hop.clone());
      }
      for (NotePadMeta notePad : notes) {
        workflowMeta.notes.add(notePad.clone());
      }

      for (String key : listParameters()) {
        workflowMeta.addParameterDefinition(
            key, getParameterDefault(key), getParameterDescription(key));
      }
      return workflowMeta;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  protected String getExtension() {
    return WORKFLOW_EXTENSION;
  }

  /** Clears the different changed flags of the workflow. */
  @Override
  public void clearChanged() {
    changedActions = false;
    changedHops = false;

    for (ActionMeta action : workflowActions) {
      action.setChanged(false);
    }
    for (WorkflowHopMeta hop : workflowHops) {
      // Look at all the hops
      hop.setChanged(false);
    }
    super.clearChanged();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.changed.ChangedFlag#hasChanged()
   */
  @Override
  public boolean hasChanged() {
    return super.hasChanged() || haveActionsChanged() || haveWorkflowHopsChanged();
  }

  /**
   * Gets the XML representation of this workflow.
   *
   * @return the XML representation of this workflow
   * @throws HopException if any errors occur during generation of the XML
   * @see IXml#getXml(IVariables)
   * @param variables
   */
  @Override
  public String getXml(IVariables variables) throws HopException {

    StringBuilder xml = new StringBuilder(500);

    xml.append(XmlHandler.getLicenseHeader(variables));

    xml.append(XmlHandler.openTag(XML_TAG)).append(Const.CR);

    xml.append("  ")
        .append(
            XmlHandler.addTagValue("name", getName())); // lossy if name is sync'ed with filename
    xml.append("  ")
        .append(
            XmlHandler.addTagValue(
                "name_sync_with_filename", info.isNameSynchronizedWithFilename()));

    xml.append("  ").append(XmlHandler.addTagValue(CONST_DESCRIPTION, info.getDescription()));
    xml.append("  ")
        .append(XmlHandler.addTagValue("extended_description", info.getExtendedDescription()));
    xml.append("  ").append(XmlHandler.addTagValue("workflow_version", workflowVersion));
    if (workflowStatus >= 0) {
      xml.append("  ").append(XmlHandler.addTagValue("workflow_status", workflowStatus));
    }

    xml.append("  ").append(XmlHandler.addTagValue("created_user", info.getCreatedUser()));
    xml.append("  ")
        .append(
            XmlHandler.addTagValue("created_date", XmlHandler.date2string(info.getCreatedDate())));
    xml.append("  ").append(XmlHandler.addTagValue("modified_user", info.getModifiedUser()));
    xml.append("  ")
        .append(
            XmlHandler.addTagValue(
                "modified_date", XmlHandler.date2string(info.getModifiedDate())));

    xml.append("    ").append(XmlHandler.openTag(XML_TAG_PARAMETERS)).append(Const.CR);
    String[] parameters = listParameters();
    for (int idx = 0; idx < parameters.length; idx++) {
      xml.append("      ").append(XmlHandler.openTag(CONST_PARAMETER)).append(Const.CR);
      xml.append(CONST_SPACE).append(XmlHandler.addTagValue("name", parameters[idx]));
      try {
        xml.append(CONST_SPACE)
            .append(XmlHandler.addTagValue("default_value", getParameterDefault(parameters[idx])));
        xml.append(CONST_SPACE)
            .append(
                XmlHandler.addTagValue(
                    CONST_DESCRIPTION, getParameterDescription(parameters[idx])));
      } catch (UnknownParamException e) {
        // skip the default value and/or description. This exception should never happen because we
        // use listParameters()
        // above.
      }
      xml.append("      ").append(XmlHandler.closeTag(CONST_PARAMETER)).append(Const.CR);
    }
    xml.append("    ").append(XmlHandler.closeTag(XML_TAG_PARAMETERS)).append(Const.CR);

    xml.append("  ").append(XmlHandler.openTag(XML_TAG_ACTIONS)).append(Const.CR);
    for (int i = 0; i < nrActions(); i++) {
      ActionMeta jge = getAction(i);
      xml.append(jge.getXml());
    }
    xml.append("  ").append(XmlHandler.closeTag(XML_TAG_ACTIONS)).append(Const.CR);

    xml.append("  ").append(XmlHandler.openTag(XML_TAG_HOPS)).append(Const.CR);
    for (WorkflowHopMeta hi : workflowHops) {
      // Look at all the hops
      xml.append(hi.getXml());
    }
    xml.append("  ").append(XmlHandler.closeTag(XML_TAG_HOPS)).append(Const.CR);

    xml.append("  ").append(XmlHandler.openTag(XML_TAG_NOTEPADS)).append(Const.CR);
    for (int i = 0; i < nrNotes(); i++) {
      NotePadMeta ni = getNote(i);
      xml.append(ni.getXml());
    }
    xml.append("  ").append(XmlHandler.closeTag(XML_TAG_NOTEPADS)).append(Const.CR);

    // Also store the attribute groups
    //
    xml.append(AttributesUtil.getAttributesXml(attributesMap));

    xml.append(XmlHandler.closeTag(XML_TAG)).append(Const.CR);

    return XmlFormatter.format(xml.toString());
  }

  public void loadXml(IVariables variables, String fname, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      // OK, try to load using the VFS stuff...
      Document doc = XmlHandler.loadXmlFile(HopVfs.getFileObject(fname));
      if (doc != null) {
        // The workflowNode
        Node workflowNode = XmlHandler.getSubNode(doc, XML_TAG);

        loadXml(workflowNode, fname, metadataProvider, variables);
      } else {
        throw new HopXmlException(
            BaseMessages.getString(PKG, "WorkflowMeta.Exception.ErrorReadingFromXMLFile") + fname);
      }
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "WorkflowMeta.Exception.UnableToLoadWorkflowFromXMLFile")
              + fname
              + "]",
          e);
    }
  }

  /**
   * Load a block of XML from an DOM node.
   *
   * @param workflowNode The node to load from
   * @param filename The filename
   * @param metadataProvider the MetaStore to use
   * @param variables
   * @throws HopXmlException
   */
  public void loadXml(
      Node workflowNode,
      String filename,
      IHopMetadataProvider metadataProvider,
      IVariables variables)
      throws HopXmlException {
    try {
      // clear the workflows
      clear();

      setFilename(filename);

      // get workflow info:
      //
      info.setName(XmlHandler.getTagValue(workflowNode, "name"));

      info.setNameSynchronizedWithFilename(
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(workflowNode, "name_sync_with_filename")));

      // description
      info.setDescription(XmlHandler.getTagValue(workflowNode, CONST_DESCRIPTION));

      // extended description
      info.setExtendedDescription(XmlHandler.getTagValue(workflowNode, "extended_description"));

      // workflow version
      workflowVersion = XmlHandler.getTagValue(workflowNode, "workflow_version");

      // workflow status
      workflowStatus = Const.toInt(XmlHandler.getTagValue(workflowNode, "workflow_status"), -1);

      // Created user/date
      info.setCreatedUser(XmlHandler.getTagValue(workflowNode, "created_user"));
      String createDate = XmlHandler.getTagValue(workflowNode, "created_date");

      if (createDate != null) {
        info.setCreatedDate(XmlHandler.stringToDate(createDate));
      }

      // Changed user/date
      info.setModifiedUser(XmlHandler.getTagValue(workflowNode, "modified_user"));
      String modDate = XmlHandler.getTagValue(workflowNode, "modified_date");
      if (modDate != null) {
        info.setModifiedDate(XmlHandler.stringToDate(modDate));
      }

      // Read the named parameters.
      //
      Node paramsNode = XmlHandler.getSubNode(workflowNode, XML_TAG_PARAMETERS);
      List<Node> paramNodes = XmlHandler.getNodes(paramsNode, CONST_PARAMETER);
      for (Node paramNode : paramNodes) {
        String parameterName = XmlHandler.getTagValue(paramNode, "name");
        String defaultValue = XmlHandler.getTagValue(paramNode, "default_value");
        String description = XmlHandler.getTagValue(paramNode, CONST_DESCRIPTION);

        addParameterDefinition(parameterName, defaultValue, description);
      }

      // read the action metadata
      //
      Node actionsNode = XmlHandler.getSubNode(workflowNode, XML_TAG_ACTIONS);
      List<Node> actionNodes = XmlHandler.getNodes(actionsNode, ActionMeta.XML_TAG);
      for (Node actionNode : actionNodes) {
        ActionMeta actionMeta = new ActionMeta(actionNode, metadataProvider, variables);

        if (actionMeta.isMissing()) {
          addMissingAction((MissingAction) actionMeta.getAction());
        }
        ActionMeta prev = findAction(actionMeta.getName());
        if (prev != null) {
          // See if the action already exists!
          // Replace previous version with this one: remove it first
          //
          int idx = indexOfAction(prev);
          removeAction(idx);
        }
        // Add the ActionCopy...
        addAction(actionMeta);
      }

      Node hopsNode = XmlHandler.getSubNode(workflowNode, XML_TAG_HOPS);
      List<Node> hopNodes = XmlHandler.getNodes(hopsNode, BaseHopMeta.XML_HOP_TAG);
      for (Node hopNode : hopNodes) {
        WorkflowHopMeta hi = new WorkflowHopMeta(hopNode, this);
        workflowHops.add(hi);
      }

      // Read the notes...
      //
      Node notepadsNode = XmlHandler.getSubNode(workflowNode, XML_TAG_NOTEPADS);
      List<Node> nodepadNodes = XmlHandler.getNodes(notepadsNode, NotePadMeta.XML_TAG);
      for (Node notepadNode : nodepadNodes) {
        NotePadMeta ni = new NotePadMeta(notepadNode);
        notes.add(ni);
      }

      // Load the attribute groups map
      //
      attributesMap =
          AttributesUtil.loadAttributes(
              XmlHandler.getSubNode(workflowNode, AttributesUtil.XML_TAG));

      clearChanged();

      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowMetaLoaded.id, this);

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "WorkflowMeta.Exception.UnableToLoadWorkflowFromXMLNode"), e);
    }
  }

  /**
   * Gets the action copy.
   *
   * @param x the x
   * @param y the y
   * @param iconsize the iconsize
   * @return the action copy
   */
  public ActionMeta getAction(int x, int y, int iconsize) {
    int i;
    int s;
    s = nrActions();
    for (i = s - 1; i >= 0; i--) {
      // Back to front because drawing goes from start to end

      ActionMeta action = getAction(i);
      Point p = action.getLocation();
      if (p != null && x >= p.x && x <= p.x + iconsize && y >= p.y && y <= p.y + iconsize) {
        return action;
      }
    }
    return null;
  }

  /**
   * Nr actions.
   *
   * @return the int
   */
  public int nrActions() {
    return workflowActions.size();
  }

  /**
   * Nr workflow hops.
   *
   * @return the int
   */
  public int nrWorkflowHops() {
    return workflowHops.size();
  }

  /**
   * Gets the workflow hop.
   *
   * @param i the i
   * @return the workflow hop
   */
  public WorkflowHopMeta getWorkflowHop(int i) {
    return workflowHops.get(i);
  }

  /**
   * Gets the action.
   *
   * @param i the i
   * @return the action
   */
  public ActionMeta getAction(int i) {
    return workflowActions.get(i);
  }

  /**
   * Adds the action.
   *
   * @param action the je
   */
  public void addAction(ActionMeta action) {
    workflowActions.add(action);
    action.setParentWorkflowMeta(this);
    setChanged();
  }

  /**
   * Adds the workflow hop.
   *
   * @param hop the hi
   */
  public void addWorkflowHop(WorkflowHopMeta hop) {
    workflowHops.add(hop);
    setChanged();
  }

  /**
   * Adds the action.
   *
   * @param p the p
   * @param action the si
   */
  public void addAction(int p, ActionMeta action) {
    workflowActions.add(p, action);
    changedActions = true;
  }

  /**
   * Adds the workflow hop.
   *
   * @param p the p
   * @param hop the hi
   */
  public void addWorkflowHop(int p, WorkflowHopMeta hop) {
    try {
      workflowHops.add(p, hop);
    } catch (IndexOutOfBoundsException e) {
      workflowHops.add(hop);
    }
    changedHops = true;
  }

  /**
   * Removes the action.
   *
   * @param i the i
   */
  public void removeAction(int i) {
    ActionMeta deleted = workflowActions.remove(i);
    if (deleted != null) {
      // give transform a chance to cleanup
      deleted.setParentWorkflowMeta(null);
      if (deleted.getAction() instanceof MissingAction missingAction) {
        removeMissingAction(missingAction);
      }
    }
    setChanged();
  }

  /**
   * Removes the workflow hop.
   *
   * @param i the i
   */
  public void removeWorkflowHop(int i) {
    workflowHops.remove(i);
    setChanged();
  }

  /**
   * Removes a hop from the pipeline. Also marks that the pipeline's hops have changed.
   *
   * @param hop The hop to remove from the list of hops
   */
  public void removeWorkflowHop(WorkflowHopMeta hop) {
    workflowHops.remove(hop);
    setChanged();
  }

  /**
   * Index of workflow hop.
   *
   * @param hop the workflow hop
   * @return the int
   */
  public int indexOfWorkflowHop(WorkflowHopMeta hop) {
    return workflowHops.indexOf(hop);
  }

  /**
   * Index of action.
   *
   * @param action the ActionMeta
   * @return the int
   */
  public int indexOfAction(ActionMeta action) {
    return workflowActions.indexOf(action);
  }

  /**
   * Sets the action.
   *
   * @param idx the idx
   * @param action the jec
   */
  public void setAction(int idx, ActionMeta action) {
    workflowActions.set(idx, action);
  }

  /**
   * Find an existing ActionMeta by it's name and number
   *
   * @param name The name of the action meta
   * @return The ActionMeta or null if nothing was found!
   */
  public ActionMeta findAction(String name) {
    for (ActionMeta action : workflowActions) {
      if (action.getName().equalsIgnoreCase(name)) {
        return action;
      }
    }
    return null;
  }

  /**
   * Find workflow hop.
   *
   * @param name the name
   * @return the workflow hop meta
   */
  public WorkflowHopMeta findWorkflowHop(String name) {
    for (WorkflowHopMeta hop : workflowHops) {
      // Look at all the hops

      if (hop.toString().equalsIgnoreCase(name)) {
        return hop;
      }
    }
    return null;
  }

  /**
   * Find workflow hop from.
   *
   * @param action the action meta
   * @return the workflow hop meta
   */
  public WorkflowHopMeta findWorkflowHopFrom(ActionMeta action) {
    if (action != null) {
      for (WorkflowHopMeta hop : workflowHops) {

        // Return the first we find!
        //
        if (hop != null && (hop.getFromAction() != null) && hop.getFromAction().equals(action)) {
          return hop;
        }
      }
    }
    return null;
  }

  /**
   * Find workflow hop.
   *
   * @param from the from
   * @param to the to
   * @return the workflow hop meta
   */
  public WorkflowHopMeta findWorkflowHop(ActionMeta from, ActionMeta to) {
    return findWorkflowHop(from, to, false);
  }

  /**
   * Find workflow hop.
   *
   * @param from the from
   * @param to the to
   * @param includeDisabled the include disabled
   * @return the workflow hop meta
   */
  public WorkflowHopMeta findWorkflowHop(ActionMeta from, ActionMeta to, boolean includeDisabled) {
    for (WorkflowHopMeta hop : workflowHops) {
      if ((hop.isEnabled() || includeDisabled)
          && hop != null
          && hop.getFromAction() != null
          && hop.getToAction() != null
          && hop.getFromAction().equals(from)
          && hop.getToAction().equals(to)) {
        return hop;
      }
    }
    return null;
  }

  /**
   * Find workflow hop to.
   *
   * @param actionMeta the action meta
   * @return the workflow hop meta
   */
  public WorkflowHopMeta findWorkflowHopTo(ActionMeta actionMeta) {
    for (WorkflowHopMeta hop : workflowHops) {
      if (hop != null && hop.getToAction() != null && hop.getToAction().equals(actionMeta)) {
        // Return the first!
        return hop;
      }
    }
    return null;
  }

  /**
   * Find nr prev actions.
   *
   * @param from the from
   * @return the int
   */
  public int findNrPrevActions(ActionMeta from) {
    return findNrPrevActions(from, false);
  }

  /**
   * Find prev action.
   *
   * @param to the to
   * @param nr the nr
   * @return the action copy
   */
  public ActionMeta findPrevAction(ActionMeta to, int nr) {
    return findPrevAction(to, nr, false);
  }

  /**
   * Find nr prev actions.
   *
   * @param to the to
   * @param info the info
   * @return the int
   */
  public int findNrPrevActions(ActionMeta to, boolean info) {
    int count = 0;

    for (WorkflowHopMeta hop : workflowHops) {
      // Look at all the hops

      if (hop.isEnabled() && hop.getToAction().equals(to)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Find prev action.
   *
   * @param to the to
   * @param nr the nr
   * @param info the info
   * @return the action copy
   */
  public ActionMeta findPrevAction(ActionMeta to, int nr, boolean info) {
    int count = 0;

    for (WorkflowHopMeta hop : workflowHops) {
      // Look at all the hops

      if (hop.isEnabled() && hop.getToAction().equals(to)) {
        if (count == nr) {
          return hop.getFromAction();
        }
        count++;
      }
    }
    return null;
  }

  /**
   * Find nr next actions.
   *
   * @param from the from
   * @return the int
   */
  public int findNrNextActions(ActionMeta from) {
    int count = 0;
    for (WorkflowHopMeta hop : workflowHops) {
      // Look at all the hops

      if (hop.isEnabled() && (hop.getFromAction() != null) && hop.getFromAction().equals(from)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Find next action.
   *
   * @param from the from
   * @param cnt the cnt
   * @return the action copy
   */
  public ActionMeta findNextAction(ActionMeta from, int cnt) {
    int count = 0;

    for (WorkflowHopMeta hop : workflowHops) {
      // Look at all the hops

      if (hop.isEnabled() && (hop.getFromAction() != null) && hop.getFromAction().equals(from)) {
        if (count == cnt) {
          return hop.getToAction();
        }
        count++;
      }
    }
    return null;
  }

  /**
   * Checks for loop.
   *
   * @param action the action
   * @return true, if successful
   */
  public boolean hasLoop(ActionMeta action) {
    clearLoopCache();
    return hasLoop(action, null);
  }

  /**
   * Checks for loop.
   *
   * @param action the action
   * @param lookup the lookup
   * @return true, if successful
   */
  public boolean hasLoop(ActionMeta action, ActionMeta lookup) {
    return hasLoop(action, lookup, new HashSet<>());
  }

  /**
   * Checks for loop.
   *
   * @param action the action
   * @param lookup the lookup
   * @return true, if successful
   */
  private boolean hasLoop(
      ActionMeta action, ActionMeta lookup, HashSet<ActionMeta> checkedActions) {
    String cacheKey = action.getName() + " - " + (lookup != null ? lookup.getName() : "");

    Boolean hasLoop = loopCache.get(cacheKey);

    if (hasLoop != null) {
      return hasLoop;
    }

    hasLoop = false;

    checkedActions.add(action);

    int nr = findNrPrevActions(action);
    for (int i = 0; i < nr; i++) {
      ActionMeta prevWorkflowMeta = findPrevAction(action, i);
      if (prevWorkflowMeta != null
          && (prevWorkflowMeta.equals(lookup)
              || (!checkedActions.contains(prevWorkflowMeta)
                  && hasLoop(
                      prevWorkflowMeta, lookup == null ? action : lookup, checkedActions)))) {
        hasLoop = true;
        break;
      }
    }

    loopCache.put(cacheKey, hasLoop);
    return hasLoop;
  }

  /** Clears the loop cache. */
  private void clearLoopCache() {
    loopCache.clear();
  }

  /**
   * Checks if is action used in hops.
   *
   * @param action the jge
   * @return true, if is action used in hops
   */
  public boolean isActionUsedInHops(ActionMeta action) {
    WorkflowHopMeta from = findWorkflowHopFrom(action);
    WorkflowHopMeta to = findWorkflowHopTo(action);
    return from != null || to != null;
  }

  /**
   * Count actions.
   *
   * @param name the name
   * @return the int
   */
  public int countActions(String name) {
    int count = 0;
    int i;
    for (i = 0; i < nrActions(); i++) {
      // Look at all the hops

      ActionMeta actionMeta = getAction(i);
      if (actionMeta.getName().equalsIgnoreCase(name)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Proposes an alternative action name when the original already exists...
   *
   * @param name The action name to find an alternative for..
   * @return The alternative action name.
   */
  public String getAlternativeActionName(String name) {
    String newname = name;
    ActionMeta action = findAction(newname);
    int nr = 1;
    while (action != null) {
      nr++;
      newname = name + " " + nr;
      action = findAction(newname);
    }

    return newname;
  }

  /**
   * Gets the all workflow graph actions.
   *
   * @param name the name
   * @return the all workflow graph actions
   */
  public ActionMeta[] getAllWorkflowGraphActions(String name) {
    int count = 0;
    for (int i = 0; i < nrActions(); i++) {
      ActionMeta actionMeta = getAction(i);
      if (actionMeta.getName().equalsIgnoreCase(name)) {
        count++;
      }
    }
    ActionMeta[] retval = new ActionMeta[count];

    count = 0;
    for (int i = 0; i < nrActions(); i++) {
      ActionMeta actionMeta = getAction(i);
      if (actionMeta.getName().equalsIgnoreCase(name)) {
        retval[count] = actionMeta;
        count++;
      }
    }
    return retval;
  }

  /**
   * Gets the all workflow hops using.
   *
   * @param name the name
   * @return the all workflow hops using
   */
  public WorkflowHopMeta[] getAllWorkflowHopsUsing(String name) {
    List<WorkflowHopMeta> hops = new ArrayList<>();

    for (WorkflowHopMeta hop : workflowHops) {
      // Look at all the hops

      if (hop.getFromAction() != null
          && hop.getToAction() != null
          && (hop.getFromAction().getName().equalsIgnoreCase(name)
              || hop.getToAction().getName().equalsIgnoreCase(name))) {
        hops.add(hop);
      }
    }
    return hops.toArray(new WorkflowHopMeta[hops.size()]);
  }

  public boolean isPathExist(IAction from, IAction to) {
    for (WorkflowHopMeta hop : workflowHops) {
      if (hop.getFromAction() != null
          && hop.getToAction() != null
          && hop.getFromAction().getName().equalsIgnoreCase(from.getName())) {
        if (hop.getToAction().getName().equalsIgnoreCase(to.getName())) {
          return true;
        }
        if (isPathExist(hop.getToAction().getAction(), to)) {
          return true;
        }
      }
    }

    return false;
  }

  /** Select all. */
  public void selectAll() {
    for (ActionMeta action : workflowActions) {
      action.setSelected(true);
    }
    for (NotePadMeta note : getNotes()) {
      note.setSelected(true);
    }
    setChanged();
    notifyObservers("refreshGraph");
  }

  /** Unselect all. */
  public void unselectAll() {
    for (ActionMeta action : workflowActions) {
      action.setSelected(false);
    }
    for (NotePadMeta note : getNotes()) {
      note.setSelected(false);
    }
  }

  /**
   * Gets the maximum.
   *
   * @return the maximum
   */
  public Point getMaximum() {
    int maxx = 0;
    int maxy = 0;
    for (ActionMeta action : workflowActions) {
      Point loc = action.getLocation();
      if (loc.x > maxx) {
        maxx = loc.x;
      }
      if (loc.y > maxy) {
        maxy = loc.y;
      }
    }
    for (NotePadMeta note : getNotes()) {
      Point loc = note.getLocation();
      if (loc.x + note.width > maxx) {
        maxx = loc.x + note.width;
      }
      if (loc.y + note.height > maxy) {
        maxy = loc.y + note.height;
      }
    }

    return new Point(maxx + 100, maxy + 100);
  }

  /**
   * Get the minimum point on the canvas of a workflow
   *
   * @return Minimum coordinate of a transform in the workflow
   */
  public Point getMinimum() {
    int minx = Integer.MAX_VALUE;
    int miny = Integer.MAX_VALUE;
    for (ActionMeta action : workflowActions) {
      Point loc = action.getLocation();
      if (loc.x < minx) {
        minx = loc.x;
      }
      if (loc.y < miny) {
        miny = loc.y;
      }
    }
    for (NotePadMeta note : getNotes()) {
      Point loc = note.getLocation();
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
   * Gets the selected actions locations.
   *
   * @return the selected actions locations
   */
  public Point[] getSelectedLocations() {
    List<ActionMeta> actions = getSelectedActions();
    Point[] retval = new Point[actions.size()];
    for (int i = 0; i < retval.length; i++) {
      ActionMeta action = actions.get(i);
      Point p = action.getLocation();
      retval[i] = new Point(p.x, p.y); // explicit copy of location
    }
    return retval;
  }

  /**
   * Get all the selected notes locations
   *
   * @return The selected notes locations.
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
   * Gets the selected actions.
   *
   * @return the selected actions
   */
  public List<ActionMeta> getSelectedActions() {
    List<ActionMeta> selection = new ArrayList<>();
    for (ActionMeta actionMeta : workflowActions) {
      if (actionMeta.isSelected()) {
        selection.add(actionMeta);
      }
    }
    return selection;
  }

  /**
   * Gets the action indexes.
   *
   * @param actions the actions
   * @return the action indexes
   */
  public int[] getActionIndexes(List<ActionMeta> actions) {
    int[] retval = new int[actions.size()];

    for (int i = 0; i < actions.size(); i++) {
      retval[i] = indexOfAction(actions.get(i));
    }

    return retval;
  }

  /**
   * Find start.
   *
   * @return the action copy
   */
  public ActionMeta findStart() {
    for (ActionMeta actionMeta : workflowActions) {
      if (actionMeta.isStart()) {
        return actionMeta;
      }
    }
    return null;
  }

  /**
   * Gets a textual representation of the workflow. If its name has been set, it will be returned,
   * otherwise the classname is returned.
   *
   * @return the textual representation of the workflow.
   */
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
      return WorkflowMeta.class.getName();
    }
  }

  public List<SqlStatement> getSqlStatements(IProgressMonitor monitor, IVariables variables)
      throws HopException {
    return getSqlStatements(null, monitor, variables);
  }

  /**
   * Builds a list of all the SQL statements that this pipeline needs in order to work properly.
   *
   * @return An ArrayList of SqlStatement objects.
   */
  public List<SqlStatement> getSqlStatements(
      IHopMetadataProvider metadataProvider, IProgressMonitor monitor, IVariables variables)
      throws HopException {
    if (monitor != null) {
      monitor.beginTask(
          BaseMessages.getString(PKG, "WorkflowMeta.Monitor.GettingSQLNeededForThisWorkflow"),
          nrActions() + 1);
    }
    List<SqlStatement> stats = new ArrayList<>();

    for (int i = 0; i < nrActions(); i++) {
      ActionMeta copy = getAction(i);
      if (monitor != null) {
        monitor.subTask(
            BaseMessages.getString(PKG, "WorkflowMeta.Monitor.GettingSQLForActionCopy")
                + copy
                + "]");
      }
      stats.addAll(copy.getAction().getSqlStatements(metadataProvider, variables));
      if (monitor != null) {
        monitor.worked(1);
      }
    }

    if (monitor != null) {
      monitor.worked(1);
    }
    if (monitor != null) {
      monitor.done();
    }

    return stats;
  }

  /**
   * Get a list of all the strings used in this workflow.
   *
   * @return A list of StringSearchResult with strings used in the workflow
   */
  public List<StringSearchResult> getStringList(
      boolean searchTransforms, boolean searchDatabases, boolean searchNotes) {
    List<StringSearchResult> stringList = new ArrayList<>();

    if (searchTransforms) {
      // Loop over all transforms in the pipeline and see what the used
      // vars are...
      for (int i = 0; i < nrActions(); i++) {
        ActionMeta actionMeta = getAction(i);
        stringList.add(
            new StringSearchResult(
                actionMeta.getName(),
                actionMeta,
                this,
                BaseMessages.getString(PKG, "WorkflowMeta.SearchMetadata.ActionName")));
        if (actionMeta.getDescription() != null) {
          stringList.add(
              new StringSearchResult(
                  actionMeta.getDescription(),
                  actionMeta,
                  this,
                  BaseMessages.getString(PKG, "WorkflowMeta.SearchMetadata.ActionDescription")));
        }
        IAction action = actionMeta.getAction();
        StringSearcher.findMetaData(action, 1, stringList, actionMeta, this);
      }
    }

    // Loop over all transforms in the pipeline and see what the used vars
    // are...
    if (searchDatabases) {
      for (DatabaseMeta meta : getDatabases()) {
        stringList.add(
            new StringSearchResult(
                meta.getName(),
                meta,
                this,
                BaseMessages.getString(PKG, "WorkflowMeta.SearchMetadata.DatabaseConnectionName")));
        if (meta.getHostname() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getHostname(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "WorkflowMeta.SearchMetadata.DatabaseHostName")));
        }
        if (meta.getDatabaseName() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getDatabaseName(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "WorkflowMeta.SearchMetadata.DatabaseName")));
        }
        if (meta.getUsername() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getUsername(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "WorkflowMeta.SearchMetadata.DatabaseUsername")));
        }
        if (meta.getPluginId() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getPluginId(),
                  meta,
                  this,
                  BaseMessages.getString(
                      PKG, "WorkflowMeta.SearchMetadata.DatabaseTypeDescription")));
        }
        if (meta.getPort() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getPort(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "WorkflowMeta.SearchMetadata.DatabasePort")));
        }
        if (meta.getServername() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getServername(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "WorkflowMeta.SearchMetadata.DatabaseServer")));
        }
        if (meta.getPassword() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getPassword(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "WorkflowMeta.SearchMetadata.DatabasePassword")));
        }
      }
    }

    // Loop over all transforms in the pipeline and see what the used vars
    // are...
    if (searchNotes) {
      for (int i = 0; i < nrNotes(); i++) {
        NotePadMeta meta = getNote(i);
        if (meta.getNote() != null) {
          stringList.add(
              new StringSearchResult(
                  meta.getNote(),
                  meta,
                  this,
                  BaseMessages.getString(PKG, "WorkflowMeta.SearchMetadata.NotepadText")));
        }
      }
    }

    return stringList;
  }

  /**
   * Gets the used variables.
   *
   * @return the used variables
   */
  public List<String> getUsedVariables() {
    // Get the list of Strings.
    List<StringSearchResult> stringList = getStringList(true, true, false);

    List<String> varList = new ArrayList<>();

    // Look around in the strings, see what we find...
    for (StringSearchResult result : stringList) {
      StringUtil.getUsedVariables(result.getString(), varList, false);
    }

    return varList;
  }

  /**
   * Have actions changed.
   *
   * @return true, if successful
   */
  public boolean haveActionsChanged() {
    if (changedActions) {
      return true;
    }

    for (ActionMeta actionMeta : workflowActions) {
      if (actionMeta.hasChanged()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Have workflow hops changed.
   *
   * @return true, if successful
   */
  public boolean haveWorkflowHopsChanged() {
    if (changedHops) {
      return true;
    }

    // Look at all the hops
    for (WorkflowHopMeta hop : workflowHops) {
      if (hop.hasChanged()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets the version of the workflow.
   *
   * @return The version of the workflow
   */
  public String getWorkflowVersion() {
    return workflowVersion;
  }

  /**
   * Set the version of the workflow.
   *
   * @param jobVersion The new version description of the workflow
   */
  public void setWorkflowVersion(String jobVersion) {
    this.workflowVersion = jobVersion;
  }

  /**
   * Gets the status of the workflow.
   *
   * @return the status of the workflow
   */
  public int getWorkflowStatus() {
    return workflowStatus;
  }

  /**
   * Set the status of the workflow.
   *
   * @param status The new status description of the workflow
   */
  public void setWorkflowStatus(int status) {
    this.workflowStatus = status;
  }

  /** This method sets various internal hop variables that can be used by the pipeline. */
  @Override
  public void setInternalHopVariables(IVariables variables) {
    setInternalFilenameHopVariables(variables);
    setInternalNameHopVariable(variables);

    updateCurrentDir(variables);

    HopVersionProvider versionProvider = new HopVersionProvider();
    variables.setVariable(Const.HOP_VERSION, versionProvider.getVersion()[0]);
  }

  // changed to protected for testing purposes
  //
  protected void updateCurrentDir(IVariables variables) {
    String prevCurrentDir = variables.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER);
    String currentDir =
        variables.getVariable(
            filename != null
                ? Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER
                : Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER);
    variables.setVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, currentDir);
    fireCurrentDirectoryChanged(prevCurrentDir, currentDir);
  }

  /**
   * Sets the internal name hop variable.
   *
   * @param variables the new internal name hop variable
   */
  @Override
  protected void setInternalNameHopVariable(IVariables variables) {
    // The name of the workflow
    variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_NAME, Const.NVL(getName(), ""));
  }

  /**
   * Sets the internal filename hop variables.
   *
   * @param variables the new internal filename hop variables
   */
  @Override
  protected void setInternalFilenameHopVariables(IVariables variables) {
    if (filename != null) {
      // we have a filename that's defined.
      try {
        FileObject fileObject = HopVfs.getFileObject(filename);
        FileName fileName = fileObject.getName();

        // The filename of the workflow
        variables.setVariable(
            Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, fileName.getBaseName());

        // The directory of the workflow
        FileName fileDir = fileName.getParent();
        variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, fileDir.getURI());
      } catch (Exception e) {
        variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "");
        variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, "");
      }
    } else {
      variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER, "");
      variables.setVariable(Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_NAME, "");
    }

    setInternalEntryCurrentDirectory(variables);
  }

  protected void setInternalEntryCurrentDirectory(IVariables variables) {
    variables.setVariable(
        Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER,
        variables.getVariable(
            filename != null
                ? Const.INTERNAL_VARIABLE_WORKFLOW_FILENAME_FOLDER
                : Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER));
  }

  /**
   * Check all actions within the workflow. Each Workflow Action has the opportunity to check their
   * own settings.
   *
   * @param remarks List of CheckResult remarks inserted into by each Action
   * @param onlySelected true if you only want to check the selected actions
   * @param monitor Progress monitor (not presently in use)
   */
  public void checkActions(
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

      List<ActionMeta> actions = (onlySelected) ? this.getSelectedActions() : getActions();

      monitor.beginTask(
          BaseMessages.getString(PKG, "WorkflowMeta.Monitor.VerifyingThisActionTask.Title"),
          actions.size());

      // Check for the presence of a start action
      if (findStart() == null) {
        remarks.add(
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(
                    PKG, "WorkflowMeta.CheckResult.StartActionIsMissing.Description"),
                null));
      }

      int worked = 1;
      for (ActionMeta actionMeta : actions) {
        if (monitor.isCanceled()) {
          break;
        }

        IAction action = actionMeta.getAction();
        if (action != null) {
          monitor.subTask(
              BaseMessages.getString(
                  PKG, "WorkflowMeta.Monitor.VerifyingAction.Title", action.getName()));

          // Check missing action plugin
          if (action instanceof MissingAction missingAction) {
            remarks.add(
                new CheckResult(
                    ICheckResult.TYPE_RESULT_ERROR,
                    BaseMessages.getString(
                        PKG,
                        "WorkflowMeta.CheckResult.ActionPluginNotFound.Description",
                        missingAction.getMissingPluginId()),
                    action));
          }

          // Check deprecated action plugin
          if (actionMeta.isDeprecated()) {
            remarks.add(
                new CheckResult(
                    ICheckResult.TYPE_RESULT_WARNING,
                    BaseMessages.getString(
                        PKG, "WorkflowMeta.CheckResult.DeprecatedActionPlugin.Description"),
                    action));
          }

          if (isActionUsedInHops(actionMeta) || getActions().size() == 1) {
            action.check(remarks, this, variables, metadataProvider);
          } else {
            remarks.add(
                new CheckResult(
                    ICheckResult.TYPE_RESULT_WARNING,
                    BaseMessages.getString(
                        PKG, "WorkflowMeta.CheckResult.ActionIsNotUsed.Description"),
                    action));
          }
        }
        // Progress bar...
        monitor.worked(worked++);
      }

      monitor.done();
    } catch (Exception e) {
      throw new RuntimeException("Error checking workflow", e);
    }
  }

  /**
   * Gets the resource dependencies.
   *
   * @return the resource dependencies
   */
  public List<ResourceReference> getResourceDependencies(IVariables variables) {
    List<ResourceReference> resourceReferences = new ArrayList<>();
    for (ActionMeta actionMeta : workflowActions) {
      IAction action = actionMeta.getAction();
      resourceReferences.addAll(action.getResourceDependencies(variables, this));
    }

    return resourceReferences;
  }

  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming namingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    String resourceName = null;
    try {
      // Handle naming for XML bases resources...
      //
      String baseName;
      String originalPath;
      String fullname;
      String extension = "hwf";
      if (StringUtils.isNotEmpty(getFilename())) {
        FileObject fileObject = HopVfs.getFileObject(variables.resolve(getFilename()));
        originalPath = fileObject.getParent().getName().getPath();
        baseName = fileObject.getName().getBaseName();
        fullname = fileObject.getName().getPath();

        resourceName =
            namingInterface.nameResource(
                baseName, originalPath, extension, IResourceNaming.FileNamingType.WORKFLOW);
        ResourceDefinition definition = definitions.get(resourceName);
        if (definition == null) {
          // If we do this once, it will be plenty :-)
          //
          WorkflowMeta workflowMeta = (WorkflowMeta) this.realClone(false);

          // Set an appropriate name
          //
          workflowMeta.setNameSynchronizedWithFilename(false);
          workflowMeta.setName(getName());

          // Add used resources, modify pipelineMeta accordingly
          // Go through the list of transforms, etc.
          // These critters change the transforms in the cloned PipelineMeta
          // At the end we make a new XML version of it in "exported"
          // format...

          // loop over transforms, databases will be exported to XML anyway.
          //
          for (ActionMeta actionMeta : workflowMeta.workflowActions) {
            actionMeta
                .getAction()
                .exportResources(variables, definitions, namingInterface, metadataProvider);
          }

          // At the end, add ourselves to the map...
          //
          definition = new ResourceDefinition(resourceName, workflowMeta.getXml(variables));

          // Also remember the original filename (if any), including variables etc.
          //
          if (Utils.isEmpty(this.getFilename())) {
            definition.setOrigin(fullname);
          } else {
            definition.setOrigin(this.getFilename());
          }

          definitions.put(fullname, definition);
        }
      }
    } catch (FileSystemException | HopFileException e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "WorkflowMeta.Exception.AnErrorOccuredReadingWorkflow", getFilename()),
          e);
    }

    return resourceName;
  }

  /**
   * See if the name of the supplied action doesn't collide with any other action in the workflow.
   *
   * @param actionMeta The action copy to verify the name for.
   */
  public void renameActionIfNameCollides(ActionMeta actionMeta) {
    // First see if the name changed.
    // If so, we need to verify that the name is not already used in the
    // workflow.
    //
    String newname = actionMeta.getName();

    // See if this name exists in the other actions
    //
    boolean found;
    int nr = 1;
    do {
      found = false;
      for (ActionMeta action : workflowActions) {
        if (action != actionMeta && action.getName().equalsIgnoreCase(newname)) {
          found = true;
        }
      }
      if (found) {
        nr++;
        newname = actionMeta.getName() + " (" + nr + ")";
      }
    } while (found);

    // Rename if required.
    //
    actionMeta.setName(newname);
  }

  /**
   * Gets the workflow actions.
   *
   * @return the workflow actions
   */
  public List<ActionMeta> getActions() {
    return workflowActions;
  }

  /**
   * Gets the workflow hops.
   *
   * @return the workflow hops
   */
  public List<WorkflowHopMeta> getWorkflowHops() {
    return workflowHops;
  }

  /**
   * Create a unique list of action interfaces
   *
   * @return
   */
  public List<IAction> composeActionList() {
    List<IAction> list = new ArrayList<>();

    for (ActionMeta action : workflowActions) {
      if (!list.contains(action.getAction())) {
        list.add(action.getAction());
      }
    }

    return list;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getLogChannelId()
   */
  public String getLogChannelId() {
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hop.core.logging.ILoggingObject#getObjectType()
   */
  public LoggingObjectType getObjectType() {
    return LoggingObjectType.WORKFLOW_META;
  }

  public boolean containsAction(ActionMeta actionMeta) {
    return workflowActions.contains(actionMeta);
  }

  public List<MissingAction> getMissingActions() {
    return missingActions;
  }

  public void addMissingAction(MissingAction missingAction) {
    if (missingActions == null) {
      missingActions = new ArrayList<>();
    }
    missingActions.add(missingAction);
  }

  public void removeMissingAction(MissingAction missingAction) {
    if (missingActions != null && missingAction != null) {
      missingActions.remove(missingAction);
    }
  }

  @Override
  public boolean hasMissingPlugins() {
    return !Utils.isEmpty(missingActions);
  }

  public String getStartActionName() {
    return startActionName;
  }

  public void setStartActionName(String name) {
    this.startActionName = name;
  }

  public boolean isExpandingRemoteWorkflow() {
    return expandingRemoteWorkflow;
  }

  public void setExpandingRemoteWorkflow(boolean expandingRemoteWorkflow) {
    this.expandingRemoteWorkflow = expandingRemoteWorkflow;
  }

  public boolean isEmpty() {
    return nrActions() == 0 && nrNotes() == 0;
  }

  /**
   * Gets info
   *
   * @return value of info
   */
  public WorkflowMetaInfo getInfo() {
    return info;
  }

  /**
   * Sets info
   *
   * @param info value of info
   */
  public void setInfo(WorkflowMetaInfo info) {
    this.info = info;
  }

  /**
   * Get the name of the workflow. If the name is synchronized with the filename, we return the base
   * filename.
   *
   * @return The name of the workflow
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
   * Gets the user by whom the pipeline was created.
   *
   * @return the user by whom the pipeline was created.
   */
  @Override
  public String getCreatedUser() {
    return info.getCreatedUser();
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
   * Gets the date the pipeline was modified.
   *
   * @return the date the pipeline was modified.
   */
  @Override
  public Date getModifiedDate() {
    return info.getModifiedDate();
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
   * Gets the user who last modified the pipeline.
   *
   * @return the user who last modified the pipeline.
   */
  @Override
  public String getModifiedUser() {
    return info.getModifiedUser();
  }

  /**
   * Sets the user who last modified the pipeline.
   *
   * @param modifiedUser The user name to set.
   */
  @Override
  public void setModifiedUser(String modifiedUser) {
    info.setModifiedUser(modifiedUser);
  }
}
