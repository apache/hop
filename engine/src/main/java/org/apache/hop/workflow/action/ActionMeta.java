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

package org.apache.hop.workflow.action;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.base.IBaseMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.IAttributes;
import org.apache.hop.core.attributes.AttributesUtil;
import org.apache.hop.core.changed.IChanged;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.gui.IGuiPosition;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHasName;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IMissingPlugin;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.copy.CopyContext;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.copy.DefaultActionCopyFactory;
import org.apache.hop.workflow.actions.missing.MissingAction;
import org.w3c.dom.Node;

/**
 * This class describes the fact that a single Action can be used multiple times in the same
 * Workflow. Therefore, it contains a link to an Action, a position, a number, etc.
 */
public class ActionMeta
    implements Cloneable, IGuiPosition, IChanged, IAttributes, IBaseMeta, IHasName {
  public static final String XML_TAG = "action";

  private static final String XML_ATTRIBUTE_WORKFLOW_ACTION_COPY = AttributesUtil.XML_TAG + "_hac";
  private static final String CONST_SPACE = "      ";

  /**
   * Tracks changes to the settings this wrapper owns - the position on the canvas - as opposed to
   * the settings an action dialog edits, which live on the {@link IAction} itself. Keeping the two
   * apart means a dialog's Cancel, which restores the action's flag as it was when the dialog
   * opened, cannot discard a change made on the canvas in the meantime. Action dialogs are not
   * modal, so that overlap is easy to hit. See issue #8022.
   */
  private boolean wrapperChanged;

  @HopMetadataProperty(inline = true)
  private IAction action;

  @HopMetadataProperty(inline = true)
  private Point location;

  /** Flag to indicate that the actions following this one are launched in parallel */
  @HopMetadataProperty(key = "parallel")
  private boolean launchingInParallel;

  /** protected for easy access by subclasses */
  @HopMetadataProperty(
      key = "group",
      groupKey = XML_ATTRIBUTE_WORKFLOW_ACTION_COPY,
      mapKeyWrapper = "name",
      mapValueWrapper = "attribute",
      mapValueClass = HashMap.class)
  private Map<String, Map<String, String>> attributesMap;

  private String suggestion = "";

  private boolean selected;

  private boolean isDeprecated;

  private WorkflowMeta parentWorkflowMeta;

  public ActionMeta() {
    clear();
  }

  public ActionMeta(IAction action) {
    this();
    setAction(action);
  }

  public String getXml() {
    StringBuilder body = new StringBuilder(100);

    action.setParentWorkflowMeta(
        parentWorkflowMeta); // Attempt to set the WorkflowMeta for entries that need it
    body.append(action.getXml());

    body.append(CONST_SPACE).append(XmlHandler.addTagValue("parallel", launchingInParallel));
    body.append(CONST_SPACE).append(XmlHandler.addTagValue("xloc", location.x));
    body.append(CONST_SPACE).append(XmlHandler.addTagValue("yloc", location.y));

    body.append(AttributesUtil.getAttributesXml(attributesMap, XML_ATTRIBUTE_WORKFLOW_ACTION_COPY));

    // The settings of an action whose plugin isn't installed are written back out untouched.
    //
    if (action instanceof IMissingPlugin missingPlugin) {
      try {
        body.append(XmlMetadataUtil.getPreservedMissingPluginXml(missingPlugin, body.toString()));
      } catch (HopException e) {
        throw new HopRuntimeException(
            "Unable to serialize the settings of missing plugin "
                + missingPlugin.getMissingPluginId(),
            e);
      }
    }

    StringBuilder xml = new StringBuilder(body.length() + 100);
    xml.append("    ").append(XmlHandler.openTag(XML_TAG)).append(Const.CR);
    xml.append(body);
    xml.append("    ").append(XmlHandler.closeTag(XML_TAG)).append(Const.CR);
    return xml.toString();
  }

  public ActionMeta(Node actionNode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      String pluginId = XmlHandler.getTagValue(actionNode, "type");
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin actionPlugin = registry.findPluginWithId(ActionPluginType.class, pluginId, true);
      if (actionPlugin == null) {
        // The plugin isn't installed: keep the XML of the action so that saving the workflow
        // doesn't throw away its configuration.
        //
        String name = XmlHandler.getTagValue(actionNode, "name");
        MissingAction missingAction = new MissingAction(name, pluginId);
        XmlMetadataUtil.preserveMissingPluginXml(missingAction, actionNode);
        // Keep pointing at the plugin which is missing so the action can be restored once it is
        // installed.
        //
        missingAction.setPluginId(pluginId);
        missingAction.setDescription(XmlHandler.getTagValue(actionNode, "description"));
        setAction(missingAction);
      } else {
        setAction(registry.loadClass(actionPlugin, IAction.class));
      }
      // Get an empty Action of the appropriate class...
      if (action != null) {
        if (actionPlugin != null) {
          action.setPluginId(actionPlugin.getIds()[0]);
          suggestion = Const.NVL(actionPlugin.getSuggestion(), "");
        }
        action.setMetadataProvider(metadataProvider); // inject metadata
        action.loadXml(actionNode, metadataProvider, variables);

        // Handle GUI information: location?
        setLaunchingInParallel(
            "Y".equalsIgnoreCase(XmlHandler.getTagValue(actionNode, "parallel")));
        int x = Const.toInt(XmlHandler.getTagValue(actionNode, "xloc"), 0);
        int y = Const.toInt(XmlHandler.getTagValue(actionNode, "yloc"), 0);
        setLocation(x, y);

        Node actionCopyAttributesNode =
            XmlHandler.getSubNode(actionNode, XML_ATTRIBUTE_WORKFLOW_ACTION_COPY);
        if (actionCopyAttributesNode != null) {
          attributesMap = AttributesUtil.loadAttributes(actionCopyAttributesNode);
        } else {
          // If the appropriate attributes node wasn't found, this must be an old file (prior to
          // this fix).
          // Before this fix it was very probable to exist two attributes groups. While this is not
          // very valid, in some
          // scenarios the Workflow worked as expected; so by trying to load the LAST one into the
          // ActionCopy, we
          // simulate that behaviour.
          attributesMap =
              AttributesUtil.loadAttributes(
                  XmlHandler.getLastSubNode(actionNode, AttributesUtil.XML_TAG));
        }
      }
    } catch (Throwable e) {
      String message = "Unable to read Workflow action copy info from XML node : " + e;
      throw new HopXmlException(message, e);
    }
  }

  public void clear() {
    location = new Point(0, 0);
    action = null;
    launchingInParallel = false;
    attributesMap = new HashMap<>();
  }

  @Override
  public ActionMeta clone() {
    ActionMeta ge = new ActionMeta();
    ge.replaceMeta(this);

    for (final Map.Entry<String, Map<String, String>> attribute : attributesMap.entrySet()) {
      ge.attributesMap.put(attribute.getKey(), attribute.getValue());
    }

    return ge;
  }

  public void replaceMeta(ActionMeta actionCopy) {
    // Use the copy factory with SAME_PIPELINE context to preserve parent references and proper
    // state
    action =
        DefaultActionCopyFactory.getInstance().copy(actionCopy.action, CopyContext.SAME_PIPELINE);
    selected = actionCopy.selected;
    if (actionCopy.location != null) {
      location = new Point(actionCopy.location.x, actionCopy.location.y);
    }
    launchingInParallel = actionCopy.launchingInParallel;

    setChanged();
  }

  public Object cloneDeep() {
    ActionMeta ge = clone();

    // Copy underlying object as well...
    ge.action = (IAction) action.clone();

    return ge;
  }

  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    ActionMeta je = (ActionMeta) o;
    return je.action.getName().equalsIgnoreCase(action.getName());
  }

  @Override
  public int hashCode() {
    return action.getName().hashCode();
  }

  public void setAction(IAction action) {
    this.action = action;
    if (action != null) {
      if (action.getPluginId() == null) {
        action.setPluginId(
            PluginRegistry.getInstance().getPluginId(ActionPluginType.class, action));
      }

      // Check if action is deprecated by annotation
      Deprecated deprecated = action.getClass().getDeclaredAnnotation(Deprecated.class);
      if (deprecated != null) {
        this.isDeprecated = true;
      }
    }
  }

  public IAction getAction() {
    return action;
  }

  /**
   * @return action in IAction.typeCode[] for native workflows, action.getTypeCode() for plugins
   */
  public String getTypeDesc() {
    IPlugin plugin =
        PluginRegistry.getInstance().findPluginWithId(ActionPluginType.class, action.getPluginId());
    // The plugin is not registered when the workflow references an action that is not installed
    return plugin == null ? action.getPluginId() : plugin.getDescription();
  }

  @Override
  public void setLocation(int x, int y) {
    int nx = (x >= 0 ? x : 0);
    int ny = (y >= 0 ? y : 0);

    Point loc = new Point(nx, ny);
    if (!loc.equals(location)) {
      setWrapperChanged();
    }
    location = loc;
  }

  @Override
  public void setLocation(Point loc) {
    if (loc != null && !loc.equals(location)) {
      setWrapperChanged();
    }
    location = loc;
  }

  @Override
  public Point getLocation() {
    return location;
  }

  @Override
  public void setChanged() {
    setChanged(true);
  }

  @SuppressWarnings("javabugs:S2259") // the action is set by every constructor
  @Override
  public void setChanged(boolean ch) {
    wrapperChanged = ch;
    action.setChanged(ch);
  }

  @Override
  public void clearChanged() {
    wrapperChanged = false;
    action.setChanged(false);
  }

  @Override
  public boolean hasChanged() {
    return wrapperChanged || action.hasChanged();
  }

  /**
   * Marks the settings owned by this wrapper as changed, without touching the action's own changed
   * flag. See {@link #wrapperChanged}.
   */
  private void setWrapperChanged() {
    wrapperChanged = true;
  }

  public void setLaunchingInParallel(boolean p) {
    launchingInParallel = p;
  }

  public boolean isLaunchingInParallel() {
    return launchingInParallel;
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

  public void setDescription(String description) {
    action.setDescription(description);
  }

  public String getDescription() {
    return action.getDescription();
  }

  public boolean isStart() {
    return action.isStart();
  }

  public boolean isJoin() {
    return action.isJoin();
  }

  public boolean isMissing() {
    return action instanceof MissingAction;
  }

  public boolean isPipeline() {
    return action.isPipeline();
  }

  public boolean isWorkflow() {
    return action.isWorkflow();
  }

  public boolean isEvaluation() {
    if (action != null) {
      return action.isEvaluation();
    }
    return false;
  }

  public boolean isUnconditional() {
    if (action != null) {
      return action.isUnconditional();
    }
    return true;
  }

  public String toString() {
    if (action != null) {
      return action.getName();
    } else {
      return "null";
    }
  }

  public String getName() {
    if (action != null) {
      return action.getName();
    } else {
      return "null";
    }
  }

  @SuppressWarnings("javabugs:S2259") // the action is set by every constructor
  public void setName(String name) {
    action.setName(name);
  }

  public boolean resetErrorsBeforeExecution() {
    return action.resetErrorsBeforeExecution();
  }

  public WorkflowMeta getParentWorkflowMeta() {
    return parentWorkflowMeta;
  }

  @SuppressWarnings("javabugs:S2259") // the action is set by every constructor
  public void setParentWorkflowMeta(WorkflowMeta parentWorkflowMeta) {
    this.parentWorkflowMeta = parentWorkflowMeta;
    this.action.setParentWorkflowMeta(parentWorkflowMeta);
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

  public boolean isDeprecated() {
    return isDeprecated;
  }

  public String getSuggestion() {
    return suggestion;
  }
}
