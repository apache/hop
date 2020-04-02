package org.apache.hop.pipeline.engines.local;

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;

@GuiPlugin
public class LocalPipelineRunConfiguration extends EmptyPipelineRunConfiguration implements IPipelineEngineRunConfiguration {

  @GuiWidgetElement(
    id = "rowSetSize",
    order = "10",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.RowSetSize.Label"
  )
  @MetaStoreAttribute(key="rowset_size")
  protected String rowSetSize;

  @GuiWidgetElement(
    id = "safeModeEnabled",
    order = "20",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.SafeModeEnabled.Label"
  )
  @MetaStoreAttribute(key="safe_mode")
  protected boolean safeModeEnabled;

  @GuiWidgetElement(
    id = "gatheringMetrics",
    order = "30",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.GatheringMetrics.Label"
  )
  @MetaStoreAttribute(key="gather_metrics")
  protected boolean gatheringMetrics;

  @GuiWidgetElement(
    id = "sortTransformsTopologically",
    order = "40",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.SortTransformsTopologically.Label"
  )
  @MetaStoreAttribute(key="topo_sort")
  protected boolean sortingTransformsTopologically;

  /**
   * Whether the feedback is shown.
   */
  @GuiWidgetElement(
    id = "feedbackShown",
    order = "50",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.FeedbackShown.Label"
  )
  @MetaStoreAttribute(key="show_feedback")
  protected boolean feedbackShown;

  /**
   * The feedback size.
   */
  @GuiWidgetElement(
    id = "feedbackSize",
    order = "60",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    i18nPackage = "org.apache.hop.ui.pipeline.config",
    label = "PipelineRunConfigurationDialog.FeedbackSize.Label"
  )
  @MetaStoreAttribute(key="feedback_size")
  protected String feedbackSize;


  public LocalPipelineRunConfiguration() {
    super();
    this.rowSetSize = Integer.toString( Const.ROWS_IN_ROWSET );
    this.feedbackShown = false;
    this.feedbackSize = Integer.toString( Const.ROWS_UPDATE );
  }

  public LocalPipelineRunConfiguration( String pluginId, String pluginName, String rowSetSize ) {
    super( pluginId, pluginName );
    this.rowSetSize = rowSetSize;
  }

  public LocalPipelineRunConfiguration( LocalPipelineRunConfiguration config ) {
    super( config );
    this.rowSetSize = config.rowSetSize;
    this.feedbackShown = config.feedbackShown;
    this.feedbackSize = config.feedbackSize;
    this.safeModeEnabled = config.safeModeEnabled;
    this.gatheringMetrics = config.gatheringMetrics;
    this.sortingTransformsTopologically = config.sortingTransformsTopologically;
  }

  public LocalPipelineRunConfiguration clone() {
    return new LocalPipelineRunConfiguration( this );
  }

  /**
   * Gets rowSetSize
   *
   * @return value of rowSetSize
   */
  public String getRowSetSize() {
    return rowSetSize;
  }

  /**
   * @param rowSetSize The rowSetSize to set
   */
  public void setRowSetSize( String rowSetSize ) {
    this.rowSetSize = rowSetSize;
  }

  /**
   * Gets safeModeEnabled
   *
   * @return value of safeModeEnabled
   */
  public boolean isSafeModeEnabled() {
    return safeModeEnabled;
  }

  /**
   * @param safeModeEnabled The safeModeEnabled to set
   */
  public void setSafeModeEnabled( boolean safeModeEnabled ) {
    this.safeModeEnabled = safeModeEnabled;
  }

  /**
   * Gets gatheringMetrics
   *
   * @return value of gatheringMetrics
   */
  public boolean isGatheringMetrics() {
    return gatheringMetrics;
  }

  /**
   * @param gatheringMetrics The gatheringMetrics to set
   */
  public void setGatheringMetrics( boolean gatheringMetrics ) {
    this.gatheringMetrics = gatheringMetrics;
  }

  /**
   * Gets sortingTransformsTopologically
   *
   * @return value of sortingTransformsTopologically
   */
  public boolean isSortingTransformsTopologically() {
    return sortingTransformsTopologically;
  }

  /**
   * @param sortingTransformsTopologically The sortingTransformsTopologically to set
   */
  public void setSortingTransformsTopologically( boolean sortingTransformsTopologically ) {
    this.sortingTransformsTopologically = sortingTransformsTopologically;
  }

  /**
   * Gets feedbackShown
   *
   * @return value of feedbackShown
   */
  public boolean isFeedbackShown() {
    return feedbackShown;
  }

  /**
   * @param feedbackShown The feedbackShown to set
   */
  public void setFeedbackShown( boolean feedbackShown ) {
    this.feedbackShown = feedbackShown;
  }

  /**
   * Gets feedbackSize
   *
   * @return value of feedbackSize
   */
  public String getFeedbackSize() {
    return feedbackSize;
  }

  /**
   * @param feedbackSize The feedbackSize to set
   */
  public void setFeedbackSize( String feedbackSize ) {
    this.feedbackSize = feedbackSize;
  }
}
