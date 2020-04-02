package org.apache.hop.ui.hopgui.file.job.context;

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionLambdaBuilder;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.JobEntryPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.job.JobMeta;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.job.HopGuiJobGraph;

import java.util.ArrayList;
import java.util.List;

public class HopGuiJobContext extends BaseGuiContextHandler implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiJobContext";

  private JobMeta jobMeta;
  private HopGuiJobGraph jobGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiJobContext> lambdaBuilder;

  public HopGuiJobContext( JobMeta jobMeta, HopGuiJobGraph jobGraph, Point click ) {
    this.jobMeta = jobMeta;
    this.jobGraph = jobGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }


  @Override public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a job.
   * We'll add the creation of every possible job entry as well as the modification of the job itself from the annotations.
   *
   * @return The list of supported actions
   */
  @Override public List<GuiAction> getSupportedActions() {
    List<GuiAction> actions = new ArrayList<>();

    // Get the actions from the plugins...
    //
    List<GuiAction> pluginActions = getPluginActions( true );
    if ( pluginActions != null ) {
      for ( GuiAction pluginAction : pluginActions ) {
        actions.add( lambdaBuilder.createLambda( pluginAction, jobGraph, this, jobGraph ) );
      }
    }

    // Add the special entries : Start and Dummy first
    //
    actions.add(createStartGuiAction());
    actions.add(createDummyGuiAction());

    // Also add all the entry creation actions...
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> entryPlugins = registry.getPlugins( JobEntryPluginType.class );
    for ( IPlugin entryPlugin : entryPlugins ) {
      if (!entryPlugin.getIds()[0].equals( JobMeta.STRING_SPECIAL )) {
        GuiAction createEntryAction =
          new GuiAction( "jobgraph-create-job-entry-" + entryPlugin.getIds()[ 0 ], GuiActionType.Create, entryPlugin.getName(), entryPlugin.getDescription(), entryPlugin.getImageFile(),
            (shiftClicked, controlClicked, t) -> {
              jobGraph.jobEntryDelegate.newJobEntry( jobMeta, entryPlugin.getIds()[ 0 ], entryPlugin.getName(), controlClicked, click );
            }
          );
      try {
        createEntryAction.setClassLoader( registry.getClassLoader( entryPlugin ) );
      } catch ( HopPluginException e ) {
        LogChannel.UI.logError( "Unable to get classloader for transform plugin " + entryPlugin.getIds()[ 0 ], e );
      }
      createEntryAction.getKeywords().add( entryPlugin.getCategory() );
      actions.add( createEntryAction );
      }
    }

    return actions;
  }

  private GuiAction createStartGuiAction() {
    return new GuiAction( "jobgraph-create-job-entry-start", GuiActionType.Create, JobMeta.STRING_SPECIAL_START, null, "ui/images/STR.svg",
      (shiftClicked, controlClicked, t) -> {
          jobGraph.jobEntryDelegate.newJobEntry( jobMeta, JobMeta.STRING_SPECIAL, JobMeta.STRING_SPECIAL_START, controlClicked, click );
        }
      );
  }

  private GuiAction createDummyGuiAction() {
    return new GuiAction( "jobgraph-create-job-entry-dummy", GuiActionType.Create, JobMeta.STRING_SPECIAL_DUMMY, null, "ui/images/DUM.svg",
      (shiftClicked, controlClicked, t) -> {
        jobGraph.jobEntryDelegate.newJobEntry( jobMeta, JobMeta.STRING_SPECIAL, JobMeta.STRING_SPECIAL_DUMMY, controlClicked, click );
      }
    );
  }

  /**
   * Gets jobMeta
   *
   * @return value of jobMeta
   */
  public JobMeta getJobMeta() {
    return jobMeta;
  }

  /**
   * @param jobMeta The jobMeta to set
   */
  public void setJobMeta( JobMeta jobMeta ) {
    this.jobMeta = jobMeta;
  }

  /**
   * Gets jobGraph
   *
   * @return value of jobGraph
   */
  public HopGuiJobGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The jobGraph to set
   */
  public void setJobGraph( HopGuiJobGraph jobGraph ) {
    this.jobGraph = jobGraph;
  }

  /**
   * Gets click
   *
   * @return value of click
   */
  public Point getClick() {
    return click;
  }

  /**
   * @param click The click to set
   */
  public void setClick( Point click ) {
    this.click = click;
  }

  /**
   * Gets lambdaBuilder
   *
   * @return value of lambdaBuilder
   */
  public GuiActionLambdaBuilder<HopGuiJobContext> getLambdaBuilder() {
    return lambdaBuilder;
  }

  /**
   * @param lambdaBuilder The lambdaBuilder to set
   */
  public void setLambdaBuilder( GuiActionLambdaBuilder<HopGuiJobContext> lambdaBuilder ) {
    this.lambdaBuilder = lambdaBuilder;
  }
}
