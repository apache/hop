package org.apache.hop.ui.hopgui.file.workflow.delegates;

import org.apache.hop.core.gui.Point;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowMeta;

import java.io.File;

public class HopGuiWorkflowClipboardExtension {
  public String filename;
  public File file;
  public HopGuiWorkflowGraph workflowGraph;
  public WorkflowMeta workflowMeta;
  public HopGuiWorkflowClipboardDelegate workflowClipboardDelegate;
  public Point location;
}
