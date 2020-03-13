package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.action.IGuiContext;

import java.util.List;

public interface IActionContextHandlersProvider {

  List<IGuiContextHandler> getContextHandlers();
}
