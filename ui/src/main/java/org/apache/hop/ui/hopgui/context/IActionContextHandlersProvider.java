package org.apache.hop.ui.hopgui.context;

import java.util.List;

public interface IActionContextHandlersProvider {

  List<IGuiContextHandler> getContextHandlers();
}
