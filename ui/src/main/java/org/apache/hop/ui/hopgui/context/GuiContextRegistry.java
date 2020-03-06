package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.GuiAction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GuiContextRegistry {

  private static GuiContextRegistry registry;

  private Map<String, List<GuiContextHandler>> contextHandlerMap;

  private GuiContextRegistry() {
    contextHandlerMap = new HashMap<>();
  }

  public static final GuiContextRegistry getInstance() {
    if ( registry == null ) {
      registry = new GuiContextRegistry();
    }
    return registry;
  }

  public static void registerActionHandler(String guiId, GuiContextHandler handler) {
    GuiContextRegistry instance = getInstance();
    List<GuiContextHandler> handlers = instance.contextHandlerMap.get( guiId );
    if (handlers==null) {
      handlers = new ArrayList<>(  );
      instance.contextHandlerMap.put(guiId, handlers);
    }
    if (!handlers.contains( handler )) {
      handlers.add(handler);
    }
  }

  public static GuiContextHandler getActionHandler(String guiId, String actionId) {
    GuiContextRegistry instance = getInstance();
    List<GuiContextHandler> handlers = instance.contextHandlerMap.get( guiId );
    if (handlers==null) {
      return null;
    }
    for (GuiContextHandler handler : handlers) {
      List<GuiAction> actions = handler.getSupportedActions();
      for (GuiAction action : actions) {
        if (action.getId().equals( actionId )) {
          return handler;
        }
      }
    }
    return null;
  }
}
