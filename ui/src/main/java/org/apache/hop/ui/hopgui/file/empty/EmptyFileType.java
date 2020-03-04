package org.apache.hop.ui.hopgui.file.empty;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EmptyFileType implements HopFileTypeInterface {
  @Override public String getName() {
    return null;
  }

  @Override public String[] getFilterExtensions() {
    return new String[ 0 ];
  }

  @Override public String[] getFilterNames() {
    return new String[ 0 ];
  }

  @Override public Properties getCapabilities() {
    return new Properties();
  }

  @Override public boolean hasCapability( String capability ) {
    return false;
  }

  @Override public HopFileTypeHandlerInterface openFile( HopGui hopGui, String filename, VariableSpace parentVariableSpace ) throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override public HopFileTypeHandlerInterface newFile( HopGui hopGui, VariableSpace parentVariableSpace ) throws HopException {
    return new EmptyHopFileTypeHandler();
  }

  @Override public boolean isHandledBy( String filename, boolean checkContent ) throws HopException {
    return false;
  }
  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }
}
