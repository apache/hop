package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EmptyHopPerspective implements IHopPerspective {

  private HopFileTypeHandlerInterface emptyHandler;

  public EmptyHopPerspective() {
    emptyHandler = new EmptyHopFileTypeHandler();
  }

  @Override public HopFileTypeHandlerInterface getActiveFileTypeHandler() {
    return emptyHandler;
  }

  @Override public List<HopFileTypeInterface> getSupportedHopFileTypes() {
    return Collections.emptyList();
  }

  @Override public void show() {
  }

  @Override public void hide() {
  }

  @Override public void navigateToPreviousFile() {
  }

  @Override public void navigateToNextFile() {
  }


  @Override public boolean hasNavigationPreviousFile() {
    return false;
  }

  @Override public boolean hasNavigationNextFile() {
    return false;
  }


  @Override public boolean isActive() {
    return false;
  }

  @Override public void initialize( HopGui hopGui, Composite parent ) {
  }

  @Override public Composite getComposite() {
    return null;
  }

  @Override public FormData getFormData() {
    return null;
  }

  @Override public boolean remove( HopFileTypeHandlerInterface typeHandler ) {
    return true;
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }
}
