package org.apache.hop.ui.hopgui.perspective;

import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EmptyHopPerspective implements IHopPerspective {

  private IHopFileTypeHandler emptyHandler;

  public EmptyHopPerspective() {
    emptyHandler = new EmptyHopFileTypeHandler();
  }

  @Override public String getId() {
    return "empty";
  }

  @Override public IHopFileTypeHandler getActiveFileTypeHandler() {
    return emptyHandler;
  }

  @Override public List<IHopFileType> getSupportedHopFileTypes() {
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

  @Override public boolean remove( IHopFileTypeHandler typeHandler ) {
    return true;
  }

  @Override public List<TabItemHandler> getItems() {
    return null;
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }
}
