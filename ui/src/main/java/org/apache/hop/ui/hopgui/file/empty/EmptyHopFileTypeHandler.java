package org.apache.hop.ui.hopgui.file.empty;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;

import java.util.ArrayList;
import java.util.List;

public class EmptyHopFileTypeHandler implements HopFileTypeHandlerInterface {

  private HopFileTypeInterface emptyFileType;

  public EmptyHopFileTypeHandler() {
    emptyFileType = new EmptyFileType();
  }

  @Override public HopFileTypeInterface getFileType() {
    return emptyFileType;
  }

  @Override public String getFilename() {
    return null;
  }

  @Override public void setFilename( String filename ) {

  }

  @Override public void save() throws HopException {

  }

  @Override public void saveAs( String filename ) throws HopException {

  }

  @Override public void start() {

  }

  @Override public void stop() {

  }

  @Override public void pause() {

  }

  @Override public void preview() {

  }

  @Override public void debug() {

  }

  @Override public void print() {

  }

  @Override public void redraw() {

  }

  @Override public void updateGui() {

  }

  @Override public void selectAll() {

  }

  @Override public void unselectAll() {

  }

  @Override public void copySelectedToClipboard() {

  }

  @Override public void cutSelectedToClipboard() {

  }

  @Override public void deleteSelected() {

  }

  @Override public void pasteFromClipboard() {

  }

  @Override public boolean isCloseable() {
    return true;
  }

  @Override public void close() {

  }

  @Override public boolean hasChanged() {
    return false;
  }

  @Override public void undo() {

  }

  @Override public void redo() {

  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>(  );
    return handlers;
  }
}
