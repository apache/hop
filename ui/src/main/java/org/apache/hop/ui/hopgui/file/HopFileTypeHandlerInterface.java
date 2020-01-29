package org.apache.hop.ui.hopgui.file;

import org.apache.hop.core.exception.HopException;

/**
 * This describes the main file operations for a supported Hop file
 */
public interface HopFileTypeHandlerInterface {

  /**
   * Get a hold of the file type details
   * @return
   */
  public HopFileTypeInterface getFileType();

  /**
   * @return The filename of the hop file
   */
  public String getFilename();

  /**
   * Change the filename of the hop file
   * @param filename The new filename
   */
  public void setFilename(String filename);

  /**
   * Save the file without asking for a new filename
   */
  public void save() throws HopException;

  /**
   * Save the file after asking for a filename
   */
  public void saveAs(String filename) throws HopException;

  /**
   * Start execution
   */
  public void start() throws HopException;

  /**
   * Stop execution
   */
  public void stop() throws HopException;

  /**
   * Pause execution
   */
  public void pause() throws HopException;

  /**
   * Preview data
   */
  public void preview() throws HopException;

  /**
   * Print the file
   */
  public void print() throws HopException;

  /**
   * Refresh the graphical file representation after model changes
   */
  public void redraw();

  /**
   * Update the toolbar, menus and so on. This is needed after a file, context or capabilities changes
   */
  public void updateGui();

  /**
   * Perform any task needed to close a file, save it if needed
   * @return true if the file is ready to close. Return false if there was a problem saving or any other issue.
   */
  boolean isCloseable();

  /**
   * Actually close the file, remove it from the user interface.
   */
  void close();

  /**
   * See if there anything has been changed by the user
   * @return true if there were changes worth saving, false if nothing has been changed.
   */
  boolean hasChanged();

  void undo();
  void redo();
}
