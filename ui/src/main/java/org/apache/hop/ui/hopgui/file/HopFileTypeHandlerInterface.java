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
   * Redraw the file on the screen
   */
  public void redraw();
}
