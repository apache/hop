package org.apache.hop.ui.hopgui.file;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.ui.hopgui.context.IActionContextHandlersProvider;

/**
 * This describes the main file operations for a supported Hop file
 */
public interface HopFileTypeHandlerInterface extends IActionContextHandlersProvider {

  /**
   * Get a hold of the file type details
   * @return
   */
  HopFileTypeInterface getFileType();

  /**
   * @return The filename of the hop file
   */
  String getFilename();

  /**
   * Change the filename of the hop file
   * @param filename The new filename
   */
  void setFilename(String filename);

  /**
   * Save the file without asking for a new filename
   */
  void save() throws HopException;

  /**
   * Save the file after asking for a filename
   */
  void saveAs(String filename) throws HopException;

  /**
   * Start execution
   */
  void start();

  /**
   * Stop execution
   */
  void stop();

  /**
   * Pause execution
   */
  void pause();

  /**
   * Preview this file
   */
  void preview();

  /**
   * Debug the file
   */
  void debug();

  /**
   * Print the file
   */
  void print();

  /**
   * Refresh the graphical file representation after model changes
   */
  void redraw();

  /**
   * Update the toolbar, menus and so on. This is needed after a file, context or capabilities changes
   */
  void updateGui();

  /**
   * Select all items in the current files
   */
  void selectAll();

  /**
   * Unselect all items in the current file
   */
  void unselectAll();

  /**
   * Copy the selected items to the clipboard
   */
  void copySelectedToClipboard();

  /**
   * Cut the selected items to the clipboard
   */
  void cutSelectedToClipboard();

  /**
   * Delete the selected items
   */
  void deleteSelected();

  /**
   * Paste items from the clipboard
   */
  void pasteFromClipboard();

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

  /**
   * Undo a change to the file
   */
  void undo();

  /**
   * Redo a change to the file
   */
  void redo();
}
