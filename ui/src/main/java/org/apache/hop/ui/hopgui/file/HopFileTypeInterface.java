package org.apache.hop.ui.hopgui.file;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLInterface;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;

import java.util.Properties;

public interface HopFileTypeInterface<T extends XMLInterface> {

  public static final String CAPABILITY_SAVE = "Save";
  public static final String CAPABILITY_SAVE_AS = "SaveAs";
  public static final String CAPABILITY_START = "Start";
  public static final String CAPABILITY_STOP = "Stop";
  public static final String CAPABILITY_PAUZE = "CanPause";
  public static final String CAPABILITY_PREVIEW = "CanPreview";

  /**
   * @return The file type extensions.
   */
  String[] getFilterExtensions();

  /**
   * @return The file names (matching the extensions)
   */
  String[] getFilterNames();

  /**
   * @return The capabilities of this file handler
   */
  Properties getCapabilities();

  /**
   * Allows you to specify which perspective is to be used to load handle this file
   * @return
   */
  Class<? extends IHopPerspective> getPerspectiveClass();

  /**
   * Load and display the file
   *
   * @param hopGui The hop GUI to reference
   * @param filename The filename to load
   * @param parentVariableSpace The parent variablespace to inherit from
   * @return The hop file handler
   */
  HopFileTypeHandlerInterface openFile( HopGui hopGui, String filename, VariableSpace parentVariableSpace ) throws HopException;

    /**
     * Look at the given file and see if it's handled by this type.
     * Usually this is done by simply looking at the file extension.
     * In rare cases we look at the content.
     *
     * @param filename The filename
     * @param checkContent True if we want to look inside the file content
     * @return true if this HopFile is handling the file
     * @throws HopException In case something goes wrong like: file doesn't exist, a permission problem, ...
     */
  public boolean isHandledBy(String filename, boolean checkContent) throws HopException;
}
