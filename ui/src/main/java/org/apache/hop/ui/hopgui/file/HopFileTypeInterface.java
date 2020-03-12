package org.apache.hop.ui.hopgui.file;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLInterface;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IActionContextHandlersProvider;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;

import java.util.List;
import java.util.Properties;

public interface HopFileTypeInterface<T extends XMLInterface> {

  public static final String CAPABILITY_NEW = "New";
  public static final String CAPABILITY_SAVE = "Save";
  public static final String CAPABILITY_START = "Start";
  public static final String CAPABILITY_CLOSE = "Close";
  public static final String CAPABILITY_STOP = "Stop";
  public static final String CAPABILITY_PAUSE = "Pause";
  public static final String CAPABILITY_PREVIEW = "Preview";
  public static final String CAPABILITY_DEBUG = "Debug";

  public static final String CAPABILITY_SELECT = "Select";
  public static final String CAPABILITY_COPY = "Copy";
  public static final String CAPABILITY_PASTE = "Paste";
  public static final String CAPABILITY_CUT = "Cut";
  public static final String CAPABILITY_DELETE = "Delete";

  public static final String CAPABILITY_FILE_HISTORY = "FileHistory";

  /**
   * @return The name of this file type
   */
  String getName();

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
   * Check to see if the capability is present
   * @param capability The capability to check
   * @return True if the capability is set to any non-null value
   */
  boolean hasCapability(String capability);

  /**
   * Load and display the file
   *
   * @param hopGui The hop GUI to reference
   * @param filename The filename to load
   * @param parentVariableSpace The parent variablespace to inherit from
   * @return The hop file handler
   */
  HopFileTypeHandlerInterface openFile( HopGui hopGui, String filename, VariableSpace parentVariableSpace ) throws HopException;

  HopFileTypeHandlerInterface newFile( HopGui hopGui, VariableSpace parentVariableSpace ) throws HopException;

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
  boolean isHandledBy(String filename, boolean checkContent) throws HopException;

  /**
   * @return A list of context handlers allowing you to see all the actions that can be taken with the current file type. (CRUD, ...)
   */
  List<IGuiContextHandler> getContextHandlers();
}
