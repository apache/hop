package org.apache.hop.ui.hopgui.file;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class contains all the available Hop File types
 */
public class HopFileTypeRegistry {
  private static HopFileTypeRegistry hopFileTypeRegistry;

  private List<HopFileTypeInterface> hopFileTypes;

  private HopFileTypeRegistry() {
    hopFileTypes = new ArrayList<>();
  }

  public static final HopFileTypeRegistry getInstance() {
    if ( hopFileTypeRegistry ==null) {
      hopFileTypeRegistry = new HopFileTypeRegistry();
    }
    return hopFileTypeRegistry;
  }

  public List<HopFileTypeInterface> getFileTypes() {
    return hopFileTypes;
  }

  public void registerHopFile( HopFileTypeInterface hopFileTypeInterface ) {
    if (!hopFileTypes.contains( hopFileTypeInterface )) {
      hopFileTypes.add( hopFileTypeInterface );
    }
  }

  /**
   * This method first tries to find a HopFile by looking at the extension.
   * If none can be found the content is looked at by each HopFileTypeInterface
   * @param filename The filename to search with
   * @return The HopFileTypeInterface we can use to open the file itself.
   * @throws HopException
   */
  public HopFileTypeInterface findHopFileType( String filename) throws HopException {
    for ( HopFileTypeInterface hopFile : hopFileTypes ) {
      if (hopFile.isHandledBy( filename, false )) {
        return hopFile;
      }
    }
    for ( HopFileTypeInterface hopFile : hopFileTypes ) {
      if (hopFile.isHandledBy( filename, true )) {
        return hopFile;
      }
    }
    return null;
  }

  /**
   * Get All the filter extensions of all the HopFile plugins
   * @return all the file extensions
   */
  public String[] getFilterExtensions() {
    List<String> filterExtensions = new ArrayList<>(  );
    for ( HopFileTypeInterface hopFile : hopFileTypes ) {
      filterExtensions.addAll( Arrays.asList(hopFile.getFilterExtensions()));
    }
    return filterExtensions.toArray(new String[0]);
  }

  /**
   * Get All the filter names of all the HopFile plugins
   * @return all the file names
   */
  public String[] getFilterNames() {
    List<String> filterNames = new ArrayList<>(  );
    for ( HopFileTypeInterface hopFile : hopFileTypes ) {
      filterNames.addAll( Arrays.asList(hopFile.getFilterNames()));
    }
    return filterNames.toArray(new String[0]);
  }

  public List<String> getFileTypeNames() {
    List<String> names = new ArrayList<>(  );
    for (HopFileTypeInterface fileType : hopFileTypes) {
      names.add(fileType.getName());
    }
    return names;
  }

  public HopFileTypeInterface getFileTypeByName(String name) {
    if ( StringUtils.isEmpty(name)) {
      return null;
    }
    for (HopFileTypeInterface fileType : hopFileTypes) {
      if (fileType.getName().equalsIgnoreCase( name )) {
        return fileType;
      }
    }
    return null;
  }
}
