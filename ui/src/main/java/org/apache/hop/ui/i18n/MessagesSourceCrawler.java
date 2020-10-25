/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.i18n;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This class takes care of crawling through the source code
 *
 * @author matt
 */
public class MessagesSourceCrawler {

  private List<String> scanPhrases;

  /**
   * The source directories to crawl through
   */
  private List<String> sourceDirectories;

  /**
   * The bundle store
   */
  private BundlesStore bundlesStore;

  /**
   * Source folder - package name - all the key occurrences in there
   */
  private Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences;

  /**
   * The file names to avoid (base names)
   */
  private List<String> filesToAvoid;

  private Pattern packagePattern;
  private Pattern importPattern;
  private Pattern stringPkgPattern;
  private List<Pattern> classPkgPatterns;

  private ILogChannel log;

  public MessagesSourceCrawler() {
    this.scanPhrases = new ArrayList<>();
    this.sourceDirectories = new ArrayList<>();
    this.filesToAvoid = new ArrayList<>();
    this.sourcePackageOccurrences = new HashMap<>();

    packagePattern = Pattern.compile( "^\\s*package .*;[ \t]*$" );
    importPattern = Pattern.compile( "^\\s*import [a-z\\._0-9]*\\.[A-Z].*;[ \t]*$" );
    stringPkgPattern = Pattern.compile( "^.*private static String PKG.*=.*$" );
    classPkgPatterns = Arrays.asList(
      Pattern.compile( "^.*private static Class.*\\sPKG\\s*=.*$" ),
      Pattern.compile( "^.*private static final Class.*\\sPKG\\s*=.*$" ),
      Pattern.compile( "^.*public static Class.*\\sPKG\\s*=.*$" ),
      Pattern.compile( "^.*public static final Class.*\\sPKG\\s*=.*$" )
    );
  }

  public MessagesSourceCrawler( ILogChannel log, String rootFolder, BundlesStore bundlesStore ) throws HopException {
    this();
    this.log = log;
    this.bundlesStore = bundlesStore;
    this.filesToAvoid = new ArrayList<>();

    // Let's look for all the src/main/java folders in the root folder.
    //
    try {
      Files
        .walk( Paths.get( rootFolder ) )
        .filter( path ->
          Files.isDirectory( path ) &&
            path.endsWith( "src/main/java" ) &&
            !path.toString().contains( "archive" ) &&
            !path.toString().contains( "/impl/" )

        )
        .forEach( path -> sourceDirectories.add( path.toAbsolutePath().toFile().getPath() ) );
      ;
    } catch ( IOException e ) {
      throw new HopException( "Error scanning root folder '" + rootFolder + "' for Java source files (*.java)", e );
    }
  }

  /**
   * Add a key occurrence to the list of occurrences. The list is kept sorted on key and message package. If the key
   * already exists, we increment the number of occurrences.
   *
   * @param occ The key occurrence to add
   */
  public void addKeyOccurrence( KeyOccurrence occ ) {

    String sourceFolder = occ.getSourceFolder();
    if ( sourceFolder == null ) {
      throw new RuntimeException( "No source folder found for key: "
        + occ.getKey() + " in package " + occ.getMessagesPackage() );
    }
    String messagesPackage = occ.getMessagesPackage();

    // Do we have a map for the source folders?
    // If not, add one...
    //
    Map<String, List<KeyOccurrence>> packageOccurrences = sourcePackageOccurrences.get( sourceFolder );
    if ( packageOccurrences == null ) {
      packageOccurrences = new HashMap<String, List<KeyOccurrence>>();
      sourcePackageOccurrences.put( sourceFolder, packageOccurrences );
    }

    // Do we have a map entry for the occurrences list in the source folder?
    // If not, add a list for the messages package
    //
    List<KeyOccurrence> occurrences = packageOccurrences.get( messagesPackage );
    if ( occurrences == null ) {
      occurrences = new ArrayList<KeyOccurrence>();
      occurrences.add( occ );
      packageOccurrences.put( messagesPackage, occurrences );
    } else {
      int index = Collections.binarySearch( occurrences, occ );
      if ( index < 0 ) {
        // Add it to the list, keep it sorted...
        //
        occurrences.add( -index - 1, occ );
      }
    }
  }

  public void crawl() throws Exception {

    for ( final String sourceDirectory : sourceDirectories ) {
      FileObject folder = HopVfs.getFileObject( sourceDirectory );
      FileObject[] javaFiles = folder.findFiles( new FileSelector() {
        @Override
        public boolean traverseDescendents( FileSelectInfo info ) {
          return true;
        }

        @Override
        public boolean includeFile( FileSelectInfo info ) {
          return info.getFile().getName().getExtension().equals( "java" );
        }
      } );

      for ( FileObject javaFile : javaFiles ) {

        /**
         * We don't want certain files, there is nothing in there for us.
         */
        boolean skip = false;
        for ( String filename : filesToAvoid ) {
          if ( javaFile.getName().getBaseName().equals( filename ) ) {
            skip = true;
          }
        }
        if ( skip ) {
          continue; // don't process this file.
        }

        // For each of these files we look for keys...
        //
        lookForOccurrencesInFile( sourceDirectory, javaFile );
      }
    }
  }

  /**
   * Look for additional occurrences of keys in the specified file.
   *
   * @param sourceFolder The folder the java file and messages files live in
   * @param javaFile     The java source file to examine
   * @throws IOException In case there is a problem accessing the specified source file.
   */
  public void lookForOccurrencesInFile( String sourceFolder, FileObject javaFile ) throws IOException {

    if (javaFile.toString().contains( "HopGuiWorkflowClipboardDelegate" )) {
      System.out.println();
    }

    BufferedReader reader = new BufferedReader( new InputStreamReader( HopVfs.getInputStream( javaFile ) ) );

    String messagesPackage = null;
    int row = 0;
    String classPackage = null;

    Map<String, String> importedClasses = new Hashtable<String, String>(); // Remember the imports we do...

    String line = reader.readLine();
    while ( line != null ) {
      row++;
      String line2 = line;
      boolean extraLine;
      do {
        extraLine = false;
        for ( String joinPhrase : new String[] { "BaseMessages.getString(", "BaseMessages.getString( PKG," } ) {
          if ( line2.endsWith( joinPhrase ) ) {
            extraLine = true;
            break;
          }
        }
        if ( extraLine ) {
          line2 = reader.readLine();
          line += line2;
        }
      } while ( extraLine );

      // Examine the line...

      // What we first look for is the import of the messages package.
      //
      // "package org.apache.hop.pipeline.transforms.sortedmerge;"
      //
      if ( packagePattern.matcher( line ).matches() ) {
        int beginIndex = line.indexOf( "org.apache.hop." );
        int endIndex = line.indexOf( ';' );
        if ( beginIndex >= 0 && endIndex >= 0 ) {
          messagesPackage = line.substring( beginIndex, endIndex ); // this is the default
          classPackage = messagesPackage;
        }
      }

      // Remember all the imports...
      //
      if ( importPattern.matcher( line ).matches() ) {
        int beginIndex = line.indexOf( "import" ) + "import".length() + 1;
        int endIndex = line.indexOf( ";", beginIndex );
        String expression = line.substring( beginIndex, endIndex );
        // The last word is the Class imported...
        // If it's * we ignore it.
        //
        int lastDotIndex = expression.lastIndexOf( '.' );
        if ( lastDotIndex > 0 ) {
          String packageName = expression.substring( 0, lastDotIndex );
          String className = expression.substring( lastDotIndex + 1 );
          if ( !"*".equals( className ) ) {
            importedClasses.put( className, packageName );
          }
        }
      }


      // Look for the value of the PKG value...
      //
      // private static String PKG = "org.apache.hop.foo.bar.somepkg";
      //
      if ( stringPkgPattern.matcher( line ).matches() ) {
        int beginIndex = line.indexOf( '"' ) + 1;
        int endIndex = line.indexOf( '"', beginIndex );
        messagesPackage = line.substring( beginIndex, endIndex );
      }

      // Look for the value of the PKG value as a fully qualified class...
      //
      // private static final Class<?> PKG = Abort.class;
      //
      if ( classPackage != null ) {
        for (Pattern classPkgPattern : classPkgPatterns) {
          if (classPkgPattern.matcher( line ).matches()) {
            int fromIndex = line.indexOf( '=' ) + 1;
            int toIndex = line.indexOf( ".class", fromIndex );
            String expression = Const.trim( line.substring( fromIndex, toIndex ) );

            // If the expression doesn't contain any package, we'll look up the package in the imports. If not found there,
            // it's a local package.
            //
            if ( expression.contains( "." ) ) {
              int lastDotIndex = expression.lastIndexOf( '.' );
              messagesPackage = expression.substring( 0, lastDotIndex );
            } else {
              String packageName = importedClasses.get( expression );
              if ( packageName == null ) {
                messagesPackage = classPackage; // Local package
              } else {
                messagesPackage = packageName; // imported
              }
            }
          }
        }

      }

      // Now look for occurrences of "Messages.getString(", "BaseMessages.getString(PKG", ...
      //
      for ( String scanPhrase : scanPhrases ) {
        int index = line.indexOf( scanPhrase );
        while ( index >= 0 ) {
          // see if there's a character [a-z][A-Z] before the search string...
          // Otherwise we're looking at BaseMessages.getString(), etc.
          //
          if ( index == 0 || ( index > 0 & !Character.isJavaIdentifierPart( line.charAt( index - 1 ) ) ) ) {
            addLineOccurrence( sourceFolder, javaFile, messagesPackage, line, row, index, scanPhrase );
          }
          index = line.indexOf( scanPhrase, index + 1 );
        }
      }

      line = reader.readLine();
    }

    reader.close();
  }

  /**
   * Extract the needed information from the line and the index on which Messages.getString() occurs.
   *
   * @param sourceFolder    The source folder the messages and java files live in
   * @param fileObject      the file we're reading
   * @param messagesPackage the messages package
   * @param line            the line
   * @param row             the row number
   * @param index           the index in the line on which "Messages.getString(" is located.
   */
  private void addLineOccurrence( String sourceFolder, FileObject fileObject, String messagesPackage, String line,
                                  int row, int index, String scanPhrase ) {
    // Right after the "Messages.getString(" string is the key, quoted (")
    // until the next comma...
    //
    int column = index + scanPhrase.length();
    String arguments = "";

    // we start at the double quote...
    //
    int startKeyIndex = line.indexOf( '"', column ) + 1;
    int endKeyIndex = line.indexOf( '"', startKeyIndex );

    String key;
    if ( endKeyIndex >= 0 ) {
      key = line.substring( startKeyIndex, endKeyIndex );

      // Can we also determine the arguments?
      // No, not always: only if the arguments are all on the same line.
      //

      // Look for the next closing bracket...
      //
      int bracketIndex = endKeyIndex;
      int nrOpen = 1;
      while ( bracketIndex < line.length() && nrOpen != 0 ) {
        int c = line.charAt( bracketIndex );
        if ( c == '(' ) {
          nrOpen++;
        }
        if ( c == ')' ) {
          nrOpen--;
        }
        bracketIndex++;
      }

      if ( bracketIndex + 1 < line.length() ) {
        arguments = line.substring( endKeyIndex + 1, bracketIndex );
      } else {
        arguments = line.substring( endKeyIndex + 1 );
      }

    } else {
      key = line.substring( startKeyIndex );
    }

    // Sanity check...
    //
    if ( key.contains( "\t" ) || key.contains( " " ) ) {
      System.out.println( "Suspect key found: [" + key + "] in file [" + fileObject + "]" );
    }

    // OK, add the occurrence to the list...
    //
    // Make sure we pass the System key occurrences to the correct package.
    //
    if ( key.startsWith( "System." ) ) {
      String i18nPackage = BaseMessages.class.getPackage().getName();
      KeyOccurrence keyOccurrence = new KeyOccurrence( fileObject, sourceFolder, i18nPackage, row, column, key, arguments, line );

      // If we just add this key, we'll get doubles in the i18n package
      //
      KeyOccurrence lookup = getKeyOccurrence( key, i18nPackage );
      if ( lookup == null ) {
        addKeyOccurrence( keyOccurrence );
      } else {
        // Adjust the line of code...
        //
        lookup.setSourceLine( lookup.getSourceLine() + Const.CR + keyOccurrence.getSourceLine() );
        lookup.incrementOccurrences();
      }
    } else {
      if (messagesPackage==null) {
        log.logError( "Could not calculate messages package in file: "+fileObject );
      } else {
        KeyOccurrence keyOccurrence = new KeyOccurrence( fileObject, sourceFolder, messagesPackage, row, column, key, arguments, line );
        addKeyOccurrence( keyOccurrence );
      }
    }
  }

  /**
   * @return A sorted list of distinct occurrences of the used message package names
   */
  public List<String> getMessagesPackagesList( String sourceFolder ) {
    Map<String, List<KeyOccurrence>> packageOccurrences = sourcePackageOccurrences.get( sourceFolder );
    List<String> list = new ArrayList<>( packageOccurrences.keySet() );
    Collections.sort( list );
    return list;
  }

  /**
   * Get all the key occurrences for a certain messages package.
   *
   * @param messagesPackage the package to hunt for
   * @return all the key occurrences for a certain messages package.
   */
  public List<KeyOccurrence> getOccurrencesForPackage( String messagesPackage ) {
    List<KeyOccurrence> list = new ArrayList<KeyOccurrence>();

    for ( String sourceFolder : sourcePackageOccurrences.keySet() ) {
      Map<String, List<KeyOccurrence>> po = sourcePackageOccurrences.get( sourceFolder );
      List<KeyOccurrence> occurrences = po.get( messagesPackage );
      if ( occurrences != null ) {
        list.addAll( occurrences );
      }
    }
    return list;
  }

  public KeyOccurrence getKeyOccurrence( String key, String selectedMessagesPackage ) {
    for ( String sourceFolder : sourcePackageOccurrences.keySet() ) {
      Map<String, List<KeyOccurrence>> po = sourcePackageOccurrences.get( sourceFolder );
      if ( po != null ) {
        List<KeyOccurrence> occurrences = po.get( selectedMessagesPackage );
        if ( occurrences != null ) {
          for ( KeyOccurrence keyOccurrence : occurrences ) {
            if ( keyOccurrence.getKey().equals( key )
              && keyOccurrence.getMessagesPackage().equals( selectedMessagesPackage ) ) {
              return keyOccurrence;
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * Get the unique package-key
   *
   * @param sourceFolder
   */
  public List<KeyOccurrence> getKeyOccurrences( String sourceFolder ) {
    Map<String, KeyOccurrence> map = new HashMap<String, KeyOccurrence>();
    Map<String, List<KeyOccurrence>> po = sourcePackageOccurrences.get( sourceFolder );
    if ( po != null ) {
      for ( List<KeyOccurrence> keyOccurrences : po.values() ) {
        for ( KeyOccurrence keyOccurrence : keyOccurrences ) {
          String key = keyOccurrence.getMessagesPackage() + " - " + keyOccurrence.getKey();
          map.put( key, keyOccurrence );
        }
      }
    }

    return new ArrayList<KeyOccurrence>( map.values() );
  }


  /**
   * Gets scanPhrases
   *
   * @return value of scanPhrases
   */
  public List<String> getScanPhrases() {
    return scanPhrases;
  }

  /**
   * @param scanPhrases The scanPhrases to set
   */
  public void setScanPhrases( List<String> scanPhrases ) {
    this.scanPhrases = scanPhrases;
  }

  /**
   * Gets sourceDirectories
   *
   * @return value of sourceDirectories
   */
  public List<String> getSourceDirectories() {
    return sourceDirectories;
  }

  /**
   * @param sourceDirectories The sourceDirectories to set
   */
  public void setSourceDirectories( List<String> sourceDirectories ) {
    this.sourceDirectories = sourceDirectories;
  }

  /**
   * Gets bundlesStore
   *
   * @return value of bundlesStore
   */
  public BundlesStore getBundlesStore() {
    return bundlesStore;
  }

  /**
   * @param bundlesStore The bundlesStore to set
   */
  public void setBundlesStore( BundlesStore bundlesStore ) {
    this.bundlesStore = bundlesStore;
  }

  /**
   * Gets sourcePackageOccurrences
   *
   * @return value of sourcePackageOccurrences
   */
  public Map<String, Map<String, List<KeyOccurrence>>> getSourcePackageOccurrences() {
    return sourcePackageOccurrences;
  }

  /**
   * @param sourcePackageOccurrences The sourcePackageOccurrences to set
   */
  public void setSourcePackageOccurrences( Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences ) {
    this.sourcePackageOccurrences = sourcePackageOccurrences;
  }

  /**
   * Gets filesToAvoid
   *
   * @return value of filesToAvoid
   */
  public List<String> getFilesToAvoid() {
    return filesToAvoid;
  }

  /**
   * @param filesToAvoid The filesToAvoid to set
   */
  public void setFilesToAvoid( List<String> filesToAvoid ) {
    this.filesToAvoid = filesToAvoid;
  }

  /**
   * Gets packagePattern
   *
   * @return value of packagePattern
   */
  public Pattern getPackagePattern() {
    return packagePattern;
  }

  /**
   * @param packagePattern The packagePattern to set
   */
  public void setPackagePattern( Pattern packagePattern ) {
    this.packagePattern = packagePattern;
  }

  /**
   * Gets importPattern
   *
   * @return value of importPattern
   */
  public Pattern getImportPattern() {
    return importPattern;
  }

  /**
   * @param importPattern The importPattern to set
   */
  public void setImportPattern( Pattern importPattern ) {
    this.importPattern = importPattern;
  }

  /**
   * Gets stringPkgPattern
   *
   * @return value of stringPkgPattern
   */
  public Pattern getStringPkgPattern() {
    return stringPkgPattern;
  }

  /**
   * @param stringPkgPattern The stringPkgPattern to set
   */
  public void setStringPkgPattern( Pattern stringPkgPattern ) {
    this.stringPkgPattern = stringPkgPattern;
  }

  /**
   * Gets classPkgPatterns
   *
   * @return value of classPkgPatterns
   */
  public List<Pattern> getClassPkgPatterns() {
    return classPkgPatterns;
  }

  /**
   * @param classPkgPatterns The classPkgPatterns to set
   */
  public void setClassPkgPatterns( List<Pattern> classPkgPatterns ) {
    this.classPkgPatterns = classPkgPatterns;
  }

  /**
   * Gets log
   *
   * @return value of log
   */
  public ILogChannel getLog() {
    return log;
  }

  /**
   * @param log The log to set
   */
  public void setLog( ILogChannel log ) {
    this.log = log;
  }
}
