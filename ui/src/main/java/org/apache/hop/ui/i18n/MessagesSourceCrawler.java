/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.i18n;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class takes care of crawling through the source code
 *
 * @author matt
 */
public class MessagesSourceCrawler {

  /** The Hop source code */
  private String rootFolder;

  /** The source directories to crawl through */
  private List<String> sourceDirectories;

  /** The bundle store */
  private BundlesStore bundlesStore;

  /** Source folder - package name - all the key occurrences in there */
  private Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences;

  /** The file names to avoid (base names) */
  private List<String> filesToAvoid;

  private Pattern packagePattern;
  private Pattern importPattern;
  private Pattern stringPkgPattern;
  private List<Pattern> classPkgPatterns;
  private Pattern scanPhrasePattern;
  private Pattern doubleQuotePattern;
  private Pattern i18nStringPattern;

  private ILogChannel log;

  public MessagesSourceCrawler() {
    this.sourceDirectories = new ArrayList<>();
    this.filesToAvoid = new ArrayList<>();
    this.sourcePackageOccurrences = new HashMap<>();

    packagePattern =
        Pattern.compile("^package [a-z\\.0-9A-Z]*;$", Pattern.DOTALL | Pattern.MULTILINE);
    importPattern =
        Pattern.compile("^import [a-z\\._0-9A-Z]*;$", Pattern.DOTALL | Pattern.MULTILINE);
    stringPkgPattern =
        Pattern.compile("private static String PKG.*=.*$", Pattern.DOTALL | Pattern.MULTILINE);
    classPkgPatterns =
        Arrays.asList(
            Pattern.compile(
                "private static Class.*\\sPKG\\s*=.*\\.class;", Pattern.DOTALL | Pattern.MULTILINE),
            Pattern.compile(
                "private static final Class.*\\sPKG\\s*=.*\\.class;",
                Pattern.DOTALL | Pattern.MULTILINE),
            Pattern.compile(
                "public static Class.*\\sPKG\\s*=.*\\.class;", Pattern.DOTALL | Pattern.MULTILINE),
            Pattern.compile(
                "public static final Class.*\\sPKG\\s*=.*\\.class;",
                Pattern.DOTALL | Pattern.MULTILINE));

    scanPhrasePattern =
        Pattern.compile(
            "BaseMessages\\s*.getString\\(\\s*PKG,.*\\);", Pattern.DOTALL | Pattern.MULTILINE);
    doubleQuotePattern = Pattern.compile("\"", Pattern.MULTILINE);
    i18nStringPattern = Pattern.compile( "\""+Const.I18N_PREFIX+"[a-z\\.0-9]*:[a-zA-Z0-9\\.]*\"", Pattern.DOTALL | Pattern.MULTILINE );
  }

  public MessagesSourceCrawler(ILogChannel log, String rootFolder, BundlesStore bundlesStore)
      throws HopException {
    this();
    this.log = log;
    this.rootFolder = rootFolder;
    this.bundlesStore = bundlesStore;
    this.filesToAvoid = new ArrayList<>();

    // Let's look for all the src/main/java folders in the root folder.
    //
    try {
      Files.walk(Paths.get(rootFolder))
          .filter(
              path ->
                  Files.isDirectory(path)
                      && path.endsWith("src/main/java")
                      && !path.toString().contains("archive")
                      && !path.toString().contains("/impl/"))
          .forEach(path -> sourceDirectories.add(path.toAbsolutePath().toFile().getPath()));
      ;
    } catch (IOException e) {
      throw new HopException(
          "Error scanning root folder '" + rootFolder + "' for Java source files (*.java)", e);
    }
  }

  /**
   * Add a key occurrence to the list of occurrences. The list is kept sorted on key and message
   * package. If the key already exists, we increment the number of occurrences.
   *
   * @param occ The key occurrence to add
   */
  public void addKeyOccurrence(KeyOccurrence occ) {

    String sourceFolder = occ.getSourceFolder();
    if (sourceFolder == null) {
      throw new RuntimeException(
          "No source folder found for key: "
              + occ.getKey()
              + " in package "
              + occ.getMessagesPackage());
    }
    String messagesPackage = occ.getMessagesPackage();

    // Do we have a map for the source folders?
    // If not, add one...
    //
    Map<String, List<KeyOccurrence>> packageOccurrences =
        sourcePackageOccurrences.get(sourceFolder);
    if (packageOccurrences == null) {
      packageOccurrences = new HashMap<>();
      sourcePackageOccurrences.put(sourceFolder, packageOccurrences);
    }

    // Do we have a map entry for the occurrences list in the source folder?
    // If not, add a list for the messages package
    //
    List<KeyOccurrence> occurrences = packageOccurrences.get(messagesPackage);
    if (occurrences == null) {
      occurrences = new ArrayList<>();
      occurrences.add(occ);
      packageOccurrences.put(messagesPackage, occurrences);
    } else {
      int index = Collections.binarySearch(occurrences, occ);
      if (index < 0) {
        // Add it to the list, keep it sorted...
        //
        occurrences.add(-index - 1, occ);
      }
    }
  }

  public void crawl() throws Exception {

    for (final String sourceDirectory : sourceDirectories) {
      FileObject folder = HopVfs.getFileObject(sourceDirectory);
      FileObject[] javaFiles =
          folder.findFiles(
              new FileSelector() {
                @Override
                public boolean traverseDescendents(FileSelectInfo info) {
                  return true;
                }

                @Override
                public boolean includeFile(FileSelectInfo info) {
                  return info.getFile().getName().getExtension().equals("java");
                }
              });

      for (FileObject javaFile : javaFiles) {

        /** We don't want certain files, there is nothing in there for us. */
        boolean skip = false;
        for (String filename : filesToAvoid) {
          if (javaFile.getName().getBaseName().equals(filename)) {
            skip = true;
          }
        }
        if (skip) {
          continue; // don't process this file.
        }

        // For each of these files we look for keys...
        //
        lookForOccurrencesInFile(sourceDirectory, javaFile);
      }
    }
  }

  /**
   * Look for additional occurrences of keys in the specified file.
   *
   * @param sourceFolder The folder the java file and messages files live in
   * @param javaFile The java source file to examine
   * @throws IOException In case there is a problem accessing the specified source file.
   */
  public void lookForOccurrencesInFile(String sourceFolder, FileObject javaFile)
      throws IOException {

    String filename = HopVfs.getFilename(javaFile);
    Path path = new File(filename).toPath();
    String javaCode = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);

    // Let's figure out where we can find the messages bundle, which package we're working in and so
    // on
    //
    Map<String, String> importsMap = findImportOccurrences(javaCode);
    String messagesPackage = findPackage(javaCode);
    String classPackage = messagesPackage;

    String specificMessagesStringPackage = findSpecificStringPackage(javaCode);
    if (specificMessagesStringPackage != null) {
      messagesPackage = specificMessagesStringPackage;
    }
    String specificMessagesClassPackage =
        findSpecificClassPackage(javaCode, importsMap, classPackage);
    if (specificMessagesClassPackage != null) {
      messagesPackage = specificMessagesClassPackage;
    }

    // Now we can look for occurrences like BaseMessages.getString()
    // They are defined in the translator.xml file
    //
    Matcher scanPhraseMatcher = scanPhrasePattern.matcher(javaCode);
    int startIndex = 0;
    while (scanPhraseMatcher.find(startIndex)) {
      // We have a match
      //
      String expression = javaCode.substring(scanPhraseMatcher.start());

      // see if there's a character [a-z][A-Z] before the search string...
      // Otherwise we're looking at BaseMessages.getString(), etc.
      //
      if (scanPhraseMatcher.start() > 0
          && !Character.isJavaIdentifierPart(javaCode.charAt(scanPhraseMatcher.start() - 1))) {
        addKeyOccurrence(sourceFolder, javaFile, messagesPackage, expression, scanPhraseMatcher.start());
      }

      startIndex = scanPhraseMatcher.start() + 1;
    }

    // Also look for "i18n:package:key" Strings as used in annotations...
    //
    Matcher i18StringMatcher = i18nStringPattern.matcher( javaCode );
    startIndex = 0;
    while (i18StringMatcher.find(startIndex)) {
      // We found something like:
      //
      //    "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform"
      //
      String expression = javaCode.substring(i18StringMatcher.start()+1, i18StringMatcher.end()-1);

      String[] i18n = expression.split( ":" );
      if (i18n.length == 3) {
        String i18nPackage = i18n[1];
        if ( StringUtils.isEmpty(i18nPackage)) {
          i18nPackage = classPackage;
        }
        String i18nKey = i18n[2];

        String actualSourceFolder = sourceFolder;
        // If we have an explicit package in the i18n:package:key expression we assume
        // that the reference is to somewhere else in the source code
        // To cover this scenario we assume a root source tree
        //
        if (StringUtils.isNotEmpty( i18n[2] )) {
          actualSourceFolder = rootFolder;
        }

        KeyOccurrence keyOccurrence = new KeyOccurrence(
            javaFile, actualSourceFolder, i18nPackage, i18StringMatcher.start(), i18nKey, "", expression);
        addKeyOccurrence(keyOccurrence);
      }
      startIndex=i18StringMatcher.start()+1;
    }
  }

  private String findSpecificClassPackage(
      String javaCode, Map<String, String> importedClasses, String classPackage) {
    for (Pattern classPkgPattern : classPkgPatterns) {
      Matcher matcher = classPkgPattern.matcher(javaCode);
      if (matcher.find()) {
        String matchedString = javaCode.substring(matcher.start(), matcher.end());

        int fromIndex = matchedString.indexOf('=') + 1;
        int toIndex = matchedString.indexOf(".class", fromIndex);
        if (fromIndex > 0 && toIndex > 0) {
          String expression = Const.trim(matchedString.substring(fromIndex, toIndex));

          // If the expression doesn't contain any package, we'll look up the package in the
          // imports. If not found there,
          // it's a local package.
          //
          if (expression.contains(".")) {
            int lastDotIndex = expression.lastIndexOf('.');
            return expression.substring(0, lastDotIndex);
          } else {
            String packageName = importedClasses.get(expression);
            if (packageName == null) {
              return classPackage; // imported
            } else {
              return packageName;
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * Look for the value of the PKG value...
   *
   * <p>private static String PKG = "org.apache.hop.foo.bar.somepkg";
   *
   * <p>This can be a wrapped line so we do a multi-line code scan
   *
   * <p>private static String PKG =
   *
   * <p>"org.apache.hop.foo.bar.somepkg"; *
   *
   * @param javaCode
   * @return The specific package or null if nothing was found
   */
  private String findSpecificStringPackage(String javaCode) {
    Matcher matcher = stringPkgPattern.matcher(javaCode);
    if (matcher.find()) {
      String expression = javaCode.substring(matcher.start(), matcher.end());
      int beginIndex = expression.indexOf('"') + 1;
      int endIndex = expression.indexOf('"', beginIndex);
      return expression.substring(beginIndex, endIndex);
    }
    return null;
  }

  private String findPackage(String javaCode) {
    Matcher matcher = packagePattern.matcher(javaCode);
    if (matcher.find()) {
      String expression = javaCode.substring(matcher.start(), matcher.end());

      int beginIndex = expression.indexOf("org.apache.hop.");
      int endIndex = expression.indexOf(';');
      if (beginIndex >= 0 && endIndex >= 0) {
        return expression.substring(beginIndex, endIndex); // this is the default
      }
    }
    return null;
  }

  private Map<String, String> findImportOccurrences(String javaCode) {
    Map<String, String> map = new HashMap<>();

    Matcher matcher = importPattern.matcher(javaCode);
    while (matcher.find()) {
      String expression = javaCode.substring(matcher.start(), matcher.end() - 1);

      // The last word is the Class imported...
      // If it's * we ignore it.
      //
      int lastDotIndex = expression.lastIndexOf('.');
      if (lastDotIndex > 0) {
        String packageName = expression.substring("import ".length(), lastDotIndex);
        String className = expression.substring(lastDotIndex + 1);
        if (!"*".equals(className)) {
          map.put(className, packageName);
        }
      }
    }

    return map;
  }

  /**
   * Extract the needed information from the expression and the index on which Messages.getString()
   * occurs.
   *
   * @param sourceFolder The source folder the messages and java files live in
   * @param fileObject the file we're reading
   * @param messagesPackage the messages package
   * @param expression the matching expression
   * @param fileIndex the position in the java file
   */
  private void addKeyOccurrence(
      String sourceFolder,
      FileObject fileObject,
      String messagesPackage,
      String expression,
      int fileIndex) {

    // Let's not keep too much around...
    //
    String shortExpression = expression;
    if (expression.length()>100) {
      shortExpression = expression.substring( 0, 100 );
    }

    // Right after the "Messages.getString(" string is the key, quoted (")
    // until the next comma...
    //
    Matcher doubleQuoteMatcher = doubleQuotePattern.matcher(expression);

    int startKeyIndex = -1;
    int endKeyIndex = -1;
    String arguments = "";

    // First double quote...
    //
    if (doubleQuoteMatcher.find()) {
      startKeyIndex = doubleQuoteMatcher.start() + 1;
      if (doubleQuoteMatcher.find(startKeyIndex)) {
        endKeyIndex = doubleQuoteMatcher.start();
      }

      String key;
      if (endKeyIndex >= 0) {
        key = expression.substring(startKeyIndex, endKeyIndex);

        // Can we also determine the arguments?
        // No, not always: only if the arguments are all on the same expression.
        //

        // Look for the next closing bracket...
        //
        int bracketIndex = endKeyIndex;
        int nrOpen = 1;
        while (bracketIndex < expression.length() && nrOpen != 0) {
          int c = expression.charAt(bracketIndex);
          if (c == '(') {
            nrOpen++;
          }
          if (c == ')') {
            nrOpen--;
          }
          bracketIndex++;
        }

        if (bracketIndex + 1 < expression.length()) {
          arguments = expression.substring(endKeyIndex + 1, bracketIndex-1);
        } else {
          arguments = expression.substring(endKeyIndex + 1);
        }

      } else {
        key = expression.substring(startKeyIndex);
      }

      // Sanity check...
      //
      if (key.contains("\t") || key.contains(" ")) {
        System.out.println("Suspect key found: [" + key + "] in file [" + fileObject + "]");
      }

      // OK, add the occurrence to the list...
      //
      // Make sure we pass the System key occurrences to the correct package.
      //
      if (key.startsWith("System.")) {
        String i18nPackage = BaseMessages.class.getPackage().getName();
        KeyOccurrence keyOccurrence =
            new KeyOccurrence(
                fileObject, sourceFolder, i18nPackage, fileIndex, key, arguments, shortExpression);

        // If we just add this key, we'll get doubles in the i18n package
        //
        KeyOccurrence lookup = getKeyOccurrence(key, i18nPackage);
        if (lookup == null) {
          addKeyOccurrence(keyOccurrence);
        } else {
          // Adjust the expression of code...
          //
          lookup.setSourceLine(lookup.getSourceLine() + Const.CR + keyOccurrence.getSourceLine());
          lookup.incrementOccurrences();
        }
      } else {
        if (messagesPackage == null) {
          log.logError("Could not calculate messages package in file: " + fileObject);
        } else {
          KeyOccurrence keyOccurrence =
              new KeyOccurrence(
                  fileObject, sourceFolder, messagesPackage, fileIndex, key, arguments, shortExpression);
          addKeyOccurrence(keyOccurrence);
        }
      }
    }
  }

  /** @return A sorted list of distinct occurrences of the used message package names */
  public List<String> getMessagesPackagesList(String sourceFolder) {
    Map<String, List<KeyOccurrence>> packageOccurrences =
        sourcePackageOccurrences.get(sourceFolder);
    List<String> list = new ArrayList<>(packageOccurrences.keySet());
    Collections.sort(list);
    return list;
  }

  /**
   * Get all the key occurrences for a certain messages package.
   *
   * @param messagesPackage the package to hunt for
   * @return all the key occurrences for a certain messages package.
   */
  public List<KeyOccurrence> getOccurrencesForPackage(String messagesPackage) {
    List<KeyOccurrence> list = new ArrayList<>();

    for (String sourceFolder : sourcePackageOccurrences.keySet()) {
      Map<String, List<KeyOccurrence>> po = sourcePackageOccurrences.get(sourceFolder);
      List<KeyOccurrence> occurrences = po.get(messagesPackage);
      if (occurrences != null) {
        list.addAll(occurrences);
      }
    }
    return list;
  }

  public KeyOccurrence getKeyOccurrence(String key, String selectedMessagesPackage) {
    for (String sourceFolder : sourcePackageOccurrences.keySet()) {
      Map<String, List<KeyOccurrence>> po = sourcePackageOccurrences.get(sourceFolder);
      if (po != null) {
        List<KeyOccurrence> occurrences = po.get(selectedMessagesPackage);
        if (occurrences != null) {
          for (KeyOccurrence keyOccurrence : occurrences) {
            if (keyOccurrence.getKey().equals(key)
                && keyOccurrence.getMessagesPackage().equals(selectedMessagesPackage)) {
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
  public List<KeyOccurrence> getKeyOccurrences(String sourceFolder) {
    Map<String, KeyOccurrence> map = new HashMap<>();
    Map<String, List<KeyOccurrence>> po = sourcePackageOccurrences.get(sourceFolder);
    if (po != null) {
      for (List<KeyOccurrence> keyOccurrences : po.values()) {
        for (KeyOccurrence keyOccurrence : keyOccurrences) {
          String key = keyOccurrence.getMessagesPackage() + " - " + keyOccurrence.getKey();
          map.put(key, keyOccurrence);
        }
      }
    }

    return new ArrayList<>(map.values());
  }

  /**
   * Gets sourceDirectories
   *
   * @return value of sourceDirectories
   */
  public List<String> getSourceDirectories() {
    return sourceDirectories;
  }

  /** @param sourceDirectories The sourceDirectories to set */
  public void setSourceDirectories(List<String> sourceDirectories) {
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

  /** @param bundlesStore The bundlesStore to set */
  public void setBundlesStore(BundlesStore bundlesStore) {
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

  /** @param sourcePackageOccurrences The sourcePackageOccurrences to set */
  public void setSourcePackageOccurrences(
      Map<String, Map<String, List<KeyOccurrence>>> sourcePackageOccurrences) {
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

  /** @param filesToAvoid The filesToAvoid to set */
  public void setFilesToAvoid(List<String> filesToAvoid) {
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

  /** @param packagePattern The packagePattern to set */
  public void setPackagePattern(Pattern packagePattern) {
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

  /** @param importPattern The importPattern to set */
  public void setImportPattern(Pattern importPattern) {
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

  /** @param stringPkgPattern The stringPkgPattern to set */
  public void setStringPkgPattern(Pattern stringPkgPattern) {
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

  /** @param classPkgPatterns The classPkgPatterns to set */
  public void setClassPkgPatterns(List<Pattern> classPkgPatterns) {
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

  /** @param log The log to set */
  public void setLog(ILogChannel log) {
    this.log = log;
  }

  /**
   * Gets rootFolder
   *
   * @return value of rootFolder
   */
  public String getRootFolder() {
    return rootFolder;
  }

  /**
   * @param rootFolder The rootFolder to set
   */
  public void setRootFolder( String rootFolder ) {
    this.rootFolder = rootFolder;
  }
}
