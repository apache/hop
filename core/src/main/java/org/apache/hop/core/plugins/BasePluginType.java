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

package org.apache.hop.core.plugins;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.i18n.GlobalMessageUtil;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.google.common.annotations.VisibleForTesting;

public abstract class BasePluginType<T extends Annotation> implements IPluginType<T> {
  protected static Class<?> classFromResourcesPackage = BasePluginType.class; // For Translator

  protected final PluginRegistry registry;
  
  private String id;
  
  private String name;

  private LogChannel log;

  private Map<Class<?>, String> objectTypes = new HashMap<>();

  private Class<T> pluginClass;

  private List<String> extraLibraryFolders;
  
  public BasePluginType( Class<T> pluginClazz ) {
    this.log = new LogChannel( "Plugin type" );

    registry = PluginRegistry.getInstance();
    this.pluginClass = pluginClazz;

    this.extraLibraryFolders = new ArrayList<>();
  }

  /**
   * @param id   The plugin type ID
   * @param name the name of the plugin
   */
  public BasePluginType( Class<T> pluginType, String id, String name ) {
    this( pluginType );
    this.id = id;
    this.name = name;
  }

  public Map<Class<?>, String> getAdditionalRuntimeObjectTypes() {
    return objectTypes;
  }

  @Override
  public void addObjectType( Class<?> clz, String xmlNodeName ) {
    objectTypes.put( clz, xmlNodeName );
  }

  @Override
  public String toString() {
    return name + "(" + id + ")";
  }

  /** Let's put in code here to search for the transform plugins.. */
  @Override
  public void searchPlugins() throws HopPluginException {

    StopWatch watch = new StopWatch();    
    if ( log.isDebug() ) {
      watch.start();
    }    
    
    // Register natives plugins
    registerNatives();
    
    // Register plugins from plugin folders
    registerPluginJars();
     
    if ( log.isDebug() ) {
      watch.stop();
      List<Plugin> plugins = registry.getPlugins(this.getClass());
      log.logBasic(pluginClass.getSimpleName()+" register " + plugins.size()+ " plugins (Time Elapsed: " + watch.getTime()+"ms)"); 
    }
  }

  protected void registerNatives() throws HopPluginException {
    try {
      JarCache cache = JarCache.getInstance();
      DotName pluginName = DotName.createSimple(pluginClass.getName());
      for (File jarFile : cache.getNativeJars()) {
        IndexView index = cache.getIndex(jarFile);

        // find annotations annotated with this meta-annotation
        for (AnnotationInstance instance : index.getAnnotations(pluginName)) {
          if (instance.target() instanceof ClassInfo) {

            ClassInfo classInfo = (ClassInfo) instance.target();
            String className = classInfo.name().toString();
            

            Class<?> clazz = this.getClass().getClassLoader().loadClass(className);

            if (clazz == null) {
              throw new HopPluginException("Unable to load class: " + className);
            }

            T annotation = clazz.getAnnotation( pluginClass );

            List<String> libraries = new ArrayList<>();
            
            handlePluginAnnotation( clazz, annotation, libraries, true, null );           
          }
        }
      }
    } catch (Exception e) {
      throw new HopPluginException("Error registring native plugins", e);
    }
  }

  @VisibleForTesting
  protected String getPropertyExternal( String key, String def ) {
    return System.getProperty( key, def );
  }

  @VisibleForTesting
  protected InputStream getResAsStreamExternal( String name ) {
    return getClass().getResourceAsStream( name );
  }

  @VisibleForTesting
  protected InputStream getFileInputStreamExternal( String name ) throws FileNotFoundException {
    return new FileInputStream( name );
  }

  /**
   * @return the id
   */
  @Override
  public String getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId( String id ) {
    this.id = id;
  }

  /**
   * @return the name
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  protected static String getCodedTranslation( String codedString ) {
    if ( codedString == null ) {
      return null;
    }

    if ( codedString.startsWith( Const.I18N_PREFIX ) ) {
      String[] parts = codedString.split( ":" );
      if ( parts.length != 3 ) {
        return codedString;
      } else {
        return BaseMessages.getString( parts[ 1 ], parts[ 2 ] );
      }
    } else {
      return codedString;
    }
  }

  protected static String[] getTranslations( String[] strings, String packageName, String altPackageName, Class<?> resourceClass) {
    if (strings==null) {
      return null;
    }
    String[] translations = new String[strings.length];
    for (int i=0;i<translations.length;i++) {
      translations[i] = getTranslation( strings[i], packageName, altPackageName, resourceClass );
    }
    return translations;
  }

  protected static String getTranslation( String string, String packageName, String altPackageName,
                                          Class<?> resourceClass ) {
    if ( string == null ) {
      return null;
    }

    if ( string.startsWith( Const.I18N_PREFIX ) ) {
      String[] parts = string.split( ":" );
      if ( parts.length != 3 ) {
        return string;
      } else {
        String i18nPackage = parts[1];
        if ( StringUtils.isEmpty( i18nPackage )) {
          i18nPackage = Const.NVL(packageName, altPackageName);
        }
        String i18nKey = parts[2];

        String translation = BaseMessages.getString( i18nPackage, i18nKey, resourceClass );
        if (translation.startsWith( "!" ) && translation.endsWith( "!" )) {
          translation = BaseMessages.getString( i18nPackage, i18nKey );
        }
        return translation;
      }
    } else {
      // Try the default package name
      //
      String translation;
      if ( !Utils.isEmpty( packageName ) ) {
        LogLevel oldLogLevel = DefaultLogLevel.getLogLevel();

        // avoid i18n messages for missing locale
        //
        DefaultLogLevel.setLogLevel( LogLevel.BASIC );

        translation = BaseMessages.getString( packageName, string, resourceClass );
        if ( translation.startsWith( "!" ) && translation.endsWith( "!" ) ) {
          translation = BaseMessages.getString( classFromResourcesPackage, string, resourceClass );
        }

        // restore loglevel, when the last alternative fails, log it when loglevel is detailed
        //
        DefaultLogLevel.setLogLevel( oldLogLevel );
        if ( !Utils.isEmpty( altPackageName ) &&  translation.startsWith( "!" ) && translation.endsWith( "!" ) ) {
          translation = BaseMessages.getString( altPackageName, string, resourceClass );
        }
      } else {
        // Translations are not supported, simply keep the original text.
        //
        translation = string;
      }

      return translation;
    }
  }

  protected List<PluginClassFile> findAnnotatedClassFiles(String annotationClassName)
      throws HopPluginException {
    JarCache cache = JarCache.getInstance();

    List<PluginClassFile> classFiles = new ArrayList<>();

    try {
      // Get all the jar files with annotation index files...
      //      
      for (File jarFile : cache.getPluginJars()) {

        // These are the jar files : find annotations in it...
        //
        IndexView index = cache.getIndex(jarFile);
        // find annotations annotated with this meta-annotation
        for (AnnotationInstance instance :
            index.getAnnotations(DotName.createSimple(pluginClass.getName()))) {
          if (instance.target() instanceof ClassInfo) {
            try {
              ClassInfo classInfo = (ClassInfo) instance.target();
              String className = classInfo.name().toString();

              File folder = jarFile.getParentFile();

              classFiles.add(
                  new PluginClassFile(className, jarFile.toURI().toURL(), folder.toURI().toURL()));

            } catch (Exception e) {
              System.out.println(
                  "Error searching annotation for " + pluginClass + " in " + jarFile);
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopPluginException("Error finding plugin annotation " + annotationClassName, e);
    }

    return classFiles;
  }

  /**
   * This method allows for custom registration of plugins that are on the main classpath. This was originally created
   * so that test environments could register test plugins programmatically.
   *
   * @param clazz the plugin implementation to register
   * @param cat   the category of the plugin
   * @param id    the id for the plugin
   * @param name  the name for the plugin
   * @param desc  the description for the plugin
   * @param image the image for the plugin
   * @throws HopPluginException
   */
  public void registerCustom( Class<?> clazz, String cat, String id, String name, String desc, String image ) throws HopPluginException {
    Class<? extends IPluginType> pluginType = getClass();
    Map<Class<?>, String> classMap = new HashMap<>();
    PluginMainClassType mainClassTypesAnnotation = pluginType.getAnnotation( PluginMainClassType.class );
    classMap.put( mainClassTypesAnnotation.value(), clazz.getName() );
    IPlugin plugin = new Plugin( new String[] { id }, pluginType, mainClassTypesAnnotation.value(), cat, name, desc, image, false,
        false, classMap, new ArrayList<>(), null, null, null, false, null, null, null );
    registry.registerPlugin( pluginType, plugin );
  }


  /**
   * Loop over the extra library folders and find all the jar files in all sub-folders
   * @return the list of jar files in all the extra library folders
   */
  private List<String> addExtraJarFiles( ) {
    List<String> files = new ArrayList<>();
    for (String extraLibraryFolder : extraLibraryFolders) {
      File folder = new File(extraLibraryFolder);
      if (folder.exists()) {
        Collection<File> jarFiles = FileUtils.listFiles( folder, new String[] { "jar", "JAR", }, true );
        jarFiles.stream().forEach( file->files.add(file.getPath()) );
      }
    }
    return files;
  }


  /**
   * @param input
   * @param localizedMap
   * @return
   */
  protected String getAlternativeTranslation( String input, Map<String, String> localizedMap ) {

    if ( Utils.isEmpty( input ) ) {
      return null;
    }

    if ( input.startsWith( "i18n" ) ) {
      return getCodedTranslation( input );
    } else {
      for ( final Locale locale : GlobalMessageUtil.getActiveLocales() ) {
        String alt = localizedMap.get( locale.toString().toLowerCase() );
        if ( !Utils.isEmpty( alt ) ) {
          return alt;
        }
      }
      // Nothing found?
      // Return the original!
      //
      return input;
    }
  }

  /**
   * Create a new URL class loader with the jar file specified. Also include all the jar files in the lib folder next to
   * that file.
   *
   * @param jarFileUrl  The jar file to include
   * @param classLoader the parent class loader to use
   * @return The URL class loader
   */
  protected URLClassLoader createUrlClassLoader( URL jarFileUrl, ClassLoader classLoader ) {
    List<URL> urls = new ArrayList<>();

    // Also append all the files in the underlying lib folder if it exists...
    //
    try {
      JarCache jarCache = JarCache.getInstance();
      
      String parentFolderName = new File( URLDecoder.decode( jarFileUrl.getFile(), "UTF-8" ) ).getParent();

      File libFolder = new File(parentFolderName + Const.FILE_SEPARATOR + "lib");
      if ( libFolder.exists() ) {        
        for ( File libFile : jarCache.findJarFiles(libFolder) ) {
          urls.add( libFile.toURI().toURL() );
        }
      }

      // Also get the libraries in the dependency folders of the plugin in question...
      // The file is called dependencies.xml
      //
      String dependenciesFileName = parentFolderName + Const.FILE_SEPARATOR + "dependencies.xml";
      File dependenciesFile = new File( dependenciesFileName );
      if ( dependenciesFile.exists() ) {
        // Add the files in the dependencies folders to the classpath...
        //
        Document document = XmlHandler.loadXmlFile( dependenciesFile );
        Node dependenciesNode = XmlHandler.getSubNode( document, "dependencies" );
        List<Node> folderNodes = XmlHandler.getNodes( dependenciesNode, "folder" );
        for ( Node folderNode : folderNodes ) {
          String relativeFolderName = XmlHandler.getNodeValue( folderNode );
          String dependenciesFolderName = parentFolderName + Const.FILE_SEPARATOR + relativeFolderName;
          File dependenciesFolder = new File( dependenciesFolderName );
          if ( dependenciesFolder.exists() ) {
            // Now get the jar files in this dependency folder
            // This includes the possible lib/ folder dependencies in there
            //            
            for ( File libFile : jarCache.findJarFiles(dependenciesFolder) ) {
              urls.add( libFile.toURI().toURL() );
            }
          }
        }
      }
    } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Unexpected error searching for plugin jar files in lib/ folder and dependencies for jar file '"
        + jarFileUrl + "'", e );
    }


    urls.add( jarFileUrl );

    return new HopURLClassLoader( urls.toArray( new URL[ urls.size() ] ), classLoader );
  }

  protected String extractCategory( T annotation ) {
    return null;
  }

  protected abstract String extractID( T annotation );

  protected abstract String extractName( T annotation );

  protected abstract String extractDesc( T annotation );

  /**
   * Extract extra classes information from a plugin annotation.
   *
   * @param annotation
   */
  protected String extractClassLoaderGroup( T annotation ) {
    return null;
  }

  protected String extractImageFile( T annotation ) {
    return null;
  }

  protected boolean extractSeparateClassLoader( T annotation ) {
    return false;
  }

  protected String extractI18nPackageName( T annotation ) {
    return null;
  }

  protected void addExtraClasses( Map<Class<?>, String> classMap, Class<?> clazz, T annotation ) {
  }

  protected String extractDocumentationUrl( T annotation ) {
    return null;
  }

  protected String extractCasesUrl( T annotation ) {
    return null;
  }

  protected String extractForumUrl( T annotation ) {
    return null;
  }

  protected String extractSuggestion( T annotation ) {
    return null;
  }

  protected String[] extractKeywords( T annotation ) {
    return new String[] {};
  }

  protected void registerPluginJars() throws HopPluginException {
                
    List<PluginClassFile> pluginClassFiles = findAnnotatedClassFiles(pluginClass.getName());
    for ( PluginClassFile pluginClassFile : pluginClassFiles ) {

      URLClassLoader urlClassLoader = createUrlClassLoader( pluginClassFile.getJarFile(), getClass().getClassLoader() );

      try {
        Class<?> clazz = urlClassLoader.loadClass( pluginClassFile.getClassName() );
        if ( clazz == null ) {
          throw new HopPluginException( "Unable to load class: " + pluginClassFile.getClassName() );
        }
        List<String> libraries = Arrays.stream( urlClassLoader.getURLs() )
          .map( URL::getFile )
          .collect( Collectors.toList() );
        T annotation = clazz.getAnnotation( pluginClass );
        
        handlePluginAnnotation( clazz, annotation, libraries, false, pluginClassFile.getFolder() );
      } catch ( Exception e ) {
        // Ignore for now, don't know if it's even possible.
        LogChannel.GENERAL.logError(
          "Unexpected error registering jar plugin file: " + pluginClassFile.getJarFile(), e );
      } finally {
        if ( urlClassLoader instanceof HopURLClassLoader ) {
          ( (HopURLClassLoader) urlClassLoader ).closeClassLoader();
        }
      }
    }
  }

  /**
   * Handle an annotated plugin
   *
   * @param clazz            The class to use
   * @param annotation       The annotation to get information from
   * @param libraries        The libraries to add
   * @param nativePluginType Is this a native plugin?
   * @param pluginFolder     The plugin folder to use
   * @throws HopPluginException
   */
  //@Override
  public void handlePluginAnnotation( Class<?> clazz, T annotation, List<String> libraries, boolean nativePluginType, URL pluginFolder ) throws HopPluginException {

    String idList = extractID( annotation );
    if ( Utils.isEmpty( idList ) ) {
      // We take the class name as ID...
      //
      idList = clazz.getName();
    }

    // Only one ID for now
    String[] ids = idList.split( "," );
    String packageName = extractI18nPackageName( annotation );
    String altPackageName = clazz.getPackage().getName();
    String pluginName = getTranslation( extractName( annotation ), packageName, altPackageName, clazz );
    String description = getTranslation( extractDesc( annotation ), packageName, altPackageName, clazz );
    String category = getTranslation( extractCategory( annotation ), packageName, altPackageName, clazz );
    String imageFile = extractImageFile( annotation );
    boolean separateClassLoader = extractSeparateClassLoader( annotation );
    String documentationUrl = extractDocumentationUrl( annotation );
    String casesUrl = extractCasesUrl( annotation );
    String forumUrl = extractForumUrl( annotation );
    String suggestion = getTranslation( extractSuggestion( annotation ), packageName, altPackageName, clazz );
    String classLoaderGroup = extractClassLoaderGroup( annotation );
    String[] keywords = getTranslations(extractKeywords( annotation ), packageName, altPackageName, clazz);

    Map<Class<?>, String> classMap = new HashMap<>();

    PluginMainClassType mainType = getClass().getAnnotation( PluginMainClassType.class );
    Class<?> mainClass;
    if ( mainType != null ) {
      mainClass = mainType.value();
    } else {
      mainClass = clazz;
    }
    classMap.put( mainClass, clazz.getName() );
    addExtraClasses( classMap, clazz, annotation );

    
    // Check if plugin main class is deprecated by annotation
    //
    Deprecated deprecated = clazz.getDeclaredAnnotation(Deprecated.class);
    if ( deprecated!=null ) {
      String str = BaseMessages.getString( classFromResourcesPackage, "System.Deprecated" ).toLowerCase();
      pluginName += " (" + str + ")";
    }
        
    // Add all the jar files in the extra library folders
    //
    List<String> extraJarFiles = addExtraJarFiles();
    libraries.addAll(extraJarFiles);

    // If there are extra classes somewhere else, don't use a plugin folder
    //
    boolean usingLibrariesOutsidePluginFolder = !extraJarFiles.isEmpty();
    IPlugin plugin = new Plugin( ids, this.getClass(), mainClass, category, pluginName, description, imageFile, separateClassLoader,
        classLoaderGroup, nativePluginType, classMap, libraries, null, keywords, pluginFolder, usingLibrariesOutsidePluginFolder, documentationUrl,
        casesUrl, forumUrl, suggestion );

    ParentFirst parentFirstAnnotation = clazz.getAnnotation( ParentFirst.class );
    if ( parentFirstAnnotation != null ) {
      registry.addParentClassLoaderPatterns( plugin, parentFirstAnnotation.patterns() );
    }
    registry.registerPlugin( this.getClass(), plugin );

    if ( libraries != null && !libraries.isEmpty() ) {
      LogChannel.GENERAL.logDetailed( "Plugin with id ["
        + ids[ 0 ] + "] has " + libraries.size() + " libaries in its private class path" );
    }
  }

  /**
   * Gets extraLibraryFolders
   *
   * @return value of extraLibraryFolders
   */
  public List<String> getExtraLibraryFolders() {
    return extraLibraryFolders;
  }

  /**
   * @param extraLibraryFolders The extraLibraryFolders to set
   */
  public void setExtraLibraryFolders( List<String> extraLibraryFolders ) {
    this.extraLibraryFolders = extraLibraryFolders;
  }

  /**
   * Register an extra plugin from the classpath. Useful for testing.
   * @param clazz The class with the annotation to register to the plugin registry.
   * @throws HopPluginException in case something goes wrong with the class or the annotation
   */
  public void registerClassPathPlugin(Class<?> clazz) throws HopPluginException {
    T annotation = clazz.getAnnotation( pluginClass );
    handlePluginAnnotation( clazz, annotation, new ArrayList<>(), true, null );
  }
}
