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

package org.apache.hop.core.variables;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.HopURLClassLoader;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.xml.XmlHandler;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.DotName;
import org.jboss.jandex.FieldInfo;
import org.jboss.jandex.IndexView;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import java.io.File;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * This singleton provides access to all the described variables in the Hop universe.
 */
public class VariableRegistry {

  private static VariableRegistry instance;
  
  private final Map<VariableScope, List<DescribedVariable>> variableScopes;
  private final List<String> deprecatedNames;
  
  private VariableRegistry() {
    variableScopes = new HashMap<>();
    variableScopes.put(VariableScope.SYSTEM, new ArrayList<>());
    variableScopes.put(VariableScope.APPLICATION, new ArrayList<>());
    variableScopes.put(VariableScope.ENGINE, new ArrayList<>());
    deprecatedNames = new ArrayList<>();
  }

  public static VariableRegistry getInstance() {
    return instance;
  }

  /**
   * Search described variables with the <code>@Variable<code> annotation 
   * and detect deprecated variables with the <code>@Deprecated</code> annotation. 
   */
  public static void init() throws HopException {

    instance = new VariableRegistry();
    
    // Search variable with the <code>@Variable<code> annotations    
    try {
      JarCache cache = JarCache.getInstance();
      DotName annotationName = DotName.createSimple(Variable.class.getName());
      
      // Search annotation in native jar
      for (File jarFile : cache.getNativeJars()) {
        IndexView index = cache.getIndex(jarFile);
        for (AnnotationInstance info : index.getAnnotations(annotationName)) {
          register(jarFile, info.target().asField());
        }
      }

      // Search annotation in plugins
      for (File jarFile : cache.getPluginJars()) {
        IndexView index = cache.getIndex(jarFile);
        for (AnnotationInstance info : index.getAnnotations(annotationName)) {
            register(jarFile, info.target().asField());
        }
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logDetailed("Unable to find variable definitions", e);
    }
  }
  
  protected static void register(File jarFile, FieldInfo fieldInfo)
      throws ClassNotFoundException, SecurityException, NoSuchFieldException, MalformedURLException {
    URLClassLoader urlClassLoader =  createUrlClassLoader(jarFile.toURI().toURL(), FieldInfo.class.getClassLoader());
    Class<?> clazz = urlClassLoader.loadClass(fieldInfo.declaringClass().name().toString());
    Field field = clazz.getDeclaredField(fieldInfo.name());
    
    // Register described variable with annotation
    Variable variable = field.getAnnotation(Variable.class);
    String description = TranslateUtil.translate(variable.description(), clazz);
    DescribedVariable describedVariable = new DescribedVariable(field.getName(), variable.value(), description);            
    List<DescribedVariable> list = instance.variableScopes.get(variable.scope());    
    if ( list!=null ) {
      list.add(describedVariable);
    }
    
    // Keep list of described variables with <code>@Deprecated</code> annotation 
    Deprecated deprecated = field.getAnnotation(Deprecated.class);
    if ( deprecated!=null ) {
      instance.deprecatedNames.add(field.getName());
    }
  }
  
  
  protected static URLClassLoader createUrlClassLoader(URL jarFileUrl, ClassLoader classLoader) {
    List<URL> urls = new ArrayList<>();

    // Also append all the files in the underlying lib folder if it exists...
    //
    try {
      JarCache jarCache = JarCache.getInstance();

      String parentFolderName =
          new File(URLDecoder.decode(jarFileUrl.getFile(), "UTF-8")).getParent();

      File libFolder = new File(parentFolderName + Const.FILE_SEPARATOR + "lib");
      if (libFolder.exists()) {
        for (File libFile : jarCache.findJarFiles(libFolder)) {
          urls.add(libFile.toURI().toURL());
        }
      }

      // Also get the libraries in the dependency folders of the plugin in question...
      // The file is called dependencies.xml
      //
      String dependenciesFileName = parentFolderName + Const.FILE_SEPARATOR + "dependencies.xml";
      File dependenciesFile = new File(dependenciesFileName);
      if (dependenciesFile.exists()) {
        // Add the files in the dependencies folders to the classpath...
        //
        Document document = XmlHandler.loadXmlFile(dependenciesFile);
        Node dependenciesNode = XmlHandler.getSubNode(document, "dependencies");
        List<Node> folderNodes = XmlHandler.getNodes(dependenciesNode, "folder");
        for (Node folderNode : folderNodes) {
          String relativeFolderName = XmlHandler.getNodeValue(folderNode);
          String dependenciesFolderName =
              parentFolderName + Const.FILE_SEPARATOR + relativeFolderName;
          File dependenciesFolder = new File(dependenciesFolderName);
          if (dependenciesFolder.exists()) {
            // Now get the jar files in this dependency folder
            // This includes the possible lib/ folder dependencies in there
            //
            for (File libFile : jarCache.findJarFiles(dependenciesFolder)) {
              urls.add(libFile.toURI().toURL());
            }
          }
        }
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logError(
          "Unexpected error searching for variable in file '"
              + jarFileUrl
              + "'",
          e);
    }

    urls.add(jarFileUrl);

    return new HopURLClassLoader(urls.toArray(new URL[urls.size()]), classLoader);
  }

  /**
   * Finds a described variable
   *
   * @return the described variable
   */
  public DescribedVariable findDescribedVariable(final String name) {
    for (VariableScope scope : VariableScope.values()) {
      for (DescribedVariable variable : variableScopes.get(scope)) {
        if (variable.getName().equals(name)) {
          return variable;
        }
      }
    }
    return null;
  }

  /**
   * Gets all described variable names
   *
   * @return list of described variable names
   */
  public Set<String> getVariableNames() {
    return getVariableNames(VariableScope.values());
  }

  /**
   * Gets described variable names in the specified scopes
   *
   * @return list of described variable names
   */ 
  public Set<String> getVariableNames(final VariableScope... scopes) {
    Set<String> names = new TreeSet<>();
    for (VariableScope scope : scopes) {
      for (DescribedVariable variable : variableScopes.get(scope)) {
        names.add(variable.getName());
      }
    }
    return names;
  }
  
  /**
   * Gets all described variables
   *
   * @return list of described variables
   */
  public List<DescribedVariable> getDescribedVariables() {
    return getDescribedVariables(VariableScope.values());
  }
  
  /**
   * Gets described variables in the specified scopes
   *
   * @return list of described variables
   */ 
  public List<DescribedVariable> getDescribedVariables(final VariableScope... scopes) {
    List<DescribedVariable> list = new ArrayList<>();    
    for (VariableScope scope : scopes) {
      for (DescribedVariable variable : variableScopes.get(scope)) {
        list.add(variable);
      }
    }
    return list;
  }
  
  /**
   * Gets deprecated variable names.
   * 
   * Deprecated variables will be detected with the <code>@Deprecated</code> annotation.
   *
   */
  public List<String> getDeprecatedVariableNames() {
    return deprecatedNames;
  }
  
}
