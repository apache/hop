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

package org.apache.hop.core;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.HopURLClassLoader;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.Variable;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HopVariablesList {

  private static HopVariablesList instance;

  private List<DescribedVariable> defaultVariables;

  private HopVariablesList() {
    defaultVariables = new ArrayList<>();
  }

  public static HopVariablesList getInstance() {
    return instance;
  }

  public static void init() throws HopException {

    instance = new HopVariablesList();
    
    // Search variable annotations    
    try {
      JarCache cache = JarCache.getInstance();
      DotName annotationName = DotName.createSimple(Variable.class.getName());
      
      // Search annotation in native jar
      for (File jarFile : cache.getNativeJars()) {
        IndexView index = cache.getIndex(jarFile);
        for (AnnotationInstance info : index.getAnnotations(annotationName)) {
          registerDescribedVariable(jarFile, info.target().asField());
        }
      }

      // Search annotation in plugins
      for (File jarFile : cache.getPluginJars()) {
        IndexView index = cache.getIndex(jarFile);
        for (AnnotationInstance info : index.getAnnotations(annotationName)) {
            registerDescribedVariable(jarFile, info.target().asField());
        }
      }
    } catch (Exception e) {
      LogChannel.GENERAL.logDetailed("Unable to find hop variables definition", e);
    }
  }
  
  private static void registerDescribedVariable(File jarFile, FieldInfo fieldInfo)
      throws ClassNotFoundException, SecurityException, NoSuchFieldException, MalformedURLException {
    URLClassLoader urlClassLoader =  createUrlClassLoader(jarFile.toURI().toURL(), FieldInfo.class.getClassLoader());
    Class<?> clazz = urlClassLoader.loadClass(fieldInfo.declaringClass().name().toString());
    Field field = clazz.getDeclaredField(fieldInfo.name());
    Variable annotation = field.getAnnotation(Variable.class);
    
    // For now, ignore non editable variable (system)
    if ( annotation.editable() ) {
      String description = TranslateUtil.translate(annotation.description(), clazz);        
      instance.defaultVariables.add(new DescribedVariable(field.getName(), annotation.value(), description));
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
          "Unexpected error searching for plugin jar files in lib/ folder and dependencies for jar file '"
              + jarFileUrl
              + "'",
          e);
    }

    urls.add(jarFileUrl);

    return new HopURLClassLoader(urls.toArray(new URL[urls.size()]), classLoader);
  }
  
  public DescribedVariable findEnvironmentVariable(String name) {
    for (DescribedVariable describedVariable : defaultVariables) {
      if (describedVariable.getName().equals(name)) {
        return describedVariable;
      }
    }
    return null;
  }

  public Set<String> getVariablesSet() {
    Set<String> variablesSet = new HashSet<>();
    for (DescribedVariable describedVariable : defaultVariables) {
      variablesSet.add(describedVariable.getName());
    }
    return variablesSet;
  }

  /**
   * Gets defaultVariables
   *
   * @return value of defaultVariables
   */
  public List<DescribedVariable> getEnvironmentVariables() {
    return defaultVariables;
  }

  /** @param defaultVariables The defaultVariables to set */
  public void setDefaultVariables(List<DescribedVariable> defaultVariables) {
    this.defaultVariables = defaultVariables;
  }
}
