/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.projects.util;

import java.lang.reflect.Method;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;

/**
 * Helper to clone Git repositories using UIGit from the hop-misc-git plugin. Uses the git plugin's
 * classloader to avoid ClassNotFoundException when plugins are loaded in isolation.
 */
public class GitCloneHelper {

  private GitCloneHelper() {}

  /**
   * Clone a repository (full clone, no credentials).
   *
   * @param directory Local directory to clone into
   * @param uri Repository URI (e.g. https://github.com/user/repo.git)
   * @return true if clone succeeded
   * @throws HopException if clone fails or git plugin is not installed
   */
  public static boolean cloneRepo(String directory, String uri) throws HopException {
    return cloneRepo(directory, uri, null, 0);
  }

  /**
   * Clone a repository with optional credentials and shallow clone depth.
   *
   * @param directory Local directory to clone into
   * @param uri Repository URI (e.g. https://github.com/user/repo.git)
   * @param token Optional token for authentication (null for none)
   * @param depth Shallow clone depth (0 = full clone, 1+ = shallow with that many commits)
   * @return true if clone succeeded
   * @throws HopException if clone fails or git plugin is not installed
   */
  public static boolean cloneRepo(String directory, String uri, String token, int depth)
      throws HopException {
    try {
      IPlugin gitPlugin = findGitPlugin();
      if (gitPlugin == null) {
        throw new HopException(
            "Git plugin is not installed. Please add the hop-misc-git plugin to your plugins folder.");
      }
      ClassLoader gitClassLoader = PluginRegistry.getInstance().getClassLoader(gitPlugin);
      Class<?> uiGitClass = gitClassLoader.loadClass("org.apache.hop.git.model.UIGit");
      Object uiGit = uiGitClass.getConstructor().newInstance();

      Object credentialsProvider = null;
      if (StringUtils.isNotEmpty(token)) {
        Method createTokenMethod =
            uiGitClass.getMethod("createTokenCredentialsProvider", String.class);
        credentialsProvider = createTokenMethod.invoke(null, token);
      }

      // Find cloneRepo(String, String, CredentialsProvider, int) by name and param count
      // to avoid loading CredentialsProvider in our classloader (causes NoClassDefFoundError
      // when projects plugin has dependencies.xml referencing git plugin)
      Method cloneRepoMethod = null;
      for (Method m : uiGitClass.getMethods()) {
        if ("cloneRepo".equals(m.getName()) && m.getParameterCount() == 4) {
          cloneRepoMethod = m;
          break;
        }
      }
      if (cloneRepoMethod == null) {
        throw new HopException("UIGit.cloneRepo(String,String,CredentialsProvider,int) not found");
      }
      Boolean success =
          (Boolean) cloneRepoMethod.invoke(uiGit, directory, uri, credentialsProvider, depth);
      return Boolean.TRUE.equals(success);
    } catch (Exception e) {
      if (e.getCause() != null) {
        throw new HopException("Clone failed: " + e.getCause().getMessage(), e.getCause());
      }
      throw new HopException("Clone failed: " + e.getMessage(), e);
    }
  }

  private static IPlugin findGitPlugin() {
    for (IPlugin plugin : PluginRegistry.getInstance().getPlugins(GuiPluginType.class)) {
      for (String className : plugin.getClassMap().values()) {
        if (className != null && className.startsWith("org.apache.hop.git.")) {
          return plugin;
        }
      }
    }
    return null;
  }
}
