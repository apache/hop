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

package org.apache.hop.ui.hopgui.perspective.explorer.web;

import org.apache.hop.core.variables.IVariables;

/**
 * Holder for the optional Hop Web explorer-file service. The RAP module sets an implementation so
 * HTML and PDF explorer tabs can {@code Browser.setUrl()} a path with a real document base.
 */
public final class HopWebExplorerFileHelper {

  private static IHopWebExplorerFileService service;

  private HopWebExplorerFileHelper() {}

  public static void setService(IHopWebExplorerFileService explorerFileService) {
    service = explorerFileService;
  }

  public static IHopWebExplorerFileService getService() {
    return service;
  }

  /**
   * Context-relative explorer-file URL, or {@code null} when not running in Hop Web or the file
   * cannot be served.
   */
  public static String urlFor(String vfsFilename, IVariables variables) {
    if (service == null) {
      return null;
    }
    return service.urlFor(vfsFilename, variables);
  }
}
