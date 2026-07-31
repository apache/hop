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

package org.apache.hop.core.gui.markdown;

import java.util.List;
import org.apache.hop.core.util.Utils;
import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.ext.task.list.items.TaskListItemsExtension;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;

/** Shared CommonMark parser configuration for Markdown notes. */
public final class CommonMarkConfig {

  public static final List<Extension> EXTENSIONS =
      List.of(TablesExtension.create(), TaskListItemsExtension.create());

  private static volatile Parser parser;
  private static volatile HtmlRenderer htmlRenderer;

  private CommonMarkConfig() {}

  public static Node parse(String markdown) {
    if (Utils.isEmpty(markdown)) {
      return parser().parse("");
    }
    return parser().parse(markdown);
  }

  /** Render Markdown to an HTML body fragment (no surrounding document). */
  public static String toHtmlBody(String markdown) {
    if (Utils.isEmpty(markdown)) {
      return "";
    }
    return htmlRenderer().render(parse(markdown));
  }

  private static Parser parser() {
    Parser local = parser;
    if (local == null) {
      synchronized (CommonMarkConfig.class) {
        local = parser;
        if (local == null) {
          local = Parser.builder().extensions(EXTENSIONS).build();
          parser = local;
        }
      }
    }
    return local;
  }

  private static HtmlRenderer htmlRenderer() {
    HtmlRenderer local = htmlRenderer;
    if (local == null) {
      synchronized (CommonMarkConfig.class) {
        local = htmlRenderer;
        if (local == null) {
          local = HtmlRenderer.builder().extensions(EXTENSIONS).build();
          htmlRenderer = local;
        }
      }
    }
    return local;
  }
}
