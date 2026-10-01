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

package org.apache.hop.pipeline.transforms.languagemodelchat.internals;

import static java.util.stream.Collectors.joining;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.TextContent;
import dev.langchain4j.data.message.UserMessage;

/**
 * Reads the text out of a chat message. ChatMessage itself no longer exposes text(): a user message
 * can carry several contents (text and images), so the text parts are collected here.
 */
public class ChatMessages {

  private ChatMessages() {}

  public static String text(ChatMessage message) {
    if (message == null) {
      return null;
    }
    return switch (message.type()) {
      case SYSTEM -> ((SystemMessage) message).text();
      case AI -> ((AiMessage) message).text();
      case USER -> userText((UserMessage) message);
      default -> null;
    };
  }

  private static String userText(UserMessage message) {
    if (message.hasSingleText()) {
      return message.singleText();
    }
    return message.contents().stream()
        .filter(TextContent.class::isInstance)
        .map(content -> ((TextContent) content).text())
        .collect(joining("\n"));
  }
}
