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

import static dev.langchain4j.data.message.AiMessage.aiMessage;
import static dev.langchain4j.data.message.ImageContent.from;
import static dev.langchain4j.data.message.SystemMessage.systemMessage;
import static dev.langchain4j.data.message.UserMessage.userMessage;
import static java.util.Optional.empty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.trimToNull;
import static org.apache.commons.lang3.StringUtils.upperCase;
import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage;

import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ImageContent;
import dev.langchain4j.data.message.TextContent;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.hop.core.exception.HopValueException;

public class Message extends BaseMessage {

  private static final Collection<String> ACCEPTED_DETAILS = List.of("LOW", "HIGH", "AUTO");
  public static final String CONST_MESSAGE_ROLE = "Message role '";
  public static final String CONST_AT_INDEX = "' at index ";

  private String imageBase64Data;
  private String imageDetailLevel;
  private String imageMimeType;
  private String imageUri;
  private String imageUrl;

  public String getImageBase64Data() {
    return imageBase64Data;
  }

  public void setImageBase64Data(String imageBase64Data) {
    this.imageBase64Data = imageBase64Data;
  }

  public String getImageDetailLevel() {
    return imageDetailLevel;
  }

  public void setImageDetailLevel(String imageDetailLevel) {
    this.imageDetailLevel = imageDetailLevel;
  }

  public String getImageMimeType() {
    return imageMimeType;
  }

  public void setImageMimeType(String imageMimeType) {
    this.imageMimeType = imageMimeType;
  }

  public String getImageUri() {
    return imageUri;
  }

  public void setImageUri(String imageUri) {
    this.imageUri = imageUri;
  }

  public String getImageUrl() {
    return imageUrl;
  }

  public void setImageUrl(String imageUrl) {
    this.imageUrl = imageUrl;
  }

  public static boolean isRoleSystem(String role) {
    switch (role) {
      case "context", "environment", "setup", "system" -> {
        return true;
      }
      default -> {
        return false;
      }
    }
  }

  public static boolean isRoleUser(String role) {
    switch (role) {
      case "customer", "client", "consumer", "participant", "actor", "user" -> {
        return true;
      }
      default -> {
        return false;
      }
    }
  }

  public static boolean isRoleAssistant(String role) {
    switch (role) {
      case "agent", "bot", "adviser", "helper", "assistant", "ai" -> {
        return true;
      }
      default -> {
        return false;
      }
    }
  }

  public static Optional<String> detectSystemRoleName(List<Message> messages) {
    for (Message message : messages) {
      if (isRoleSystem(message.getRole())) {
        return Optional.of(message.getRole());
      }
    }

    return empty();
  }

  public static Optional<String> detectUserRoleName(List<Message> messages) {
    for (Message message : messages) {
      if (isRoleUser(message.getRole())) {
        return Optional.of(message.getRole());
      }
    }

    return empty();
  }

  public static Optional<String> detectAssistantRoleName(List<Message> messages) {
    for (Message message : messages) {
      if (isRoleAssistant(message.getRole())) {
        return Optional.of(message.getRole());
      }
    }

    return empty();
  }

  public static List<ChatMessage> toChatMessages(List<Message> incoming) throws HopValueException {
    List<ChatMessage> messages = new ArrayList<>(incoming.size());

    for (int i = 0; i < incoming.size(); i++) {
      Message m = incoming.get(i);
      String role = trimToNull(m.getRole());
      notNull(role, "Message role not set at index " + i);
      notNull(role, "Message at index " + i + " does not have 'role' set");

      String content = trimToNull(m.getContent());
      if (isRoleSystem(role)) {
        notNull(
            content,
            CONST_MESSAGE_ROLE + role + CONST_AT_INDEX + i + " does not have 'content' set");
        messages.add(systemMessage(content));
      } else if (isRoleAssistant(role)) {
        notNull(
            content,
            CONST_MESSAGE_ROLE + role + CONST_AT_INDEX + i + " does not have 'content' set");
        messages.add(aiMessage(content));
      } else if (isRoleUser(role)) {
        String uri = trimToNull(m.getImageUri());
        String url = trimToNull(m.getImageUrl());
        String base64 = trimToNull(m.getImageBase64Data());
        String mime = trimToNull(m.getImageMimeType());
        String detail = upperCase(trimToNull(m.getImageDetailLevel())); // low, high, auto

        ImageContent.DetailLevel detailLevel = null;
        if (isNotBlank(detail)) {
          boolean v = ACCEPTED_DETAILS.contains(detail);
          isTrue(
              v,
              CONST_MESSAGE_ROLE
                  + role
                  + CONST_AT_INDEX
                  + i
                  + " does not have a valid 'imageDetailLevel' set. Must be either low, high, or auto.");
          detailLevel = ImageContent.DetailLevel.valueOf(detail);
        }

        if (isNotBlank(base64) || isNotBlank(mime)) {
          boolean v = isNotBlank(base64) && isNotBlank(mime);
          isTrue(
              v,
              CONST_MESSAGE_ROLE
                  + role
                  + CONST_AT_INDEX
                  + i
                  + " does not have both 'imageBase64Data' and 'imageMimeType' set. When either one is set, both are required.");
          ImageContent image =
              detailLevel == null ? from(base64, mime) : from(base64, mime, detailLevel);
          ChatMessage userMessage =
              isNotBlank(content)
                  ? userMessage(TextContent.from(content), image)
                  : userMessage(image);
          messages.add(userMessage);
        } else if (isNotBlank(uri)) {
          try {
            URI imageUri = URI.create(uri);
            ImageContent image = detailLevel == null ? from(imageUri) : from(imageUri, detailLevel);
            ChatMessage userMessage =
                isNotBlank(content)
                    ? userMessage(TextContent.from(content), image)
                    : userMessage(image);
            messages.add(userMessage);
          } catch (Exception e) {
            String err =
                CONST_MESSAGE_ROLE
                    + role
                    + CONST_AT_INDEX
                    + i
                    + " has a problem with 'imageUri': "
                    + getRootCauseMessage(e);
            throw new IllegalArgumentException(err);
          }
        } else if (isNotBlank(url)) {
          ImageContent image = detailLevel == null ? from(url) : from(url, detailLevel);
          ChatMessage userMessage =
              isNotBlank(content)
                  ? userMessage(TextContent.from(content), image)
                  : userMessage(image);
          messages.add(userMessage);
        } else if (isNotBlank(content)) {
          messages.add(userMessage(content));
        }

      } else {
        throw new HopValueException("Invalid message role at index " + i);
      }
    }

    return messages;
  }
}
