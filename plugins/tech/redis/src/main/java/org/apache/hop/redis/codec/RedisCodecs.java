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

package org.apache.hop.redis.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import org.apache.hop.core.exception.HopException;

/**
 * Factory and holders for the four RedisTemplate-style codec slots: key, value, hashKey and
 * hashValue.
 */
public final class RedisCodecs {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final RedisValueCodec keyCodec;
  private final RedisValueCodec valueCodec;
  private final RedisValueCodec hashKeyCodec;
  private final RedisValueCodec hashValueCodec;

  public RedisCodecs(
      RedisCodecType keyType,
      RedisCodecType valueType,
      RedisCodecType hashKeyType,
      RedisCodecType hashValueType) {
    this.keyCodec = create(keyType);
    this.valueCodec = create(valueType);
    this.hashKeyCodec = create(hashKeyType);
    this.hashValueCodec = create(hashValueType);
  }

  public static RedisCodecs of(
      RedisCodecType keyType,
      RedisCodecType valueType,
      RedisCodecType hashKeyType,
      RedisCodecType hashValueType) {
    return new RedisCodecs(
        Objects.requireNonNullElse(keyType, RedisCodecType.STRING),
        Objects.requireNonNullElse(valueType, RedisCodecType.STRING),
        Objects.requireNonNullElse(hashKeyType, RedisCodecType.STRING),
        Objects.requireNonNullElse(hashValueType, RedisCodecType.STRING));
  }

  public RedisValueCodec key() {
    return keyCodec;
  }

  public RedisValueCodec value() {
    return valueCodec;
  }

  public RedisValueCodec hashKey() {
    return hashKeyCodec;
  }

  public RedisValueCodec hashValue() {
    return hashValueCodec;
  }

  public static RedisValueCodec create(RedisCodecType type) {
    RedisCodecType resolved = type == null ? RedisCodecType.STRING : type;
    return switch (resolved) {
      case STRING -> new StringCodec();
      case JSON -> new JsonCodec();
      case JAVA_OBJECT -> new JavaObjectCodec();
      case BYTE -> new ByteCodec();
    };
  }

  static final class StringCodec implements RedisValueCodec {
    @Override
    public byte[] encode(Object value) {
      if (value == null) {
        return new byte[0];
      }
      if (value instanceof byte[] bytes) {
        return bytes;
      }
      return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object decode(byte[] bytes) {
      if (bytes == null) {
        return null;
      }
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  static final class JsonCodec implements RedisValueCodec {
    @Override
    public byte[] encode(Object value) throws HopException {
      try {
        return switch (value) {
          case null -> null;
          case byte[] bytes -> bytes;
          case String string ->
              // Treat as JSON text already
              string.getBytes(StandardCharsets.UTF_8);
          default -> OBJECT_MAPPER.writeValueAsBytes(value);
        };
      } catch (JsonProcessingException e) {
        throw new HopException(e);
      }
    }

    @Override
    public Object decode(byte[] bytes) throws HopException {
      if (bytes == null) {
        return null;
      }
      // Return JSON as String for Hop row compatibility
      try {
        Object parsed = OBJECT_MAPPER.readValue(bytes, Object.class);
        if (parsed instanceof String || parsed instanceof Number || parsed instanceof Boolean) {
          return parsed;
        }
        return OBJECT_MAPPER.writeValueAsString(parsed);
      } catch (IOException e) {
        throw new HopException(e);
      }
    }
  }

  static final class JavaObjectCodec implements RedisValueCodec {
    @Override
    public byte[] encode(Object value) throws HopException {
      if (value == null) {
        return new byte[0];
      }
      if (value instanceof byte[] bytes) {
        return bytes;
      }
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(value);
      } catch (IOException e) {
        throw new HopException(e);
      }
      return bos.toByteArray();
    }

    @Override
    public Object decode(byte[] bytes) throws HopException {
      if (bytes == null) {
        return null;
      }
      try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
        return ois.readObject();
      } catch (Exception e) {
        throw new HopException(e);
      }
    }
  }

  static final class ByteCodec implements RedisValueCodec {
    @Override
    public byte[] encode(Object value) {
      if (value == null) {
        return new byte[0];
      }
      if (value instanceof byte[] bytes) {
        return bytes;
      }
      // Numbers are stored as UTF-8 decimal text (not Base64 — "12" is valid Base64 for 0xD7).
      if (value instanceof Number) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
      }
      String text = String.valueOf(value);
      if (looksLikeBase64(text)) {
        try {
          return Base64.getDecoder().decode(text);
        } catch (IllegalArgumentException e) {
          // fall through to UTF-8
        }
      }
      return text.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object decode(byte[] bytes) {
      return bytes;
    }

    /**
     * Only treat padded, length-aligned strings as Base64 so short values like {@code "12"} or
     * {@code "age"} are stored as UTF-8 text.
     */
    static boolean looksLikeBase64(String text) {
      int len = text.length();
      if (len < 4 || len % 4 != 0) {
        return false;
      }
      for (int i = 0; i < len; i++) {
        char c = text.charAt(i);
        boolean ok =
            (c >= 'A' && c <= 'Z')
                || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9')
                || c == '+'
                || c == '/'
                || c == '=';
        if (!ok) {
          return false;
        }
      }
      // padding only at end
      int pad = 0;
      if (text.charAt(len - 1) == '=') {
        pad++;
        if (text.charAt(len - 2) == '=') {
          pad++;
        }
      }
      for (int i = 0; i < len - pad; i++) {
        if (text.charAt(i) == '=') {
          return false;
        }
      }
      return pad <= 2;
    }
  }
}
