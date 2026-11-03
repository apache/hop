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

package org.apache.hop.core.compress;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.CompressorStreamProvider;
import org.apache.hop.core.compress.spi.HopCompressorStreamProvider;

/**
 * Hop's view of the Apache Commons Compress compression registry.
 *
 * <p>Apache Commons Compress discovers {@link CompressorStreamProvider} implementations through
 * {@link ClassLoader#getSystemClassLoader()} when the {@link CompressorStreamFactory} singleton is
 * first used. In embedded contexts (application class loader, OSGi, isolated class loaders) Hop's
 * own {@code none}/{@code zip} SPI contribution may therefore not be visible to the singleton.
 *
 * <p>This registry decorates the commons-compress singleton and guarantees that the Hop codecs
 * ({@code none}, {@code zip}) are always available regardless of class loader: it merges the
 * singleton's providers with the providers registered through {@link
 * #register(CompressorStreamProvider)} (the Hop provider is registered by default), de-duplicating
 * by registry key, and stream creation falls back to the registered providers when the singleton
 * does not know a key.
 *
 * <p>Extensions can contribute codecs through {@link #register(CompressorStreamProvider)} / {@link
 * #unregister(CompressorStreamProvider)} at any time, and format metadata goes through the {@link
 * CompressionPlugin} annotation as usual. If an extension also ships a {@code
 * META-INF/services/...CompressorStreamProvider} file, both paths merge and the format is
 * registered exactly once.
 */
public final class HopCompressionRegistry {

  private static final HopCompressionRegistry INSTANCE = new HopCompressionRegistry();

  private final CompressorStreamFactory singletonsFactory = CompressorStreamFactory.getSingleton();
  private final CopyOnWriteArrayList<CompressorStreamProvider> providers =
      new CopyOnWriteArrayList<>();

  private HopCompressionRegistry() {
    providers.add(new HopCompressorStreamProvider());
  }

  public static HopCompressionRegistry getInstance() {
    return INSTANCE;
  }

  /** Registers a provider (no-op when already registered). */
  public void register(CompressorStreamProvider provider) {
    if (provider != null && !providers.contains(provider)) {
      providers.add(provider);
    }
  }

  /** Removes a provider; the default Hop provider (none/zip) is always kept. */
  public void unregister(CompressorStreamProvider provider) {
    if (provider != null && !(provider instanceof HopCompressorStreamProvider)) {
      providers.remove(provider);
    }
  }

  /** The providers contributed through {@link #register(CompressorStreamProvider)}. */
  public Set<CompressorStreamProvider> getRegisteredProviders() {
    return Set.copyOf(providers);
  }

  /** All input stream compressor names known to Hop: cc singleton ∪ registered providers. */
  public Set<String> getCompressorInputStreamNames() {
    Map<String, CompressorStreamProvider> merged = mergedInputProviders();
    return merged.keySet();
  }

  /** All output stream compressor names known to Hop: cc singleton ∪ registered providers. */
  public Set<String> getCompressorOutputStreamNames() {
    Map<String, CompressorStreamProvider> merged = mergedOutputProviders();
    return merged.keySet();
  }

  public CompressorInputStream createCompressorInputStream(
      String name, InputStream in, boolean decompressUntilEof) throws CompressorException {
    try {
      return singletonsFactory.createCompressorInputStream(name, in, decompressUntilEof);
    } catch (CompressorException e) {
      // class loader resilience: the singleton may not know a registered provider's key
      CompressorStreamProvider provider = mergedInputProviders().get(keyOf(name));
      if (provider != null) {
        return provider.createCompressorInputStream(name, in, decompressUntilEof);
      }
      throw e;
    }
  }

  public CompressorOutputStream<? extends OutputStream> createCompressorOutputStream(
      String name, OutputStream out) throws CompressorException {
    try {
      return singletonsFactory.createCompressorOutputStream(name, out);
    } catch (CompressorException e) {
      CompressorStreamProvider provider = mergedOutputProviders().get(keyOf(name));
      if (provider != null) {
        return provider.createCompressorOutputStream(name, out);
      }
      throw e;
    }
  }

  /** Merges the singleton's input providers with the registered ones, de-duplicating by key. */
  private Map<String, CompressorStreamProvider> mergedInputProviders() {
    TreeMap<String, CompressorStreamProvider> merged =
        new TreeMap<>(singletonsFactory.getCompressorInputStreamProviders());
    for (CompressorStreamProvider provider : providers) {
      for (String name : provider.getInputStreamCompressorNames()) {
        merged.put(keyOf(name), provider);
      }
    }
    return merged;
  }

  /** Merges the singleton's output providers with the registered ones, de-duplicating by key. */
  private Map<String, CompressorStreamProvider> mergedOutputProviders() {
    TreeMap<String, CompressorStreamProvider> merged =
        new TreeMap<>(singletonsFactory.getCompressorOutputStreamProviders());
    for (CompressorStreamProvider provider : providers) {
      for (String name : provider.getOutputStreamCompressorNames()) {
        merged.put(keyOf(name), provider);
      }
    }
    return merged;
  }

  /** Commons-compress normalizes registry keys to upper case (see its {@code toKey}). */
  /**
   * Commons-compress normalizes registry keys to upper case (see its {@code toKey}); the rule is
   * shared with {@link CompressionFormat#normalizeId(String)}.
   */
  static String keyOf(String name) {
    return CompressionFormat.normalizeId(name);
  }
}
