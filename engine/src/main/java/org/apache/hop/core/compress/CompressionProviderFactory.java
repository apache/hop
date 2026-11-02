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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hop.core.compress.hadoopsnappy.HadoopSnappyCompressionProvider;
import org.apache.hop.core.compress.snappy.SnappyCompressionProvider;
import org.apache.hop.core.compress.spi.HopCompressorStreamProvider;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;

/**
 * Factory for compression providers.
 *
 * <p><b>This factory is deprecated.</b> The Apache Commons Compress {@link CompressorStreamFactory}
 * is now the first-class compression registry in Hop: it serves all 14 common formats out of the
 * box, and Hop's own {@code none} and {@code zip} codecs are contributed to that registry through
 * the {@link CompressorStreamProvider} SPI (see {@link HopCompressorStreamProvider}). All format
 * metadata (name, description, default extension, input/output support) is declared by the {@link
 * CompressionPlugin} annotation of the {@link CommonsCompressCompressionProvider} subclasses.
 *
 * <p>This class remains for backward compatibility: its legacy methods keep their historical
 * signatures and semantics, but they now delegate to the commons-compress registry. New code should
 * use {@link #getFormats()}, {@link #getFormat(String)}, {@link #createCompressionInputStream} and
 * {@link #createCompressionOutputStream}.
 *
 * <p>The provider classes are discovered through the regular plugin machinery ({@link
 * CompressionPluginType} + {@link PluginRegistry}, backed by the Jandex annotation index), so
 * external {@link CompressionPlugin} providers are picked up as well. Two legacy providers cannot
 * migrate to commons-compress because their wire format differs: xerial {@code Snappy} (raw
 * framing) and {@code Hadoop-snappy} (reflection based Hadoop shim). They are resolved by name
 * through their legacy classes only.
 */
@Deprecated
public class CompressionProviderFactory {

  protected static CompressionProviderFactory INSTANCE = new CompressionProviderFactory();

  private final LogChannel log = new LogChannel("CompressionProviderFactory");

  private volatile List<ICompressionProvider> registeredProviders;
  private volatile boolean listenerInstalled;

  private CompressionProviderFactory() {}

  /**
   * Ensures the provider cache is invalidated whenever a compression plugin is added, removed or
   * changed in the {@link PluginRegistry}, so dynamically registered extensions are picked up
   * without manual cache invalidation.
   */
  private void ensurePluginListenerInstalled() {
    if (!listenerInstalled) {
      synchronized (this) {
        if (!listenerInstalled) {
          try {
            PluginRegistry.getInstance()
                .addPluginListener(
                    CompressionPluginType.class,
                    new org.apache.hop.core.plugins.IPluginTypeListener() {
                      @Override
                      public void pluginAdded(Object serviceObject) {
                        invalidateProviderCache();
                      }

                      @Override
                      public void pluginRemoved(Object serviceObject) {
                        invalidateProviderCache();
                      }

                      @Override
                      public void pluginChanged(Object serviceObject) {
                        invalidateProviderCache();
                      }
                    });
            listenerInstalled = true;
          } catch (Exception e) {
            // The registry may not be initialized yet in very early startup; the cache is then
            // populated lazily and the listener can be installed on the next call.
            log.logDebug("Unable to install compression plugin listener: " + e.getMessage());
          }
        }
      }
    }
  }

  public static CompressionProviderFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Invalidates the cached registered-provider list. Called by tests between registrations and
   * available for extensions that register or unregister compression plugins dynamically.
   */
  synchronized void invalidateProviderCache() {
    registeredProviders = null;
  }

  // ------------------------------------------------------------------
  // Format metadata (new primary API)
  // ------------------------------------------------------------------

  /**
   * The formats whose codec is available at runtime, i.e. present in the commons-compress registry
   * (built-in codecs plus Hop's SPI contribution). Input-only formats appear here too.
   *
   * <p>The format set is discovered from the {@link CompressionPlugin} annotated provider classes
   * registered in the {@link PluginRegistry}, then filtered by codec availability.
   */
  public List<CompressionFormat> getFormats() {
    final Set<String> inNames =
        HopCompressionRegistry.getInstance().getCompressorInputStreamNames();
    final Set<String> outNames =
        HopCompressionRegistry.getInstance().getCompressorOutputStreamNames();
    Map<String, CompressionFormat> formatsById = new LinkedHashMap<>();
    // 1. annotation-based providers (PluginRegistry discovery, covers external @CompressionPlugin
    //    classes: this is the supported extension point, no META-INF/services file required).
    //    The commons-compress backed providers are collected first so that, on an id conflict
    //    with a legacy/external provider, the cc-backed one wins deterministically.
    for (ICompressionProvider provider : getRegisteredProviders()) {
      if (!(provider instanceof CommonsCompressCompressionProvider)) {
        continue;
      }
      addFormatIfAvailable(formatsById, provider, inNames, outNames);
    }
    // 2. legacy and external providers (their codec is classpath-loaded, so always available);
    //    they fill in the ids not already provided by the cc-backed built-ins
    for (ICompressionProvider provider : getRegisteredProviders()) {
      if (provider instanceof CommonsCompressCompressionProvider) {
        continue;
      }
      CompressionFormat format = toFormat(provider);
      if (format == null) {
        continue;
      }
      formatsById.putIfAbsent(CompressionFormat.normalizeId(format.id()), format);
    }
    // 3. codecs contributed to the Hop registry without an annotation class (e.g. an extension
    //    that only registers a commons-compress provider): generic metadata is derived. When both
    //    are present, the annotation metadata wins so the plugin is registered exactly once.
    for (String id : inNames) {
      formatsById.putIfAbsent(id, derivedFormat(id, true, outNames.contains(id)));
    }
    for (String id : outNames) {
      formatsById.putIfAbsent(id, derivedFormat(id, inNames.contains(id), true));
    }
    List<CompressionFormat> formats = new ArrayList<>(formatsById.values());
    formats.sort(DISPLAY_ORDER);
    return List.copyOf(formats);
  }

  /** Adds the provider's format when its codec is available (cc-backed availability rule). */
  private static void addFormatIfAvailable(
      Map<String, CompressionFormat> formatsById,
      ICompressionProvider provider,
      Set<String> inNames,
      Set<String> outNames) {
    CompressionFormat format = toFormat(provider);
    if (format == null) {
      return;
    }
    if (isProviderAvailable(provider, format, inNames, outNames)) {
      formatsById.put(CompressionFormat.normalizeId(format.id()), format);
    }
  }

  /** "None" first (historical default), then alphabetical by name. */
  private static final Comparator<CompressionFormat> DISPLAY_ORDER =
      Comparator.comparing((CompressionFormat f) -> !"None".equals(f.name()))
          .thenComparing(CompressionFormat::name);

  /**
   * Generic metadata for a registry-known codec that has no {@link CompressionPlugin} class. The
   * display name is derived from the (upper case) registry key so the UI does not show raw keys
   * (e.g. {@code "ZSTD"} -> {@code "Zstd"}).
   */
  private static CompressionFormat derivedFormat(
      String id, boolean supportsInput, boolean supportsOutput) {
    String displayName =
        id == null || id.isEmpty()
            ? id
            : id.substring(0, 1) + id.substring(1).toLowerCase(Locale.ROOT);
    return new CompressionFormat(id, displayName, displayName, null, supportsInput, supportsOutput);
  }

  /**
   * The commons-compress registry keys are normalized to upper case (see its {@code toKey}); {@code
   * null} and empty ids are treated as unknown.
   */
  private static boolean isAvailable(String id, Set<String> registryKeys) {
    return id != null && !id.isEmpty() && registryKeys.contains(CompressionFormat.normalizeId(id));
  }

  /** Formats that can decompress (read) data, in display order. */
  public List<CompressionFormat> getInputFormats() {
    return getFormats().stream().filter(CompressionFormat::supportsInput).toList();
  }

  /** Formats that can compress (write) data, in display order. */
  public List<CompressionFormat> getOutputFormats() {
    return getFormats().stream().filter(CompressionFormat::supportsOutput).toList();
  }

  /**
   * Availability of a provider: legacy and external providers (not backed by a commons-compress
   * codec) load their codec from their own classpath, so they are always available; the
   * commons-compress backed providers depend on the presence of their codec in the registry.
   */
  private static boolean isProviderAvailable(
      ICompressionProvider provider,
      CompressionFormat format,
      Set<String> inNames,
      Set<String> outNames) {
    if (!(provider instanceof CommonsCompressCompressionProvider)) {
      return true;
    }
    return (format.supportsInput() && isAvailable(format.id(), inNames))
        || (format.supportsOutput() && isAvailable(format.id(), outNames));
  }

  /**
   * Resolves a format by its {@link CompressionFormat#name() name} or {@link CompressionFormat#id()
   * id}, case-insensitively.
   *
   * @return the format, or {@code null} when unknown
   */
  public CompressionFormat getFormat(String nameOrId) {
    if (nameOrId == null) {
      return null;
    }
    final String needle = nameOrId.toLowerCase(Locale.ROOT);
    for (CompressionFormat format : getFormats()) {
      if (needle.equals(format.name().toLowerCase(Locale.ROOT))
          || needle.equals(format.id().toLowerCase(Locale.ROOT))) {
        return format;
      }
    }
    return null;
  }

  // ------------------------------------------------------------------
  // Stream creation (new primary API)
  // ------------------------------------------------------------------

  /** Creates a decompressing input stream for the given format name or id. */
  public CompressionInputStream createCompressionInputStream(String nameOrId, InputStream in)
      throws IOException {
    ICompressionProvider provider = getProvider(nameOrId);
    if (provider == null) {
      throw new IOException("Unknown compression format: " + nameOrId);
    }
    return provider.createInputStream(in);
  }

  /** Creates a compressing output stream for the given format name or id. */
  public CompressionOutputStream createCompressionOutputStream(String nameOrId, OutputStream out)
      throws IOException {
    ICompressionProvider provider = getProvider(nameOrId);
    if (provider == null) {
      throw new IOException("Unknown compression format: " + nameOrId);
    }
    return provider.createOutputStream(out);
  }

  /**
   * Detects the compression format of the given stream by its magic bytes.
   *
   * @return the commons-compress registry key (e.g. {@code "zstd"}), or {@code null} when the
   *     format is not recognized
   */
  public String detectCompressionFormat(InputStream in) throws IOException {
    try {
      return CompressorStreamFactory.detect(in);
    } catch (CompressorException e) {
      return null;
    }
  }

  // ------------------------------------------------------------------
  // Discovery
  // ------------------------------------------------------------------

  /**
   * All registered compression providers, cached after first resolution: the commons-compress
   * backed built-ins (discovered via {@link CompressionPluginType}) plus the legacy
   * wire-incompatible {@code Snappy} and {@code Hadoop-snappy} providers.
   */
  protected List<ICompressionProvider> getRegisteredProviders() {
    ensurePluginListenerInstalled();
    List<ICompressionProvider> providers = registeredProviders;
    if (providers == null) {
      synchronized (this) {
        if (registeredProviders == null) {
          registeredProviders = resolveRegisteredProviders();
        }
        providers = registeredProviders;
      }
    }
    return providers;
  }

  private List<ICompressionProvider> resolveRegisteredProviders() {
    List<ICompressionProvider> providers = new ArrayList<>();
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins(CompressionPluginType.class);
    if (plugins != null) {
      for (IPlugin plugin : plugins) {
        try {
          ICompressionProvider provider =
              PluginRegistry.getInstance().loadClass(plugin, ICompressionProvider.class);
          if (provider != null) {
            providers.add(provider);
          }
        } catch (Exception e) {
          // Skip providers that cannot be loaded (e.g. missing optional libraries), but log so
          // programming errors (bad plugin class) are not silently hidden
          log.logDebug(
              "Unable to load compression provider '" + plugin.getName() + "': " + e.getMessage());
        }
      }
    }
    return List.copyOf(providers);
  }

  /**
   * Resolves a provider by format name or id in a single pass over the registered providers,
   * falling back to the legacy name resolution for {@code Snappy} and {@code Hadoop-snappy}.
   */
  private ICompressionProvider getProvider(String nameOrId) {
    if (nameOrId == null) {
      return null;
    }
    final String needle = nameOrId.toLowerCase(Locale.ROOT);
    for (ICompressionProvider provider : getRegisteredProviders()) {
      CompressionFormat format = toFormat(provider);
      if (format == null) {
        continue;
      }
      if (needle.equals(format.name().toLowerCase(Locale.ROOT))
          || needle.equals(format.id().toLowerCase(Locale.ROOT))) {
        return provider;
      }
    }
    if (needle.equals("snappy")) {
      return new SnappyCompressionProvider();
    }
    if (needle.equals("hadoop-snappy")) {
      return new HadoopSnappyCompressionProvider();
    }
    // a codec contributed to the Hop registry without an annotation class
    CompressionFormat format = getFormat(nameOrId);
    if (format != null) {
      return new RegistryBackedProvider(format);
    }
    return null;
  }

  /**
   * Derives the format metadata from a provider: prefers the {@link CompressionPlugin} annotation,
   * falls back to the legacy {@link ICompressionProvider} accessors for external providers.
   */
  private static CompressionFormat toFormat(ICompressionProvider provider) {
    CompressionPlugin annotation = provider.getClass().getAnnotation(CompressionPlugin.class);
    if (annotation != null) {
      String extension = annotation.defaultExtension();
      return new CompressionFormat(
          annotation.id(),
          annotation.name(),
          annotation.description(),
          extension.isEmpty() ? null : extension,
          annotation.supportsInput(),
          annotation.supportsOutput());
    }
    // external provider without the extended annotation members: use the legacy accessors
    String extension = provider.getDefaultExtension();
    return new CompressionFormat(
        provider.getName(),
        provider.getName(),
        provider.getDescription(),
        extension,
        provider.supportsInput(),
        provider.supportsOutput());
  }

  // ------------------------------------------------------------------
  // Legacy API (kept for backward compatibility, delegates to cc)
  // ------------------------------------------------------------------

  /**
   * @deprecated use {@link #getOutputFormats()} / {@link #getInputFormats()} / {@link
   *     #getFormats()}
   */
  @Deprecated
  public ICompressionProvider createCompressionProviderInstance(String name) {
    return getProvider(name);
  }

  /**
   * @deprecated use {@link #getFormats()}
   */
  @Deprecated
  public Collection<ICompressionProvider> getCompressionProviders() {
    return getRegisteredProviders();
  }

  /**
   * @deprecated use {@link #getOutputFormats()} / {@link #getInputFormats()} / {@link
   *     #getFormats()}
   */
  @Deprecated
  public String[] getCompressionProviderNames() {
    return getCompressionProviders().stream()
        .map(ICompressionProvider::getName)
        .toArray(String[]::new);
  }

  /**
   * @deprecated use {@link #getFormat(String)}
   */
  @Deprecated
  public ICompressionProvider getCompressionProviderByName(String name) {
    return getProvider(name);
  }
}
