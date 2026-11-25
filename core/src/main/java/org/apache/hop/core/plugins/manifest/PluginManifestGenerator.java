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
package org.apache.hop.core.plugins.manifest;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import javax.xml.parsers.DocumentBuilder;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Build-time generator of {@link PluginManifest}: inspects a finished plugin (or engine module) zip
 * and adds the manifest to it.
 *
 * <p>The zip is the source of truth for what ships and where; the module's resolved dependencies
 * only add Maven coordinates and scopes to the jars found, the module pom adds the direct
 * dependencies (other Hop plugins, drivers the user must supply) and the jar's Jandex index adds
 * the plugin ids. Builds run it with the goal {@code hop:manifest} of {@code
 * org.apache.hop:hop-maven-plugin}; {@link #main} runs it on any zip:
 *
 * <pre>
 * --zip target/hop-transform-json-2.20.0.zip --group org.apache.hop --artifact hop-transform-json
 * --version 2.20.0 [--deps dependency-list.txt] [--pom pom.xml] [--out target/plugin-manifest.json]
 * </pre>
 *
 * <p>With {@code --base true} it describes the base of a client zip instead: the jars under {@code
 * lib/} that come from the client module's own dependencies (hop-core, hop-engine, hop-ui and what
 * they need), not the plugins it bundles. See {@link #generateBase}.
 *
 * <p>{@code --deps} is {@code dependency:list} output written with {@code
 * outputAbsoluteArtifactFilename=true}. Without it the jars get their coordinates from their own
 * {@code pom.properties}, which is enough to describe a zip nobody built a manifest for.
 */
public class PluginManifestGenerator {

  /** Hop modules that are the engine itself, never a plugin another plugin "requires". */
  static final Set<String> ENGINE_ARTIFACTS =
      Set.of("hop-core", "hop-engine", "hop-ui", "hop-ui-rcp", "hop-ui-rap", "hop-libs");

  private static final String HOP_GROUP_ID = "org.apache.hop";
  private static final String ANNOTATION_PACKAGE_PREFIX = "org.apache.hop.";

  /** Annotation members that hold a plugin id, in the order the plugin types use them. */
  private static final List<String> ID_MEMBERS = List.of("id", "key", "type", "code");

  private final Consumer<String> warnings;

  /** Warnings go to standard output. */
  public PluginManifestGenerator() {
    this(System.out);
  }

  /** Warnings go to {@code log}, one line each. */
  public PluginManifestGenerator(PrintStream log) {
    this(message -> log.println("[hop-manifest] WARNING " + message));
  }

  /** Warnings go to {@code warnings}, such as the log of a Maven goal. */
  public PluginManifestGenerator(Consumer<String> warnings) {
    this.warnings = warnings;
  }

  public static void main(String[] args) throws Exception {
    Map<String, String> options = parseArgs(args);
    Path zip = Path.of(require(options, "zip"));
    Path deps = options.containsKey("deps") ? Path.of(options.get("deps")) : null;
    Path pom = options.containsKey("pom") ? Path.of(options.get("pom")) : null;
    Path out = options.containsKey("out") ? Path.of(options.get("out")) : null;

    DependencyList dependencies =
        deps != null && Files.isRegularFile(deps) ? DependencyList.parse(deps) : null;
    if (dependencies == null) {
      System.out.println(
          "[hop-manifest] no dependency list at "
              + deps
              + ": jars get coordinates from their own "
              + "pom.properties only");
      dependencies = DependencyList.empty();
    }

    PluginManifest manifest =
        new PluginManifestGenerator()
            .run(
                zip,
                require(options, "group"),
                require(options, "artifact"),
                require(options, "version"),
                dependencies,
                pom != null && Files.isRegularFile(pom) ? pom : null,
                Boolean.parseBoolean(options.get("base")),
                out);
    System.out.println("[hop-manifest] " + summary(manifest));
  }

  /**
   * Describe a finished zip, add the manifest to it and, when {@code out} is set, write a copy
   * there. This is what the Maven goal {@code hop:manifest} and {@link #main} run.
   *
   * @param dependencies the module's resolved dependencies, possibly empty
   * @param pom the module pom (direct dependencies), or {@code null}; not used for a base
   * @param base describe the base of a client zip ({@link #generateBase}) instead of one plugin
   * @param out where to write a copy of the manifest, or {@code null}
   */
  public PluginManifest run(
      Path zip,
      String groupId,
      String artifactId,
      String version,
      DependencyList dependencies,
      Path pom,
      boolean base,
      Path out)
      throws IOException {
    if (!Files.isRegularFile(zip)) {
      throw new IOException("Zip not found: " + zip);
    }
    PluginManifest manifest;
    if (base) {
      manifest = generateBase(zip, groupId, artifactId, version, dependencies);
      addToZip(zip, manifest, topFolder(zip));
    } else {
      manifest = generate(zip, groupId, artifactId, version, dependencies, pom);
      addToZip(zip, manifest);
    }
    if (out != null) {
      Files.createDirectories(out.toAbsolutePath().getParent());
      Files.write(out, manifest.toJson());
    }
    return manifest;
  }

  /** One line about a manifest, for the build log. */
  public static String summary(PluginManifest manifest) {
    return manifest.getManifestPath()
        + ": "
        + manifest.getFiles().size()
        + " files, "
        + manifest.getFiles(ManifestFile.Location.PRIVATE).size()
        + " private, "
        + manifest.getFiles(ManifestFile.Location.SHARED).size()
        + " shared, "
        + manifest.getFiles(ManifestFile.Location.JDBC).size()
        + " jdbc";
  }

  /**
   * Describe the zip without modifying it.
   *
   * @param zip the finished plugin or engine module zip
   * @param groupId Maven coordinates of the module that built the zip
   * @param dependencies the module's resolved dependencies, possibly empty
   * @param pom the module pom (direct dependencies), or {@code null}
   */
  public PluginManifest generate(
      Path zip,
      String groupId,
      String artifactId,
      String version,
      DependencyList dependencies,
      Path pom)
      throws IOException {
    PluginManifest manifest = new PluginManifest();
    manifest.setGroupId(groupId);
    manifest.setArtifactId(artifactId);
    manifest.setVersion(version);

    try (ZipFile zipFile = new ZipFile(zip.toFile())) {
      List<ZipEntry> entries = new ArrayList<>();
      List<String> versionFiles = new ArrayList<>();
      Enumeration<? extends ZipEntry> e = zipFile.entries();
      while (e.hasMoreElements()) {
        ZipEntry entry = e.nextElement();
        if (entry.isDirectory()) {
          continue;
        }
        String name = normalize(entry.getName());
        if (name.startsWith("plugins/") && name.endsWith("/version.xml")) {
          versionFiles.add(name);
        }
        entries.add(entry);
      }
      if (versionFiles.size() > 1) {
        throw new IOException(
            "Zip "
                + zip
                + " contains "
                + versionFiles.size()
                + " plugin folders; a manifest "
                + "describes exactly one plugin. Skip aggregate assemblies with -Dhop.manifest.skip=true.");
      }
      if (versionFiles.size() == 1) {
        manifest.setKind(PluginManifest.Kind.PLUGIN);
        String versionFile = versionFiles.get(0);
        manifest.setFolder(versionFile.substring(0, versionFile.lastIndexOf('/')));
      } else {
        manifest.setKind(PluginManifest.Kind.ENGINE);
      }

      ZipEntry ownJar = null;
      for (ZipEntry entry : entries) {
        if (normalize(entry.getName()).equals(manifest.getManifestPath())) {
          continue; // added by an earlier run, replaced by this one
        }
        ManifestFile file = describe(zipFile, entry, manifest, dependencies);
        manifest.getFiles().add(file);
        if (isOwnJar(file, manifest)) {
          ownJar = entry;
        }
      }
      manifest.getFiles().sort((a, b) -> a.getPath().compareTo(b.getPath()));

      if (ownJar != null) {
        manifest.setPluginIds(readPluginIds(zipFile, ownJar));
      } else if (manifest.getKind() == PluginManifest.Kind.PLUGIN) {
        warn(
            manifest,
            "no jar named " + artifactId + "-" + version + ".jar in " + manifest.getFolder());
      }

      if (manifest.getFolder() != null) {
        ZipEntry dependenciesXml = zipFile.getEntry(manifest.getFolder() + "/dependencies.xml");
        if (dependenciesXml != null) {
          try (InputStream in = zipFile.getInputStream(dependenciesXml)) {
            manifest.setRequiresFolders(readDependencyFolders(in, manifest.getFolder()));
          }
        }
      }
    }

    if (pom != null) {
      addDirectDependencies(manifest, pom, dependencies);
    }
    checkScopes(manifest);
    return manifest;
  }

  /**
   * Describe the base of a client zip: the jars under {@code lib/} that the client module's own
   * dependencies put there, and the SWT jars of {@code lib/swt}. The plugins the zip bundles are
   * not part of it; their own manifests describe them, including the copies of base jars a plugin
   * zip ships to {@code lib/core} as well.
   *
   * <p>With the plugin manifests this tells, for every jar in {@code lib/core}, whether the client
   * itself needs it: a jar no plugin and not the base claims can go, a jar the base claims stays.
   *
   * @param dependencies the client module's resolved runtime dependencies, without the zips it
   *     unpacks
   */
  public PluginManifest generateBase(
      Path zip, String groupId, String artifactId, String version, DependencyList dependencies)
      throws IOException {
    PluginManifest manifest = new PluginManifest();
    manifest.setKind(PluginManifest.Kind.ENGINE);
    manifest.setGroupId(groupId);
    manifest.setArtifactId(artifactId);
    manifest.setVersion(version);
    try (ZipFile zipFile = new ZipFile(zip.toFile())) {
      Enumeration<? extends ZipEntry> e = zipFile.entries();
      while (e.hasMoreElements()) {
        ZipEntry entry = e.nextElement();
        String name = baseRelative(normalize(entry.getName()));
        if (entry.isDirectory() || name == null || !name.startsWith("lib/")) {
          continue;
        }
        String fileName = name.substring(name.lastIndexOf('/') + 1);
        boolean swt = name.startsWith("lib/swt/");
        if (!swt && !(fileName.endsWith(".jar") && dependencies.findByFileName(fileName) != null)) {
          continue;
        }
        ManifestFile file = describe(zipFile, entry, manifest, dependencies);
        file.setPath(name);
        file.setLocation(locate(name, null));
        manifest.getFiles().add(file);
      }
    }
    manifest.getFiles().sort((a, b) -> a.getPath().compareTo(b.getPath()));
    return manifest;
  }

  /** The folder the zip's {@code lib/core} sits in, such as {@code hop/}, or empty for the root. */
  static String topFolder(Path zip) throws IOException {
    try (ZipFile zipFile = new ZipFile(zip.toFile())) {
      Enumeration<? extends ZipEntry> e = zipFile.entries();
      while (e.hasMoreElements()) {
        String name = normalize(e.nextElement().getName());
        int index = name.indexOf("lib/core/");
        if (index >= 0 && (index == 0 || name.charAt(index - 1) == '/')) {
          return name.substring(0, index);
        }
      }
    }
    return "";
  }

  /**
   * A client zip has a top folder ({@code hop/}): the path below it, or {@code null} for the top
   * folder itself. Paths of a zip without one are returned as they are.
   */
  private static String baseRelative(String name) {
    if (name.startsWith("lib/")) {
      return name;
    }
    int slash = name.indexOf('/');
    return slash < 0 ? null : name.substring(slash + 1);
  }

  /** Write the manifest into the zip at {@link PluginManifest#getManifestPath()}. */
  public void addToZip(Path zip, PluginManifest manifest) throws IOException {
    addToZip(zip, manifest, "");
  }

  /**
   * Write the manifest into the zip at {@link PluginManifest#getManifestPath()}, below {@code
   * topFolder} ({@code hop/} for a client zip, empty for a plugin zip). The entries already in the
   * zip keep their bytes and the manifest takes the time of the newest of them, so a reproducible
   * zip stays reproducible: see {@link ZipAppender}.
   */
  public void addToZip(Path zip, PluginManifest manifest, String topFolder) throws IOException {
    ZipAppender.append(zip, topFolder + manifest.getManifestPath(), manifest.toJson());
  }

  private ManifestFile describe(
      ZipFile zipFile, ZipEntry entry, PluginManifest manifest, DependencyList dependencies)
      throws IOException {
    String path = normalize(entry.getName());
    ManifestFile file = new ManifestFile(path, locate(path, manifest.getFolder()));

    MessageDigest digest = sha256();
    long size = 0;
    byte[] buffer = new byte[1 << 16];
    try (InputStream in = zipFile.getInputStream(entry)) {
      int read;
      while ((read = in.read(buffer)) > 0) {
        digest.update(buffer, 0, read);
        size += read;
      }
    }
    file.setSize(size);
    file.setSha256(hex(digest.digest()));

    if (file.isJar()) {
      String fileName = path.substring(path.lastIndexOf('/') + 1);
      DependencyList.Artifact artifact = dependencies.findByFileName(fileName);
      if (artifact != null) {
        file.setGroupId(artifact.groupId());
        file.setArtifactId(artifact.artifactId());
        file.setVersion(artifact.version());
        file.setClassifier(artifact.classifier());
        file.setScope(artifact.scope());
      } else if (fileName.equals(manifest.getArtifactId() + "-" + manifest.getVersion() + ".jar")) {
        file.setGroupId(manifest.getGroupId());
        file.setArtifactId(manifest.getArtifactId());
        file.setVersion(manifest.getVersion());
      } else {
        Properties pomProperties = readNestedProperties(zipFile, entry);
        if (pomProperties != null) {
          file.setGroupId(pomProperties.getProperty("groupId"));
          file.setArtifactId(pomProperties.getProperty("artifactId"));
          file.setVersion(pomProperties.getProperty("version"));
        }
      }
    }
    return file;
  }

  static ManifestFile.Location locate(String path, String folder) {
    if (folder != null && path.startsWith(folder + "/")) {
      return path.startsWith(folder + "/lib/")
          ? ManifestFile.Location.PRIVATE
          : ManifestFile.Location.PLUGIN;
    }
    if (path.startsWith("lib/core/")) {
      return ManifestFile.Location.SHARED;
    }
    if (path.startsWith("lib/jdbc/")) {
      return ManifestFile.Location.JDBC;
    }
    return ManifestFile.Location.OTHER;
  }

  private static boolean isOwnJar(ManifestFile file, PluginManifest manifest) {
    if (!file.isJar()
        || !manifest.getGroupId().equals(file.getGroupId())
        || !manifest.getArtifactId().equals(file.getArtifactId())) {
      return false;
    }
    return manifest.getKind() == PluginManifest.Kind.ENGINE
        ? file.getLocation() == ManifestFile.Location.SHARED
        : file.getLocation() == ManifestFile.Location.PLUGIN;
  }

  /**
   * Plugin ids from the Jandex index the build put in the jar: every class-level annotation from an
   * {@code org.apache.hop} package with a string id member ({@code id}, or {@code key} for
   * metadata, {@code type} for database and VFS plugins, {@code code} for row distributions).
   */
  private Map<String, List<String>> readPluginIds(ZipFile zipFile, ZipEntry jar)
      throws IOException {
    Map<String, TreeSet<String>> ids = new TreeMap<>();
    try (ZipInputStream in = new ZipInputStream(zipFile.getInputStream(jar))) {
      ZipEntry nested;
      while ((nested = in.getNextEntry()) != null) {
        if (!"META-INF/jandex.idx".equals(nested.getName())) {
          continue;
        }
        Index index = new IndexReader(in).read();
        for (ClassInfo classInfo : index.getKnownClasses()) {
          for (AnnotationInstance annotation : classInfo.declaredAnnotations()) {
            if (annotation.target() == null
                || annotation.target().kind() != AnnotationTarget.Kind.CLASS
                || !annotation.name().toString().startsWith(ANNOTATION_PACKAGE_PREFIX)) {
              continue;
            }
            AnnotationValue id = null;
            for (String member : ID_MEMBERS) {
              id = annotation.value(member);
              if (id != null) {
                break;
              }
            }
            if (id == null || id.kind() != AnnotationValue.Kind.STRING) {
              continue;
            }
            ids.computeIfAbsent(annotation.name().withoutPackagePrefix(), k -> new TreeSet<>())
                .add(id.asString());
          }
        }
        break;
      }
    }
    Map<String, List<String>> result = new TreeMap<>();
    ids.forEach((annotation, set) -> result.put(annotation, new ArrayList<>(set)));
    return result;
  }

  /**
   * The {@code <folder>} and {@code <libs><folder>} entries of a plugin's dependencies.xml,
   * resolved against the plugin folder, without the plugin folder itself.
   */
  static List<String> readDependencyFolders(InputStream in, String pluginFolder)
      throws IOException {
    TreeSet<String> folders = new TreeSet<>();
    Document document = parseXml(in);
    Element root = document.getDocumentElement();
    if (root == null) {
      return new ArrayList<>();
    }
    for (Element child : children(root)) {
      String relative = null;
      if ("folder".equals(child.getTagName())) {
        relative = child.getTextContent();
      } else if ("libs".equals(child.getTagName())) {
        for (Element libsChild : children(child)) {
          if ("folder".equals(libsChild.getTagName())) {
            relative = libsChild.getTextContent();
          }
        }
      }
      if (relative != null && !relative.isBlank()) {
        folders.add(resolveRelative(pluginFolder, relative.trim()));
      }
    }
    folders.remove(pluginFolder); // a folder pointing at the plugin itself is a no-op
    return new ArrayList<>(folders);
  }

  /**
   * Direct dependencies from the module pom: other Hop modules become {@code requiresArtifacts};
   * anything else the zip does not contain is recorded as not shipped.
   */
  private void addDirectDependencies(PluginManifest manifest, Path pom, DependencyList dependencies)
      throws IOException {
    Set<String> shippedGas = new TreeSet<>();
    for (ManifestFile file : manifest.getFiles()) {
      if (file.getGa() != null) {
        shippedGas.add(file.getGa());
      }
    }
    TreeSet<String> requires = new TreeSet<>(manifest.getRequiresArtifacts());
    Document document;
    try (InputStream in = Files.newInputStream(pom)) {
      document = parseXml(in);
    }
    Element project = document.getDocumentElement();
    for (Element dependenciesElement : children(project, "dependencies")) {
      for (Element dependency : children(dependenciesElement, "dependency")) {
        String groupId = text(dependency, "groupId");
        String artifactId = text(dependency, "artifactId");
        String scope = text(dependency, "scope");
        if (groupId == null || artifactId == null || "test".equals(scope)) {
          continue;
        }
        if (scope == null) {
          scope = "compile";
        }
        String ga = groupId + ":" + artifactId;
        if (HOP_GROUP_ID.equals(groupId)) {
          if (!ENGINE_ARTIFACTS.contains(artifactId)
              && !artifactId.equals(manifest.getArtifactId())) {
            requires.add(ga);
          }
          continue;
        }
        if (shippedGas.contains(ga)) {
          continue;
        }
        ManifestFile notShipped = new ManifestFile();
        notShipped.setGroupId(groupId);
        notShipped.setArtifactId(artifactId);
        notShipped.setScope(scope);
        DependencyList.Artifact resolved = dependencies.findByGa(groupId, artifactId);
        if (resolved != null) {
          notShipped.setVersion(resolved.version());
          notShipped.setClassifier(resolved.classifier());
          notShipped.setScope(resolved.scope());
        } else {
          String version = text(dependency, "version");
          if (version != null && !version.contains("${")) {
            notShipped.setVersion(version);
          }
        }
        manifest.getNotShipped().add(notShipped);
      }
    }
    manifest.setRequiresArtifacts(new ArrayList<>(requires));
  }

  /**
   * A jar in the wrong place for its scope means the assembly routing and the pom disagree. One
   * line per kind of mismatch: a hand-curated assembly can have hundreds of the same one.
   */
  private void checkScopes(PluginManifest manifest) {
    if (manifest.getKind() != PluginManifest.Kind.PLUGIN) {
      return;
    }
    Map<String, List<String>> mismatches = new TreeMap<>();
    for (ManifestFile file : manifest.getFiles()) {
      String scope = file.getScope();
      if (scope == null || !file.isJar()) {
        continue;
      }
      boolean provided = "provided".equals(scope);
      boolean mismatch =
          switch (file.getLocation()) {
            case PRIVATE -> provided;
            case SHARED, JDBC -> !provided;
            default -> false;
          };
      if (mismatch) {
        String kind = scope + " dependency shipped as " + file.getLocation().name().toLowerCase();
        mismatches.computeIfAbsent(kind, k -> new ArrayList<>()).add(file.getPath());
      }
    }
    mismatches.forEach(
        (kind, paths) ->
            warn(
                manifest,
                paths.size()
                    + " x "
                    + kind
                    + ": "
                    + String.join(", ", paths.subList(0, Math.min(3, paths.size())))
                    + (paths.size() > 3 ? ", ..." : "")));
  }

  private void warn(PluginManifest manifest, String message) {
    warnings.accept(manifest.getArtifactId() + ": " + message);
  }

  // --- small helpers ---

  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> options = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("--") && i + 1 < args.length) {
        options.put(args[i].substring(2), args[++i]);
      }
    }
    return options;
  }

  private static String require(Map<String, String> options, String key) {
    String value = options.get(key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing --" + key);
    }
    return value;
  }

  private static String normalize(String zipEntryName) {
    String name = zipEntryName.replace('\\', '/');
    while (name.startsWith("./")) {
      name = name.substring(2);
    }
    return name;
  }

  /** Resolve {@code ../postgresql} against {@code plugins/databases/cratedb}. */
  static String resolveRelative(String base, String relative) {
    Deque<String> parts = new ArrayDeque<>();
    for (String part : base.split("/")) {
      if (!part.isEmpty()) {
        parts.addLast(part);
      }
    }
    for (String part : relative.replace('\\', '/').split("/")) {
      if (part.isEmpty() || ".".equals(part)) {
        continue;
      }
      if ("..".equals(part)) {
        if (!parts.isEmpty()) {
          parts.removeLast();
        }
      } else {
        parts.addLast(part);
      }
    }
    return String.join("/", parts);
  }

  private static Properties readNestedProperties(ZipFile zipFile, ZipEntry jar) throws IOException {
    try (ZipInputStream in = new ZipInputStream(zipFile.getInputStream(jar))) {
      ZipEntry nested;
      while ((nested = in.getNextEntry()) != null) {
        String name = nested.getName();
        if (name.startsWith("META-INF/maven/") && name.endsWith("/pom.properties")) {
          Properties properties = new Properties();
          properties.load(in);
          return properties;
        }
      }
    }
    return null;
  }

  private static Document parseXml(InputStream in) throws IOException {
    try {
      DocumentBuilder builder =
          XmlParserFactoryProducer.createSecureDocBuilderFactory().newDocumentBuilder();
      return builder.parse(in);
    } catch (Exception e) {
      throw new IOException("Unable to parse XML", e);
    }
  }

  private static List<Element> children(Element parent) {
    return children(parent, null);
  }

  private static List<Element> children(Element parent, String tagName) {
    if (parent == null) {
      return Collections.emptyList();
    }
    List<Element> result = new ArrayList<>();
    NodeList nodes = parent.getChildNodes();
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      if (node instanceof Element element
          && (tagName == null || tagName.equals(element.getTagName()))) {
        result.add(element);
      }
    }
    return result;
  }

  private static String text(Element parent, String tagName) {
    List<Element> elements = children(parent, tagName);
    if (elements.isEmpty()) {
      return null;
    }
    String text = elements.get(0).getTextContent();
    return text == null || text.isBlank() ? null : text.trim();
  }

  private static MessageDigest sha256() throws IOException {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }

  private static String hex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }
}
