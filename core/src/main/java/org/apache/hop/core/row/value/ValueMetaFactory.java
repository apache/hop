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

package org.apache.hop.core.row.value;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.DataInputStream;
import java.io.EOFException;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.plugins.IClassLoadingPlugin;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.json.simple.JSONObject;

/** This class will hand out value meta objects from the plugin registry. */
public class ValueMetaFactory {

  private ValueMetaFactory() {
    // Factory
  }

  public static PluginRegistry pluginRegistry = PluginRegistry.getInstance();

  /**
   * What the plugin registry answers about value types, kept until the registry changes. Every
   * column of every result set asks for a value type, and asking the registry costs a scan over the
   * plugins and a class lookup each time.
   */
  private static volatile Snapshot snapshot;

  /**
   * One immutable picture of the value type plugins, taken at one {@link
   * PluginRegistry#getChangeCount()}. Only the constructors are filled in lazily, as each type is
   * first created.
   */
  private static final class Snapshot {
    private final PluginRegistry registry;
    private final long changeCount;

    /** As {@link PluginRegistry#getPlugin(Class, String)} finds them: the first plugin wins. */
    private final Map<Integer, IPlugin> pluginsById = new HashMap<>();

    /** The name of the first plugin whose main id is the type. */
    private final Map<Integer, String> namesById = new HashMap<>();

    /** The main id of the first plugin with that name, matched ignoring case. */
    private final Map<String, String> idsByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private final List<IPlugin> plugins;

    /**
     * The no-argument constructor of each type's class, taken from a class the registry loaded, so
     * through the plugin's own class loader, with the class name it was loaded for. Only filled in
     * lazily, as each type is first created.
     */
    private final Map<Integer, KnownConstructor> constructors = new ConcurrentHashMap<>();

    private Snapshot(PluginRegistry registry, long changeCount) {
      this.registry = registry;
      this.changeCount = changeCount;
      this.plugins = List.copyOf(registry.getPlugins(ValueMetaPluginType.class));
      for (IPlugin plugin : plugins) {
        String[] ids = plugin.getIds();
        for (String id : ids) {
          Integer type = asTypeId(id);
          if (type != null) {
            pluginsById.putIfAbsent(type, plugin);
          }
        }
        Integer mainType = asTypeId(ids[0]);
        if (mainType != null) {
          namesById.putIfAbsent(mainType, plugin.getName());
        }
        if (plugin.getName() != null) {
          idsByName.putIfAbsent(plugin.getName(), ids[0]);
        }
      }
    }

    /** The id as the int it stands for, or null when no int prints as that exact id. */
    private static Integer asTypeId(String id) {
      if (id == null) {
        return null;
      }
      try {
        int type = Integer.parseInt(id);
        return Integer.toString(type).equals(id) ? type : null;
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private IValueMeta newValueMeta(int type) throws HopPluginException {
      IPlugin plugin = pluginsById.get(type);
      if (plugin == null) {
        throw new HopPluginException("Unable to locate value meta plugin of type (id) " + type);
      }
      // A plugin that loads its own classes, or whose class comes from a class factory (no class
      // name in its class map), is asked every time: it may hand out something other than a fresh
      // instance of one class. The class map is read on every call, so a class merged into the
      // plugin later is picked up as the registry would.
      String className =
          plugin instanceof IClassLoadingPlugin ? null : plugin.getClassMap().get(IValueMeta.class);
      if (className != null) {
        KnownConstructor known = constructors.get(type);
        if (known != null && known.className().equals(className)) {
          return PluginRegistry.newInstance(known.constructor());
        }
      }
      IValueMeta valueMeta = registry.loadClass(plugin, IValueMeta.class);
      if (className != null && className.equals(valueMeta.getClass().getName())) {
        try {
          Constructor<? extends IValueMeta> loaded = valueMeta.getClass().getDeclaredConstructor();
          if (loaded.canAccess(null)) {
            constructors.put(type, new KnownConstructor(className, loaded));
          }
        } catch (NoSuchMethodException | SecurityException e) {
          // Not reachable for a class the registry just instantiated; keep loading it each time.
        }
      }
      return valueMeta;
    }
  }

  /** A constructor of a value meta class, and the class name the plugin mapped it from. */
  private record KnownConstructor(
      String className, Constructor<? extends IValueMeta> constructor) {}

  /**
   * @return the current snapshot, or null when {@link #pluginRegistry} is not the real registry (a
   *     test double), which is then asked every time as before
   */
  private static Snapshot snapshot() {
    PluginRegistry registry = pluginRegistry;
    if (registry != PluginRegistry.getInstance()) {
      return null;
    }
    // Read the change count before the plugins: a change in between makes the next call rebuild.
    long changeCount = registry.getChangeCount();
    Snapshot current = snapshot;
    if (current == null || current.registry != registry || current.changeCount != changeCount) {
      current = new Snapshot(registry, changeCount);
      snapshot = current;
    }
    return current;
  }

  public static IValueMeta createValueMeta(String name, int type, int length, int precision)
      throws HopPluginException {
    IValueMeta valueMeta;
    Snapshot current = snapshot();
    if (current != null) {
      valueMeta = current.newValueMeta(type);
    } else {
      IPlugin plugin = pluginRegistry.getPlugin(ValueMetaPluginType.class, String.valueOf(type));
      if (plugin == null) {
        throw new HopPluginException("Unable to locate value meta plugin of type (id) " + type);
      }
      valueMeta = pluginRegistry.loadClass(plugin, IValueMeta.class);
    }
    valueMeta.setName(name);
    valueMeta.setLength(length, precision);
    return valueMeta;
  }

  public static IValueMeta createValueMeta(String name, int type) throws HopPluginException {
    return createValueMeta(name, type, -1, -1);
  }

  public static IValueMeta createValueMeta(int type) throws HopPluginException {
    return createValueMeta(null, type, -1, -1);
  }

  public static IValueMeta cloneValueMeta(IValueMeta source) throws HopPluginException {
    return cloneValueMeta(source, source.getType());
  }

  public static IValueMeta cloneValueMeta(IValueMeta source, int targetType)
      throws HopPluginException {
    IValueMeta target = null;

    // If we're Cloneable and not changing types, call clone()
    if (source.getType() == targetType) {
      target = source.clone();
    } else {
      target =
          createValueMeta(source.getName(), targetType, source.getLength(), source.getPrecision());
    }

    cloneInfo(source, target);

    return target;
  }

  /**
   * Creates the String value metadata to parse text into {@code target} with: a constant, a default
   * value, a lookup key or a value read from a file. Pass it as the source metadata, for example
   * {@code target.convertData(parseMeta, text)} or {@code target.convertDataFromString(text,
   * parseMeta, nullIf, ifNull, trimType)}.
   *
   * <p>The result carries everything that decides how text is parsed: the conversion mask, the
   * decimal, grouping and currency symbols, the trim type, the string encoding, the date format
   * locale, time zone and leniency, and lenient number parsing. It always has normal storage: the
   * text to parse is a plain {@code String}, also when the target is a lazy (binary string) field.
   *
   * @param target the field the text is parsed into
   * @return a new String value meta, safe to change
   * @throws HopPluginException if the String value type can't be loaded
   */
  public static IValueMeta createParseMeta(IValueMeta target) throws HopPluginException {
    IValueMeta parseMeta = cloneValueMeta(target, IValueMeta.TYPE_STRING);
    // cloneInfo() drops the currency symbol, which a mask with a currency sign needs to parse
    // the text the field itself writes
    parseMeta.setCurrencySymbol(target.getCurrencySymbol());
    // cloneInfo() copies the target's storage: a lazy target would make every parse fail with a
    // ClassCastException, because the text is a String and not a byte[]
    parseMeta.setStorageType(IValueMeta.STORAGE_TYPE_NORMAL);
    parseMeta.setStorageMetadata(null);
    parseMeta.setIndex(null);
    // A String meta parses with the mask or the default parse masks; its length plays no part
    parseMeta.setLength(-1, -1);
    return parseMeta;
  }

  /**
   * Creates the parse metadata of every field in {@code target}, in the same order: see {@link
   * #createParseMeta(IValueMeta)}.
   *
   * @param target the row metadata of the fields the text is parsed into
   * @return new row metadata with one String value meta per field
   * @throws HopValueException if the String value type can't be loaded
   */
  public static IRowMeta createParseRowMeta(IRowMeta target) throws HopValueException {
    List<IValueMeta> parseMetas = new ArrayList<>(target.size());
    try {
      for (IValueMeta valueMeta : target.getValueMetaList()) {
        parseMetas.add(createParseMeta(valueMeta));
      }
    } catch (HopPluginException e) {
      throw new HopValueException(e);
    }
    IRowMeta parseRowMeta = new RowMeta();
    parseRowMeta.setValueMetaList(parseMetas);
    return parseRowMeta;
  }

  public static void cloneInfo(IValueMeta source, IValueMeta target) throws HopPluginException {
    target.setConversionMask(source.getConversionMask());
    target.setDecimalSymbol(source.getDecimalSymbol());
    target.setGroupingSymbol(source.getGroupingSymbol());
    target.setStorageType(source.getStorageType());
    if (source.getStorageMetadata() != null) {
      target.setStorageMetadata(
          cloneValueMeta(source.getStorageMetadata(), source.getStorageMetadata().getType()));
    }
    target.setStringEncoding(source.getStringEncoding());
    target.setTrimType(source.getTrimType());
    target.setRoundingType(source.getRoundingType());
    target.setDateFormatLenient(source.isDateFormatLenient());
    target.setDateFormatLocale(source.getDateFormatLocale());
    target.setDateFormatTimeZone(source.getDateFormatTimeZone());
    target.setLenientStringToNumber(source.isLenientStringToNumber());
    target.setLargeTextField(source.isLargeTextField());
    target.setComments(source.getComments());
    target.setCaseInsensitive(source.isCaseInsensitive());
    target.setCollatorDisabled(source.isCollatorDisabled());
    target.setCollatorStrength(source.getCollatorStrength());
    target.setIndex(source.getIndex());

    target.setOrigin(source.getOrigin());

    target.setOriginalAutoIncrement(source.isOriginalAutoIncrement());
    target.setOriginalColumnType(source.getOriginalColumnType());
    target.setOriginalColumnTypeName(source.getOriginalColumnTypeName());
    target.setOriginalNullable(source.isOriginalNullable());
    target.setOriginalPrecision(source.getOriginalPrecision());
    target.setOriginalScale(source.getOriginalScale());
    target.setOriginalSigned(source.isOriginalSigned());
  }

  public static String[] getValueMetaNames() {
    Snapshot current = snapshot();
    if (current != null) {
      return valueMetaNames(current.plugins);
    }
    return valueMetaNames(pluginRegistry.getPlugins(ValueMetaPluginType.class));
  }

  private static String[] valueMetaNames(List<IPlugin> plugins) {
    List<String> strings = new ArrayList<>();
    for (IPlugin plugin : plugins) {
      int id = Integer.parseInt(plugin.getIds()[0]);
      if (id > 0 && id != IValueMeta.TYPE_SERIALIZABLE) {
        strings.add(plugin.getName());
      }
    }
    return strings.toArray(new String[strings.size()]);
  }

  public static String[] getAllValueMetaNames() {
    Snapshot current = snapshot();
    if (current != null) {
      return allValueMetaNames(current.plugins);
    }
    return allValueMetaNames(pluginRegistry.getPlugins(ValueMetaPluginType.class));
  }

  private static String[] allValueMetaNames(List<IPlugin> plugins) {
    List<String> strings = new ArrayList<>();
    for (IPlugin plugin : plugins) {
      String id = plugin.getIds()[0];
      if (!("0".equals(id))) {
        strings.add(plugin.getName());
      }
    }
    return strings.toArray(new String[strings.size()]);
  }

  public static String getValueMetaName(int type) {
    Snapshot current = snapshot();
    if (current != null) {
      return current.namesById.getOrDefault(type, "-");
    }
    for (IPlugin plugin : pluginRegistry.getPlugins(ValueMetaPluginType.class)) {
      if (Integer.toString(type).equals(plugin.getIds()[0])) {
        return plugin.getName();
      }
    }
    return "-";
  }

  public static int getIdForValueMeta(String valueMetaName) {
    Snapshot current = snapshot();
    if (current != null) {
      String id = valueMetaName == null ? null : current.idsByName.get(valueMetaName);
      return id == null ? IValueMeta.TYPE_NONE : Integer.parseInt(id);
    }
    for (IPlugin plugin : pluginRegistry.getPlugins(ValueMetaPluginType.class)) {
      if (valueMetaName != null && valueMetaName.equalsIgnoreCase(plugin.getName())) {
        return Integer.parseInt(plugin.getIds()[0]);
      }
    }
    return IValueMeta.TYPE_NONE;
  }

  public static List<IValueMeta> getValueMetaPluginClasses() throws HopPluginException {

    List<IValueMeta> list = new ArrayList<>();

    List<IPlugin> plugins = pluginRegistry.getPlugins(ValueMetaPluginType.class);
    for (IPlugin plugin : plugins) {
      IValueMeta iValueMeta = (IValueMeta) pluginRegistry.loadClass(plugin);
      list.add(iValueMeta);
    }

    return list;
  }

  /**
   * This method makes attempt to guess hop value meta interface based on Object class. This may be
   * the case when we somehow obtain an Object as a result of any calculation, and we are trying to
   * assign some ValueMeta for it.
   *
   * <p>As an example - we have target value meta Number (which is java Double under the hood) and
   * value as a BigDecimal.<br>
   * This BigDecimal can be converted to a Double value.<br>
   * we have {@link IValueMeta#convertData(IValueMeta, Object)} call for this where is IValueMeta
   * object is our target value meta, Object is a BigDecimal - so we need to pass ValueMetaBigNumber
   * as a first parameter, value Object as a second and as the result we will have target Double
   * (ValueMetaNumber) value so we can safely put it into output rowset.
   *
   * <p>Something similar we had for ValueMetaBase.getValueFromSQLType(...) to guess value meta for
   * java sql type.
   *
   * <p>Currently this method does not have support for plugin value meta. Hope if this approach
   * will be found usable this may be implemented later.
   *
   * @param object object to guess applicable IValueMeta.
   * @return IValueMeta
   * @see IValueMeta if the hop value meta is recognized, null otherwise.
   */
  public static IValueMeta guessValueMetaInterface(Object object) {
    return switch (object) {
      case BigDecimal ignored -> new ValueMetaBigNumber();
      case Double ignored -> new ValueMetaNumber();
      case Long ignored -> new ValueMetaInteger();
      case String ignored -> new ValueMetaString();
      case Date ignored -> new ValueMetaDate();
      case Boolean ignored -> new ValueMetaBoolean();
      case byte[] ignored -> new ValueMetaBinary();
      case JsonNode ignored -> new ValueMetaJson();
      case null, default -> null;
    };
  }

  public static IValueMeta loadValueMetaFromJson(JSONObject jValue) throws HopPluginException {
    String name = (String) jValue.get("name");
    long type = (long) jValue.get("type");

    IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, (int) type);
    valueMeta.loadMetaFromJson(jValue);
    return valueMeta;
  }

  public static IValueMeta loadValueMeta(DataInputStream inputStream) throws HopFileException {
    try {
      int type = inputStream.readInt();
      IValueMeta valueMeta = createValueMeta(type);
      valueMeta.readMetaData(inputStream);
      return valueMeta;
    } catch (EOFException e) {
      throw new HopEofException(e);
    } catch (Exception e) {
      throw new HopFileException("Unable to read value metadata from input stream", e);
    }
  }
}
