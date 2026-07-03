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
package org.apache.hop.pipeline.transforms.fake;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import net.datafaker.Faker;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;

/**
 * Reflection-driven catalog of every DataFaker generator that the Fake transform can expose.
 *
 * <p>Rather than maintaining a hand-written list of providers, the catalog walks the DataFaker
 * {@code Faker} class: every zero-argument accessor that returns a provider becomes a category, and
 * every public method on that provider whose return type and parameters the transform can handle
 * becomes a {@link Generator}. This automatically picks up new providers and parameterized methods
 * as the library evolves.
 *
 * <p>The same class also centralizes the type plumbing shared by the dialog, the {@code FakeMeta}
 * row-metadata and the {@code Fake} runtime: value-meta mapping, argument conversion and overload
 * resolution.
 */
public final class FakerCatalog {

  private static final String PROVIDER_PACKAGE = "net.datafaker.providers.";

  /** Methods every object exposes that are never useful as data generators. */
  private static final Set<String> IGNORED_METHODS =
      Set.of(
          "toString", "equals", "hashCode", "getFaker", "getClass", "wait", "notify", "notifyAll");

  private FakerCatalog() {
    // utility class
  }

  /** Initialization-on-demand holder: the catalog is discovered once, on first access. */
  private static final class Holder {
    static final List<Generator> GENERATORS = discover();
  }

  /** A single callable generator: a provider method reachable from {@code Faker}. */
  public record Generator(String category, String categoryLabel, Method method) {

    public String functionName() {
      return method.getName();
    }

    public Class<?>[] parameterTypes() {
      return method.getParameterTypes();
    }

    public Class<?> returnType() {
      return method.getReturnType();
    }

    public boolean hasParameters() {
      return method.getParameterCount() > 0;
    }

    /** Comma-separated simple parameter type names, e.g. {@code int, int}. */
    public String parameterSignature() {
      StringBuilder sb = new StringBuilder();
      for (Class<?> p : parameterTypes()) {
        if (!sb.isEmpty()) {
          sb.append(", ");
        }
        sb.append(p.getSimpleName());
      }
      return sb.toString();
    }

    /** Human label, e.g. {@code numberBetween(int, int)}. */
    public String displayName() {
      return functionName() + "(" + parameterSignature() + ")";
    }

    /**
     * A friendly title for this generator: the curated one from {@link FakerDescriptors} when
     * available, otherwise the function name turned into words (e.g. {@code numberBetween} ->
     * "Number Between").
     */
    public String displayTitle() {
      String curated = FakerDescriptors.get(category + "." + functionName() + ".title");
      return curated != null ? curated : prettyLabel(functionName());
    }

    /** A curated one-line description, or an empty string when none has been written. */
    public String description() {
      String curated = FakerDescriptors.get(category + "." + functionName() + ".description");
      return curated != null ? curated : "";
    }

    /**
     * The curated label for parameter {@code index}, or {@code null} when none is defined.
     *
     * <p>Two keys are tried, most specific first: the type-signature key {@code
     * category.function.<type1>-<type2>....param.<index>} disambiguates overloads that share an
     * arity but differ in parameter types (e.g. {@code date.future(int, TimeUnit, String)} versus
     * {@code date.future(int, int, TimeUnit)}); the arity key {@code
     * category.function.<count>.param.<index>} covers the common case where all same-arity
     * overloads mean the same thing (e.g. {@code numberBetween}).
     */
    private String curatedParameterLabel(int index) {
      String bySignature = FakerDescriptors.get(paramKeyBase(true) + ".param." + index);
      if (bySignature != null) {
        return bySignature;
      }
      return FakerDescriptors.get(paramKeyBase(false) + ".param." + index);
    }

    private String paramKeyBase(boolean bySignature) {
      StringBuilder sb = new StringBuilder(category).append('.').append(functionName()).append('.');
      if (bySignature) {
        Class<?>[] types = parameterTypes();
        for (int i = 0; i < types.length; i++) {
          if (i > 0) {
            sb.append('-');
          }
          sb.append(types[i].getSimpleName());
        }
      } else {
        sb.append(parameterTypes().length);
      }
      return sb.toString();
    }

    /**
     * A friendly label for parameter {@code index}: the curated label from {@link FakerDescriptors}
     * when available, otherwise a generic "Parameter N". The parameter type is always appended.
     */
    public String parameterLabel(int index) {
      String curated = curatedParameterLabel(index);
      String base = curated != null ? curated : "Parameter " + (index + 1);
      return base + " (" + parameterTypes()[index].getSimpleName() + ")";
    }

    /**
     * A compact label for parameter {@code index}, meant for inline display in a list: the curated
     * descriptor label with any parenthetical clarification trimmed off (e.g. {@code "Minimum
     * (inclusive)"} -> {@code "Minimum"}), or the simple parameter type name when nothing has been
     * curated - so the list shows meaningful names where we have them and the raw type otherwise.
     */
    public String shortParameterLabel(int index) {
      String curated = curatedParameterLabel(index);
      if (curated == null) {
        return parameterTypes()[index].getSimpleName();
      }
      int paren = curated.indexOf('(');
      return paren > 0 ? curated.substring(0, paren).trim() : curated;
    }

    /** Comma-separated {@link #shortParameterLabel} values, e.g. {@code Minimum, Maximum}. */
    public String parameterDisplaySignature() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < parameterTypes().length; i++) {
        if (!sb.isEmpty()) {
          sb.append(", ");
        }
        sb.append(shortParameterLabel(i));
      }
      return sb.toString();
    }
  }

  /** A generator resolved against a live {@code Faker} instance, ready to produce values. */
  public static final class BoundGenerator {
    private final String label;
    private final Object provider;
    private final Method method;
    private final Object[] arguments;
    private final int valueMetaType;

    private BoundGenerator(
        String label, Object provider, Method method, Object[] arguments, int valueMetaType) {
      this.label = label;
      this.provider = provider;
      this.method = method;
      this.arguments = arguments;
      this.valueMetaType = valueMetaType;
    }

    /** Invoke the generator and coerce the result to the Hop value-meta native type. */
    public Object produce() throws HopException {
      try {
        return coerce(method.invoke(provider, arguments), valueMetaType);
      } catch (Exception e) {
        throw new HopException("Error generating fake value for '" + label + "'", e);
      }
    }

    public int getValueMetaType() {
      return valueMetaType;
    }
  }

  /** All discoverable generators, sorted by category then function. Discovered once and cached. */
  public static List<Generator> getGenerators() {
    return Holder.GENERATORS;
  }

  /**
   * The providers (categories) the browser shows by default. DataFaker exposes hundreds of
   * providers, many of them novelty/fandom sets (Harry Potter, Zelda, ...) that are rarely useful
   * for realistic test data. Everything not listed here is still reachable via the browser's "Show
   * all" toggle - this is purely about what is <em>surfaced by default</em>, not what exists. Edit
   * freely: an unknown name simply matches nothing.
   */
  private static final Set<String> FEATURED_CATEGORIES =
      Set.of(
          "name",
          "address",
          "phoneNumber",
          "internet",
          "number",
          "date",
          "company",
          "business",
          "commerce",
          "finance",
          "money",
          "color",
          "job",
          "idNumber",
          "code",
          "file",
          "lorem",
          "text",
          "country",
          "nation",
          "demographic",
          "educator",
          "university",
          "medication",
          "disease",
          "hacker",
          "app",
          "avatar",
          "barcode",
          "book",
          "animal",
          "food",
          "weather");

  /**
   * Whether a provider (category accessor, e.g. {@code name}) is shown by default in the browser.
   */
  public static boolean isFeaturedCategory(String category) {
    return FEATURED_CATEGORIES.contains(category);
  }

  private static List<Generator> discover() {
    List<Generator> result = new ArrayList<>();

    // Every zero-arg accessor on Faker that returns a provider object is a category.
    Map<String, Class<?>> accessors = new TreeMap<>();
    for (Method m : Faker.class.getMethods()) {
      if (m.getParameterCount() == 0
          && Modifier.isPublic(m.getModifiers())
          && !Modifier.isStatic(m.getModifiers())
          && m.getReturnType().getName().startsWith(PROVIDER_PACKAGE)) {
        accessors.putIfAbsent(m.getName(), m.getReturnType());
      }
    }

    for (Map.Entry<String, Class<?>> entry : accessors.entrySet()) {
      String accessor = entry.getKey();
      String label = prettyLabel(accessor);
      Class<?> providerClass = entry.getValue();

      // De-duplicate overrides/bridges that getMethods() may report more than once.
      Map<String, Method> unique = new LinkedHashMap<>();
      for (Method m : providerClass.getMethods()) {
        if (!isUsableGeneratorMethod(m)) {
          continue;
        }
        unique.putIfAbsent(m.getName() + signatureKey(m), m);
      }
      for (Method m : unique.values()) {
        result.add(new Generator(accessor, label, m));
      }
    }

    result.sort(
        Comparator.comparing(Generator::categoryLabel)
            .thenComparing(Generator::functionName)
            .thenComparingInt(g -> g.parameterTypes().length));
    return result;
  }

  private static boolean isUsableGeneratorMethod(Method m) {
    if (!Modifier.isPublic(m.getModifiers())
        || Modifier.isStatic(m.getModifiers())
        || m.isSynthetic()
        || m.isBridge()
        || m.isAnnotationPresent(Deprecated.class)
        || IGNORED_METHODS.contains(m.getName())
        || m.getDeclaringClass() == Object.class) {
      return false;
    }
    if (valueMetaType(m.getReturnType()) < 0) {
      return false;
    }
    for (Class<?> p : m.getParameterTypes()) {
      if (!isSupportedParameterType(p)) {
        return false;
      }
    }
    return true;
  }

  private static String signatureKey(Method m) {
    StringBuilder sb = new StringBuilder();
    for (Class<?> p : m.getParameterTypes()) {
      sb.append('#').append(p.getName());
    }
    return sb.toString();
  }

  /** Turn a camelCase accessor into a readable label, e.g. {@code phoneNumber} -> Phone Number. */
  public static String prettyLabel(String camel) {
    if (Utils.isEmpty(camel)) {
      return camel;
    }
    String spaced = camel.replaceAll("(?<=[a-z0-9])(?=[A-Z])", " ");
    return Character.toUpperCase(spaced.charAt(0)) + spaced.substring(1);
  }

  // ---------------------------------------------------------------------------------------------
  // Parameter types
  // ---------------------------------------------------------------------------------------------

  /** A method is exposable only when all of its parameters are types the dialog can edit. */
  public static boolean isSupportedParameterType(Class<?> c) {
    if (c.isPrimitive()) {
      return c != void.class;
    }
    return c == String.class || CharSequence.class.isAssignableFrom(c) || c.isEnum();
  }

  /** Canonical, persistable name for a parameter type (see {@link FakeArgument#getType()}). */
  public static String parameterTypeName(Class<?> c) {
    if (c.isPrimitive()) {
      return c.getName();
    }
    if (c == String.class) {
      return "string";
    }
    return c.getName();
  }

  /** Inverse of {@link #parameterTypeName(Class)}. */
  public static Class<?> parameterClass(String typeName) throws ClassNotFoundException {
    return switch (typeName) {
      case "int" -> int.class;
      case "long" -> long.class;
      case "double" -> double.class;
      case "float" -> float.class;
      case "short" -> short.class;
      case "byte" -> byte.class;
      case "boolean" -> boolean.class;
      case "char" -> char.class;
      case "string" -> String.class;
      default -> Class.forName(typeName);
    };
  }

  /** Convert an (already variable-resolved) string value into a typed argument object. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Object convertArgument(String typeName, String value) throws HopException {
    try {
      Class<?> c = parameterClass(typeName);
      String v = value == null ? "" : value.trim();
      if (c == int.class || c == Integer.class) {
        return Integer.valueOf(v);
      } else if (c == long.class || c == Long.class) {
        return Long.valueOf(v);
      } else if (c == double.class || c == Double.class) {
        return Double.valueOf(v);
      } else if (c == float.class || c == Float.class) {
        return Float.valueOf(v);
      } else if (c == short.class || c == Short.class) {
        return Short.valueOf(v);
      } else if (c == byte.class || c == Byte.class) {
        return Byte.valueOf(v);
      } else if (c == boolean.class || c == Boolean.class) {
        return Boolean.valueOf(v);
      } else if (c == char.class || c == Character.class) {
        return v.isEmpty() ? ' ' : v.charAt(0);
      } else if (c.isEnum()) {
        return Enum.valueOf((Class<Enum>) c, v);
      } else {
        // String / CharSequence: keep the raw (untrimmed) value
        return value == null ? "" : value;
      }
    } catch (Exception e) {
      throw new HopException(
          "Unable to convert argument value '" + value + "' to type '" + typeName + "'", e);
    }
  }

  /** A sensible default value to pre-fill a parameter editor with. */
  public static String defaultArgumentValue(Class<?> c) {
    if (c == double.class || c == float.class || c == Double.class || c == Float.class) {
      return "1.0";
    }
    if (c == boolean.class || c == Boolean.class) {
      return "true";
    }
    if (c == char.class || c == Character.class) {
      return "a";
    }
    if (c.isPrimitive() || Number.class.isAssignableFrom(c)) {
      return "1";
    }
    if (c.isEnum()) {
      Object[] constants = c.getEnumConstants();
      return constants.length > 0 ? ((Enum<?>) constants[0]).name() : "";
    }
    return "";
  }

  /**
   * Default values to pre-fill a whole parameter list. Numeric parameters get generous, strictly
   * increasing values (8, 16, 32, ...) so that {@code (min, max)}-style ranges are valid and there
   * is room for any "include uppercase/digit/..." flags - avoiding the "min > max" / "bound must be
   * positive" failures that a naive {@code 1} for every parameter would cause.
   */
  public static String[] defaultArgumentValues(Class<?>[] types) {
    String[] values = new String[types.length];
    int numericSeen = 0;
    for (int i = 0; i < types.length; i++) {
      if (isNumericParameter(types[i])) {
        long value = 8L << numericSeen; // 8, 16, 32, ...
        boolean decimal =
            types[i] == double.class
                || types[i] == float.class
                || types[i] == Double.class
                || types[i] == Float.class;
        values[i] = decimal ? value + ".0" : String.valueOf(value);
        numericSeen++;
      } else {
        values[i] = defaultArgumentValue(types[i]);
      }
    }
    return values;
  }

  private static boolean isNumericParameter(Class<?> c) {
    return c == int.class
        || c == long.class
        || c == short.class
        || c == byte.class
        || c == double.class
        || c == float.class
        || Number.class.isAssignableFrom(c);
  }

  // ---------------------------------------------------------------------------------------------
  // Return types / value metadata
  // ---------------------------------------------------------------------------------------------

  /**
   * Map a generator return type to a Hop value-meta type.
   *
   * @return one of the {@code IValueMeta.TYPE_*} constants, or -1 when the type can't be
   *     represented (arrays, collections, {@code InetAddress}, ...). Such methods are left out of
   *     the catalog.
   */
  public static int valueMetaType(Class<?> rt) {
    if (rt == void.class || rt == Void.class) {
      return -1;
    }
    if (rt == String.class
        || CharSequence.class.isAssignableFrom(rt)
        || rt == char.class
        || rt == Character.class
        || rt.isEnum()) {
      return IValueMeta.TYPE_STRING;
    }
    if (rt == int.class
        || rt == long.class
        || rt == short.class
        || rt == byte.class
        || rt == Integer.class
        || rt == Long.class
        || rt == Short.class
        || rt == Byte.class
        || rt == java.math.BigInteger.class) {
      return IValueMeta.TYPE_INTEGER;
    }
    if (rt == double.class
        || rt == float.class
        || rt == Double.class
        || rt == Float.class
        || rt == java.math.BigDecimal.class) {
      return IValueMeta.TYPE_NUMBER;
    }
    if (rt == boolean.class || rt == Boolean.class) {
      return IValueMeta.TYPE_BOOLEAN;
    }
    if (Date.class.isAssignableFrom(rt)
        || rt == LocalDate.class
        || rt == LocalDateTime.class
        || rt == Instant.class) {
      return IValueMeta.TYPE_DATE;
    }
    return -1;
  }

  /** Build a value-meta of the given Hop type. */
  public static IValueMeta createValueMeta(String name, int valueMetaType) {
    return switch (valueMetaType) {
      case IValueMeta.TYPE_INTEGER -> new ValueMetaInteger(name);
      case IValueMeta.TYPE_NUMBER -> new ValueMetaNumber(name);
      case IValueMeta.TYPE_BOOLEAN -> new ValueMetaBoolean(name);
      case IValueMeta.TYPE_DATE -> new ValueMetaDate(name);
      default -> new ValueMetaString(name);
    };
  }

  /** Coerce a raw faker return value to the native type expected by the Hop value-meta. */
  public static Object coerce(Object raw, int valueMetaType) {
    if (raw == null) {
      return null;
    }
    switch (valueMetaType) {
      case IValueMeta.TYPE_INTEGER:
        if (raw instanceof Number number) {
          return number.longValue();
        }
        if (raw instanceof Character c) {
          return (long) c;
        }
        return Long.valueOf(String.valueOf(raw));
      case IValueMeta.TYPE_NUMBER:
        if (raw instanceof Number number) {
          return number.doubleValue();
        }
        return Double.valueOf(String.valueOf(raw));
      case IValueMeta.TYPE_BOOLEAN:
        return raw instanceof Boolean b ? b : Boolean.valueOf(String.valueOf(raw));
      case IValueMeta.TYPE_DATE:
        if (raw instanceof Date date) {
          return date;
        }
        if (raw instanceof LocalDate ld) {
          return Date.from(ld.atStartOfDay(ZoneId.systemDefault()).toInstant());
        }
        if (raw instanceof LocalDateTime ldt) {
          return Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
        }
        if (raw instanceof Instant instant) {
          return Date.from(instant);
        }
        return raw;
      default:
        if (raw instanceof char[] chars) {
          return new String(chars);
        }
        return String.valueOf(raw);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Resolution against a Faker / field
  // ---------------------------------------------------------------------------------------------

  /**
   * Generators that JavaFaker exposed but DataFaker renamed, moved to another provider or dropped.
   * Each legacy {@code accessor.topic} is redirected to its closest DataFaker equivalent so that
   * pipelines built on the old library keep producing data without any change. Keys and values are
   * both {@code accessor.topic} pairs.
   */
  private static final Map<String, String> LEGACY_ALIASES =
      Map.ofEntries(
          // The Crypto provider was dropped; DataFaker offers the same hashes via "hashing".
          Map.entry("crypto.md5", "hashing.md5"),
          Map.entry("crypto.sha1", "hashing.sha1"),
          Map.entry("crypto.sha256", "hashing.sha256"),
          Map.entry("crypto.sha512", "hashing.sha512"),
          // Providers/methods that moved or were renamed.
          Map.entry("name.bloodGroup", "bloodtype.bloodGroup"),
          Map.entry("internet.avatar", "avatar.image"),
          Map.entry("internet.userAgentAny", "internet.userAgent"),
          Map.entry("address.firstName", "name.firstName"),
          Map.entry("address.lastName", "name.lastName"),
          Map.entry("commerce.color", "color.name"),
          Map.entry("hitchhikersGuideToTheGalaxy.specie", "hitchhikersGuideToTheGalaxy.species"),
          Map.entry("starTrek.specie", "starTrek.species"));

  /**
   * Resolve a stored {@code (type, topic)} pair to a callable DataFaker {@code (accessor, topic)}
   * pair, applying both the legacy enum-name translation ({@link FakerType}) and the alias table
   * for generators DataFaker renamed, moved or dropped.
   *
   * @return a two-element array: {@code [accessor, topic]}
   */
  public static String[] resolveGenerator(String type, String topic) {
    String accessor = FakerType.resolveAccessorMethod(type);
    String aliased = LEGACY_ALIASES.get(accessor + "." + topic);
    if (aliased != null) {
      int dot = aliased.indexOf('.');
      return new String[] {aliased.substring(0, dot), aliased.substring(dot + 1)};
    }
    return new String[] {accessor, topic};
  }

  /** Resolve the Hop value-meta type for a field, without needing a {@code Faker} instance. */
  public static int resolveValueMetaType(FakeField field) throws HopException {
    String[] generator = resolveGenerator(field.getType(), field.getTopic());
    try {
      Class<?> providerClass = Faker.class.getMethod(generator[0]).getReturnType();
      Method method = providerClass.getMethod(generator[1], argumentClasses(field));
      return valueMetaType(method.getReturnType());
    } catch (Exception e) {
      throw new HopException(
          "Unable to resolve fake data generator '"
              + field.getType()
              + "."
              + field.getTopic()
              + "'",
          e);
    }
  }

  /** Bind a field to a live {@code Faker}, converting and resolving its arguments. */
  public static BoundGenerator bind(Faker faker, FakeField field, IVariables variables)
      throws HopException {
    String[] generator = resolveGenerator(field.getType(), field.getTopic());
    try {
      Object provider = faker.getClass().getMethod(generator[0]).invoke(faker);
      List<FakeArgument> args = field.getArguments();
      Object[] values = new Object[args.size()];
      for (int i = 0; i < args.size(); i++) {
        FakeArgument a = args.get(i);
        values[i] = convertArgument(a.getType(), variables.resolve(Const.NVL(a.getValue(), "")));
      }
      Method method = provider.getClass().getMethod(generator[1], argumentClasses(field));
      return new BoundGenerator(
          generator[0] + "." + generator[1],
          provider,
          method,
          values,
          valueMetaType(method.getReturnType()));
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Unable to prepare fake data generator '"
              + field.getType()
              + "."
              + field.getTopic()
              + "'",
          e);
    }
  }

  private static Class<?>[] argumentClasses(FakeField field) throws ClassNotFoundException {
    List<FakeArgument> args = field.getArguments();
    Class<?>[] classes = new Class<?>[args.size()];
    for (int i = 0; i < classes.length; i++) {
      classes[i] = parameterClass(args.get(i).getType());
    }
    return classes;
  }

  /** Build a {@code Faker} for the given locale string (blank locale uses the JVM default). */
  public static Faker createFaker(String locale) {
    if (Utils.isEmpty(locale)) {
      return new Faker();
    }
    return new Faker(Locale.forLanguageTag(locale.replace('_', '-')));
  }
}
