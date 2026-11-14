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

package org.apache.hop.pipeline.transforms.script;

import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.DelegatingMetaClass;
import groovy.lang.GroovyRuntimeException;
import groovy.lang.MetaClass;
import groovy.lang.MissingMethodException;
import groovy.lang.MissingPropertyException;
import groovy.lang.Script;
import groovy.lang.Tuple;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.codehaus.groovy.util.ManagedConcurrentValueMap;

/**
 * Runs the row script of a Groovy Script transform without going through the JSR-223 engine for
 * every row.
 *
 * <p>The engine's {@code CompiledScript.eval()} builds, for every call, a new script object, a
 * closure for each of its methods and a metaclass for that one object. The new metaclass makes
 * Groovy link every call in the script again, which is most of the cost of a short script. Here the
 * script class is compiled once and each row gets a new instance that is run directly, with the
 * same metaclass object as the row before.
 *
 * <p>What a script can see and do is kept the same as with the engine:
 *
 * <ul>
 *   <li>a new script object per row, so {@code @Field} variables start fresh for every row;
 *   <li>variables are read from and written to the script context, with the engine's own rules for
 *       {@code out} and {@code context};
 *   <li>a method the script does not have is looked up among the functions of the scripts the
 *       engine evaluated before, such as the start script, and then among the closures in the
 *       context, as the engine does;
 *   <li>the functions of the row script are known to the engine, for the start and end scripts;
 *   <li>an exception leaves as a {@link ScriptException} around the original one.
 * </ul>
 */
final class GroovyRowScript {

  private final Class<? extends Script> scriptClass;
  private final Constructor<? extends Script> constructor;
  private final Binding binding;
  private final ManagedConcurrentValueMap<String, Closure<?>> globalClosures;
  private final ScriptContext context;

  /** The metaclass of the script objects, see {@link #newScript()}. */
  private GlobalFunctionsMetaClass metaClass;

  /** The script object of the current row. */
  private Script lastScript;

  private GroovyRowScript(
      Class<? extends Script> scriptClass,
      Constructor<? extends Script> constructor,
      ScriptContext context,
      ManagedConcurrentValueMap<String, Closure<?>> globalClosures) {
    this.scriptClass = scriptClass;
    this.constructor = constructor;
    this.binding = new ContextBinding(context);
    this.globalClosures = globalClosures;
    this.context = context;
  }

  /**
   * @return the row script, or null when this is not Groovy's JSR-223 engine, the source is a class
   *     and not a script, or the engine's functions cannot be reached: the caller then keeps using
   *     the engine
   */
  static GroovyRowScript create(ScriptEngine engine, String source, ScriptContext context) {
    if (!(engine instanceof GroovyScriptEngineImpl groovyEngine) || context == null) {
      return null;
    }
    // The engine's own class loader: classes that the start script defined are visible.
    Class<?> parsed = groovyEngine.getClassLoader().parseClass(source);
    if (!Script.class.isAssignableFrom(parsed)) {
      return null;
    }
    Class<? extends Script> scriptClass = parsed.asSubclass(Script.class);
    ManagedConcurrentValueMap<String, Closure<?>> globalClosures = globalClosures(groovyEngine);
    if (globalClosures == null) {
      return null;
    }
    Constructor<? extends Script> constructor;
    try {
      constructor = scriptClass.getConstructor(Binding.class);
    } catch (NoSuchMethodException e) {
      return null;
    }
    GroovyRowScript rowScript =
        new GroovyRowScript(scriptClass, constructor, context, globalClosures);
    rowScript.registerFunctions();
    return rowScript;
  }

  /**
   * The functions of every script the engine evaluated, by name. The engine keeps them in a private
   * map, so this needs reflection.
   *
   * @return the map, or null when it cannot be reached
   */
  @SuppressWarnings("unchecked")
  private static ManagedConcurrentValueMap<String, Closure<?>> globalClosures(
      GroovyScriptEngineImpl engine) {
    try {
      Field field = GroovyScriptEngineImpl.class.getDeclaredField("globalClosures");
      field.setAccessible(true);
      return (ManagedConcurrentValueMap<String, Closure<?>>) field.get(engine);
    } catch (ReflectiveOperationException | RuntimeException e) {
      return null;
    }
  }

  /** Runs the script for one row and returns its result. */
  Object run() throws ScriptException {
    try {
      Script script = newScript();
      lastScript = script;
      return script.run();
    } catch (Exception e) {
      throw new ScriptException(e);
    }
  }

  /**
   * A new script object for the row. Like the engine, its metaclass wraps the one the object got
   * from its class. Unlike the engine, the wrapper is the same object from row to row as long as
   * the class keeps its metaclass: Groovy's call sites check the metaclass of the object they call,
   * and stay linked as long as it does not change.
   *
   * <p>The wrapper is set on the object, not registered for the class. Registering a metaclass
   * makes Groovy relink the calls of every Groovy script in the JVM, and a script that changes the
   * metaclass of its own class would remove it.
   */
  private Script newScript() {
    Script script;
    try {
      script = constructor.newInstance(binding);
    } catch (ReflectiveOperationException e) {
      // As InvokerHelper.createScript, which the engine uses, reports it
      throw new GroovyRuntimeException(
          "Failed to create Script instance for class: " + scriptClass + ". Reason: " + e, e);
    }
    MetaClass classMetaClass = script.getMetaClass();
    if (metaClass == null || metaClass.getAdaptee() != classMetaClass) {
      metaClass = new GlobalFunctionsMetaClass(classMetaClass, globalClosures, context);
    }
    script.setMetaClass(metaClass);
    return script;
  }

  /**
   * Tells the engine about the functions of the row script, as it does itself for every script it
   * runs: a function of the start script that calls one of the row script, and the end script, find
   * them there.
   */
  private void registerFunctions() {
    for (Method method : scriptClass.getMethods()) {
      String name = method.getName();
      globalClosures.put(name, new CurrentScriptFunction(name));
    }
  }

  /** A function of the row script, called on the script object of the current row. */
  private final class CurrentScriptFunction extends Closure<Object> {
    private static final long serialVersionUID = 1L;
    private final String name;

    CurrentScriptFunction(String name) {
      super(GroovyRowScript.class);
      this.name = name;
    }

    @Override
    public Object call(Object... args) {
      Script script = lastScript;
      if (script == null) {
        throw new MissingMethodException(name, scriptClass, args);
      }
      return InvokerHelper.invokeMethod(script, name, args);
    }
  }

  /** Variable lookup in the script context, as the engine's own binding does it. */
  private static final class ContextBinding extends Binding {
    private final transient ScriptContext context;

    ContextBinding(ScriptContext context) {
      super(context.getBindings(ScriptContext.ENGINE_SCOPE));
      this.context = context;
    }

    @Override
    public Object getVariable(String name) {
      synchronized (context) {
        int scope = context.getAttributesScope(name);
        if (scope != -1) {
          return context.getAttribute(name, scope);
        }
        // Script output goes to the context writer when no out variable is provided
        if ("out".equals(name)) {
          Writer writer = context.getWriter();
          if (writer != null) {
            return writer instanceof PrintWriter printWriter
                ? printWriter
                : new PrintWriter(writer, true);
          }
        }
        if ("context".equals(name)) {
          return context;
        }
      }
      throw new MissingPropertyException(name, getClass());
    }

    @Override
    public void setVariable(String name, Object value) {
      synchronized (context) {
        int scope = context.getAttributesScope(name);
        if (scope == -1) {
          scope = ScriptContext.ENGINE_SCOPE;
        }
        context.setAttribute(name, value, scope);
      }
    }
  }

  /**
   * Passes a method the script does not have on to the functions of the scripts the engine
   * evaluated, the start script's for one, and then to the closures in the context. This is the
   * engine's own lookup: what such a function throws reaches the script as it is.
   */
  private static final class GlobalFunctionsMetaClass extends DelegatingMetaClass {
    private final ManagedConcurrentValueMap<String, Closure<?>> globalClosures;
    private final ScriptContext context;

    GlobalFunctionsMetaClass(
        MetaClass delegate,
        ManagedConcurrentValueMap<String, Closure<?>> globalClosures,
        ScriptContext context) {
      super(delegate);
      this.globalClosures = globalClosures;
      this.context = context;
    }

    @Override
    public Object invokeMethod(Object object, String name, Object args) {
      if (args == null) {
        return invokeMethod(object, name, new Object[0]);
      }
      if (args instanceof Tuple<?> tuple) {
        return invokeMethod(object, name, tuple.toArray());
      }
      if (args instanceof Object[] array) {
        return invokeMethod(object, name, array);
      }
      return invokeMethod(object, name, new Object[] {args});
    }

    @Override
    public Object invokeMethod(Object object, String name, Object[] args) {
      try {
        return super.invokeMethod(object, name, args);
      } catch (MissingMethodException e) {
        if (!isAboutThisCall(e, object, name)) {
          throw e;
        }
        return callGlobal(name, args);
      }
    }

    @Override
    public Object invokeStaticMethod(Object object, String name, Object[] args) {
      try {
        return super.invokeStaticMethod(object, name, args);
      } catch (MissingMethodException e) {
        if (!isAboutThisCall(e, object, name)) {
          throw e;
        }
        return callGlobal(name, args);
      }
    }

    /**
     * Whether the method that is missing is the one called here, the check Groovy's own call sites
     * make. A method that is missing further down, inside the method that was found, must reach the
     * script as it is: looking it up among the functions would run the method that was found again.
     */
    private static boolean isAboutThisCall(MissingMethodException e, Object object, String name) {
      Class<?> type = object instanceof Class<?> c ? c : object.getClass();
      return name.equals(e.getMethod()) && e.getType() == type;
    }

    private Object callGlobal(String name, Object[] args) {
      Closure<?> closure = globalClosures.get(name);
      if (closure != null) {
        return closure.call(args);
      }
      Object value = context.getAttribute(name);
      if (value instanceof Closure<?> variable) {
        return variable.call(args);
      }
      throw new MissingMethodException(name, GroovyScriptEngineImpl.class, args);
    }
  }
}
