package org.apache.hop.core.action;

/**
 * This simply flags a class as being a context.
 * A context has a certain ID and name (i18n label like Transformation, GUI, MetaStore, ...)
 */
public interface IActionContext {

  String getId();

  String getName();
}
