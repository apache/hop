package org.apache.hop.ui.core.bus;

import org.apache.hop.core.exception.HopException;

/**
 * This lamda allows you to be notified of certain events in the Hop GUI
 *
 * @param <T> The optional type of the event
 */
public interface IHopGuiEventListener<T> {

  void event(HopGuiEvent<T> event) throws HopException;

}
