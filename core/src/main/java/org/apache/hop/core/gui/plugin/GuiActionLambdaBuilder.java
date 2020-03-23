package org.apache.hop.core.gui.plugin;

import java.lang.reflect.Method;

public class GuiActionLambdaBuilder<T> {

  public GuiActionLambdaBuilder() {
  }


  /**
   * Create a copy of the given action and create an action lambda for it.
   *
   * @param guiAction
   * @param methodParent
   * @param methodParameter
   * @return The action with the appropriate lambda capable of executing the provided method in the given parent object
   */
  public GuiAction createLambda( GuiAction guiAction, Object methodParent, T methodParameter, IGuiRefresher refresher ) {
    if ( guiAction.getMethodName() == null ) {
      throw new RuntimeException( "We need a method to execute this action" );
    }
    // Create a copy to make sure we're not doing anything stupid
    //
    GuiAction action = new GuiAction( guiAction );

    try {
      Method method = methodParent.getClass().getMethod( action.getMethodName(), methodParameter.getClass() );
      if ( method == null ) {
        throw new RuntimeException( "Unable to find method " + action.getMethodName() + " with parameter " + methodParameter.getClass().getName() + " in class " + methodParent.getClass().getName() );
      }
      IGuiActionLambda<T> actionLambda = ( shiftClicked, controlClicked, objects ) -> {
        try {
          method.invoke( methodParent, methodParameter );
          if ( refresher != null ) {
            refresher.updateGui();
          }
        } catch ( Exception e ) {
          throw new RuntimeException( "Error executing method : " + action.getMethodName() + " in class " + methodParameter.getClass().getName(), e );
        }
      };
      action.setActionLambda( actionLambda );
      return action;
    } catch ( Exception e ) {
      throw new RuntimeException( "Error creating action function for action : " + toString(), e );
    }
  }
}
