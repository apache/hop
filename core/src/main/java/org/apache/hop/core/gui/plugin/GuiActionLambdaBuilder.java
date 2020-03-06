package org.apache.hop.core.gui.plugin;

import java.lang.reflect.Method;

public class GuiActionLambdaBuilder<T> {

  public GuiActionLambdaBuilder() {
  }

  public GuiAction createLambda( GuiAction action, Object methodParent, T methodParameter ) {
    if (action.getMethodName()==null) {
      throw new RuntimeException( "We need a method to execute this action" );
    }
    try {
      Method method = methodParent.getClass().getMethod( action.getMethodName(), methodParameter.getClass() );
      if (method==null) {
        throw new RuntimeException( "Unable to find method "+action.getMethodName()+" with parameter "+methodParameter.getClass().getName()+" in class "+methodParent.getClass().getName() );
      }
      IGuiActionLambda<T> actionLambda = (objects) -> {
        try {
          method.invoke( methodParent, methodParameter );
        } catch(Exception e) {
          throw new RuntimeException( "Error executing method : "+action.getMethodName()+" in class "+methodParameter.getClass().getName(), e );
        }
      };
      action.setActionLambda( actionLambda );
      return action;
    } catch(Exception e) {
      throw new RuntimeException("Error creating action function for action : "+toString(), e);
    }
  }
}
