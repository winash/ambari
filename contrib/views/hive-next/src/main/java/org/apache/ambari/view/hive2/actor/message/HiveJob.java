package org.apache.ambari.view.hive2.actor.message;

import com.google.common.collect.ImmutableList;
import org.apache.ambari.view.ViewContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public abstract class HiveJob {

  private final String username;
  private final Type type;
  private final ViewContext viewContext;

  public HiveJob(Type type, String username,ViewContext viewContext) {
    this.type = type;
    this.username = username;
    this.viewContext = viewContext;
  }

  public String getUsername() {
    return username;
  }




  public Type getType() {
    return type;
  }



  public ViewContext getViewContext() {
    return viewContext;
  }


  public enum Type {
    SYNC,
    ASYNC
  }

}
