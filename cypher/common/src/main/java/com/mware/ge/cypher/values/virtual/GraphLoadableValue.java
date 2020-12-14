package com.mware.ge.cypher.values.virtual;

import com.mware.ge.Element;
import com.mware.ge.ElementType;

public interface GraphLoadableValue {
    void setGraphElement(Element element);
    ElementType getType();
    String id();
    boolean isLoaded();
}
