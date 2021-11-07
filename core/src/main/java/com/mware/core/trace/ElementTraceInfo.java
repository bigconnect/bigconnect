package com.mware.core.trace;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ElementTraceInfo {
    private String action;
    private String description;
    private long timestamp;
}
