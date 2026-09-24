package com.vdf.streaming.models;

import java.io.Serializable;
import java.util.List;

/**
 * Bộ lọc sự kiện.
 */
public record EventFilter(
        String source,
        List<TriggerCondition> filter
) implements Serializable {
}
