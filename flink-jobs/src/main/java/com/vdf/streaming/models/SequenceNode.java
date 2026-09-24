package com.vdf.streaming.models;

import java.util.List;

/**
 * Nút chuỗi cho CEP.
 */
public record SequenceNode(
        String pattern,
        int minTime,
        int maxTime,
        String timeUnit,
        List<JoinKey> joinKeys,
        EventFilter first,
        EventFilter second
) implements ConditionNode {
}
