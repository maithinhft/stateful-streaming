package com.vdf.streaming.models;

/**
 * Phân loại Rule theo cách thức đánh giá.
 * <ul>
 *   <li>STATELESS: Đánh giá in-memory trên event hiện tại</li>
 *   <li>STATEFUL: Đọc/ghi Keyed State (batch profile lookup, IN_DATASET)</li>
 *   <li>CEP: Đăng ký EventTime/ProcessingTime Timer + Sequence MapState</li>
 * </ul>
 */
public enum RuleType {
    STATELESS,
    STATEFUL,
    CEP
}
