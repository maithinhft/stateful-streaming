package com.vdf.streaming.index;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.roaringbitmap.RoaringBitmap;

import com.vdf.streaming.models.CompiledRuleEnvelope;
import com.vdf.streaming.models.CompiledTriggerCriteria;
import com.vdf.streaming.models.TriggerCondition;

/**
 * Trình quản lý cấp cao điều phối tất cả các đối tượng SourceVersionIndex.
 * Chịu trách nhiệm đăng ký, hủy đăng ký, cập nhật và tìm kiếm các rule candidates.
 */
public class InvertedIndexManager implements Serializable {
    private static final long serialVersionUID = 1L;

    // Map chứa SourceVersionIndex cho từng cặp source:schemaVersion
    // Dùng volatile reference để hỗ trợ pattern Copy-On-Write (COW)
    private volatile Map<String, SourceVersionIndex> indexMap;
    
    // Trình quản lý cấp phát slot cho các rule
    private SlotManager slotManager;

    public InvertedIndexManager() {
        this.indexMap = new HashMap<>();
        this.slotManager = new SlotManager();
    }

    private InvertedIndexManager(InvertedIndexManager other) {
        this.indexMap = new HashMap<>();
        for (Map.Entry<String, SourceVersionIndex> entry : other.indexMap.entrySet()) {
            this.indexMap.put(entry.getKey(), entry.getValue().deepCopy());
        }
        this.slotManager = other.slotManager.deepCopy();
    }

    /**
     * Đăng ký một rule mới.
     * Cấp phát slot từ slotManager. Phân tích các điều kiện (conditions) từ triggers 
     * và lập chỉ mục tương ứng trong SourceVersionIndex.
     */
    public void registerRule(CompiledRuleEnvelope rule) {
        int slotId = slotManager.allocateSlot(rule.getRuleId());
        rule.setSlotId(slotId); // Đồng bộ slotId thực tế vào envelope
        slotManager.setRule(slotId, rule);

        if (rule.getTriggers() != null) {
            for (CompiledTriggerCriteria trigger : rule.getTriggers()) {
                String key = buildIndexKey(trigger.getSource(), trigger.getSchemaVersion());
                SourceVersionIndex index = indexMap.computeIfAbsent(key, k -> new SourceVersionIndex());
                
                if (trigger.getDnfConditions() != null) {
                    for (List<TriggerCondition> andConditions : trigger.getDnfConditions()) {
                        for (TriggerCondition condition : andConditions) {
                            String field = condition.getField();
                            if (field == null && condition.getFields() != null && !condition.getFields().isEmpty()) {
                                field = condition.getFields().get(0);
                            }
                            String operator = condition.getOp();
                            Object value = condition.getValue();
                        
                        if (operator == null) continue;

                        switch (operator.toUpperCase()) {
                            case "==":
                            case "EQUALS":
                                index.addExactEntry(field, String.valueOf(value), slotId);
                                break;
                            case "IN":
                                if (value instanceof List) {
                                    index.addExactEntries(field, (List<?>) value, slotId);
                                }
                                break;
                            case "IN_DATASET":
                                index.addDatasetEntry(field, String.valueOf(value), slotId);
                                break;
                            case ">":
                            case "<":
                            case ">=":
                            case "<=":
                            case "!=":
                            case "BETWEEN":
                            case "CONTAINS":
                            case "STARTS_WITH":
                            case "ENDS_WITH":
                            case "NOT_STARTS_WITH":
                            case "NOT_ENDS_WITH":
                            case "NOT_CONTAINS":
                            case "IS_NULL":
                            case "IS_NOT_NULL":
                            case "LENGTH":
                                index.addComplexEntry(slotId);
                                break;
                            default:
                                index.addComplexEntry(slotId);
                                break;
                        }
                    }
                }
            }
        }
    }
    }

    /**
     * Hủy đăng ký một rule dựa vào ruleId.
     * Giải phóng slot trong slotManager. Các chỉ mục rác trong exactIndex 
     * sẽ bị bỏ qua (AND NOT) khi mask với freeSlotsBitmap.
     */
    public void unregisterRule(String ruleId) {
        int slotId = slotManager.getSlotId(ruleId);
        if (slotId != -1) {
            slotManager.releaseSlot(ruleId);
        }
    }

    /**
     * Cập nhật một rule.
     * Xóa slot cũ (bằng cách thiết lập bit trong freeSlotsBitmap thông qua unregisterRule) 
     * và sau đó đăng ký lại. Tránh scan O(N) trên tất cả bitmap.
     */
    public void updateRule(CompiledRuleEnvelope rule) {
        unregisterRule(rule.getRuleId());
        registerRule(rule);
    }

    /**
     * Tìm các rule ứng viên phù hợp với event được đưa vào.
     * Truy xuất SourceVersionIndex theo source và schemaVersion, 
     * sau đó filter theo eventFields và mask out các slot đã xóa.
     */
    public RoaringBitmap findCandidateRules(String source, String schemaVersion, Map<String, Object> eventFields) {
        String key = buildIndexKey(source, schemaVersion);
        SourceVersionIndex index = indexMap.get(key);
        
        if (index == null) {
            return new RoaringBitmap();
        }
        
        return index.queryCandidates(eventFields, slotManager.getFreeSlotsBitmap());
    }

    /**
     * Lấy rule từ slotManager bằng ruleId.
     */
    public CompiledRuleEnvelope getRuleById(String ruleId) {
        int slotId = slotManager.getSlotId(ruleId);
        if (slotId != -1) {
            return slotManager.getRule(slotId);
        }
        return null;
    }

    /**
     * Lấy rule từ slotManager bằng slotId.
     */
    public CompiledRuleEnvelope getRule(int slotId) {
        return slotManager.getRule(slotId);
    }

    /**
     * Kiểm tra xem event có nên bị bỏ qua do version nhỏ hơn version hiện tại của rule không.
     */
    public boolean shouldSkipCdcEvent(String ruleId, long incomingVersion) {
        CompiledRuleEnvelope existingRule = getRuleById(ruleId);
        if (existingRule != null && existingRule.getCdcVersion() >= incomingVersion) {
            return true;
        }
        return false;
    }

    public SlotManager getSlotManager() {
        return slotManager;
    }

    /**
     * Tạo bản sao sâu cho toàn bộ cấu trúc dữ liệu để phục vụ Copy-On-Write.
     */
    public InvertedIndexManager deepCopy() {
        return new InvertedIndexManager(this);
    }

    /**
     * Hàm hỗ trợ tạo key dạng source:schemaVersion
     */
    public String buildIndexKey(String source, String schemaVersion) {
        return source + ":" + schemaVersion;
    }

    public void printDebugInfo() {
        System.out.println("\n========== BÁO CÁO INVERTED INDEX ==========");
//        System.out.println("Tổng số Slot đã cấp phát (maxAllocatedIndex): " + slotManager.getMaxAllocatedIndex());
//        System.out.println("Các Slot đang trống (đã xóa): " + slotManager.getFreeSlotsBitmap().toString());
//        System.out.println("Các tập SourceVersionIndex:");
//        for (Map.Entry<String, SourceVersionIndex> entry : indexMap.entrySet()) {
//            System.out.println("  [+] " + entry.getKey());
//            entry.getValue().printDebugInfo();
//        }
        System.out.println("=============================================\n");
    }
}
