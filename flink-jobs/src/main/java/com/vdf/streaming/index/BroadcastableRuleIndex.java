package com.vdf.streaming.index;

import com.vdf.streaming.models.CompiledRuleEnvelope;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.roaringbitmap.RoaringBitmap;

/**
 * Wrapper hỗ trợ broadcast state với cơ chế Copy-On-Write (COW).
 * Dùng AtomicReference để đọc lock-free, ghi bằng deepCopy + atomic swap.
 *
 * Luồng đọc (event matching): gọi getIndex() hoặc findCandidateRules() → lock-free.
 * Luồng ghi (CDC rule update): gọi applyUpdate/applyDelete → COW + atomic swap.
 *
 * Dùng trong BroadcastProcessFunction: luồng rule CDC broadcast sửa index,
 * luồng event đọc index để findCandidateRules.
 */
public class BroadcastableRuleIndex implements Serializable {
    private static final long serialVersionUID = 1L;

    // Sử dụng AtomicReference để đọc lock-free
    private final AtomicReference<InvertedIndexManager> indexRef;

    public BroadcastableRuleIndex() {
        this.indexRef = new AtomicReference<>(new InvertedIndexManager());
    }

    public BroadcastableRuleIndex(InvertedIndexManager initial) {
        this.indexRef = new AtomicReference<>(initial);
    }

    /**
     * Lấy InvertedIndexManager hiện tại để đọc (lock-free).
     * Luồng event matching gọi hàm này.
     */
    public InvertedIndexManager getIndex() {
        return indexRef.get();
    }

    /**
     * Kiểm tra version trước khi xử lý CDC event.
     *
     * @param ruleId          ID của rule
     * @param incomingVersion Version CDC đang đến
     * @return true nếu nên bỏ qua (đã có bản mới hơn hoặc bằng)
     */
    public boolean shouldSkipVersion(String ruleId, long incomingVersion) {
        return indexRef.get().shouldSkipCdcEvent(ruleId, incomingVersion);
    }

    /**
     * Áp dụng cập nhật rule theo kiểu COW (Copy-On-Write).
     * Tạo bản sao sâu → sửa đổi trên bản sao → atomic swap.
     *
     * @param rule Rule đã biên dịch
     * @param op   Loại thao tác CDC: "c" (create), "u" (update), "r" (read/snapshot)
     *             Hoặc dạng tường minh: "REGISTER", "UPDATE"
     */
    public void applyUpdate(CompiledRuleEnvelope rule, String op) {
        InvertedIndexManager currentIndex = indexRef.get();
        InvertedIndexManager newIndex = currentIndex.deepCopy();

        switch (op.toLowerCase()) {
            case "u", "update":
                newIndex.updateRule(rule);
                break;
            case "c", "r", "register":
            default:
                // "c" (create), "r" (snapshot/read) → đăng ký mới
                newIndex.registerRule(rule);
                break;
        }

        // Atomic pointer swap — luồng đọc cũ vẫn dùng bản cũ an toàn
        indexRef.set(newIndex);
    }

    /**
     * Áp dụng xóa/vô hiệu hóa rule theo kiểu COW.
     *
     * @param ruleId ID của rule cần xóa
     */
    public void applyDelete(String ruleId) {
        InvertedIndexManager currentIndex = indexRef.get();
        InvertedIndexManager newIndex = currentIndex.deepCopy();
        newIndex.unregisterRule(ruleId);
        indexRef.set(newIndex);
    }

    /**
     * Tìm rule ứng viên (delegate sang index hiện tại, lock-free).
     */
    public RoaringBitmap findCandidateRules(String source, String schemaVersion, Map<String, Object> eventFields) {
        return indexRef.get().findCandidateRules(source, schemaVersion, eventFields);
    }

    /**
     * Lấy rule theo slotId từ index hiện tại.
     */
    public CompiledRuleEnvelope getRule(int slotId) {
        return indexRef.get().getRule(slotId);
    }

    /**
     * Lấy rule theo ruleId từ index hiện tại.
     */
    public CompiledRuleEnvelope getRuleById(String ruleId) {
        return indexRef.get().getRuleById(ruleId);
    }
}
