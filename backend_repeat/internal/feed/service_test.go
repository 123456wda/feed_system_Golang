package feed

import (
	"testing"
)

// ===== K-way Merge 边界测试 =====

func TestMergeAndDedup_NilCursors(t *testing.T) {
	result := mergeAndDedup(nil, 10)
	if len(result) != 0 {
		t.Fatalf("expected empty result for nil cursors, got %d items", len(result))
	}
}

func TestMergeAndDedup_AllStreamsEmpty(t *testing.T) {
	cursors := make([]*streamCursor, 5)
	for i := range cursors {
		cursors[i] = &streamCursor{items: []VideoWithTime{}, source: "empty"}
	}
	result := mergeAndDedup(cursors, 10)
	if len(result) != 0 {
		t.Fatalf("expected empty result, got %d items", len(result))
	}
}

func TestMergeAndDedup_SingleStream(t *testing.T) {
	items := []VideoWithTime{
		{VideoID: 3, CreateTime: 300},
		{VideoID: 2, CreateTime: 200},
		{VideoID: 1, CreateTime: 100},
	}
	cursors := []*streamCursor{{items: items, source: "single"}}
	result := mergeAndDedup(cursors, 10)
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	expected := []uint{3, 2, 1}
	for i, id := range expected {
		if result[i] != id {
			t.Fatalf("position %d: expected %d, got %d", i, id, result[i])
		}
	}
}

// TestMergeAndDedup_DuplicateIDs 验证多个流中重复出现的 video ID 只保留一次（首次出现）。
// 这是推拉结合中 inbox + 多个大V outbox 可能出现重复内容时的去重保证。
func TestMergeAndDedup_DuplicateIDs(t *testing.T) {
	cursors := []*streamCursor{
		{items: []VideoWithTime{
			{VideoID: 1, CreateTime: 300},
			{VideoID: 2, CreateTime: 100},
		}, source: "stream1"},
		{items: []VideoWithTime{
			{VideoID: 1, CreateTime: 250}, // 重复 ID=1
			{VideoID: 3, CreateTime: 200},
		}, source: "stream2"},
	}
	result := mergeAndDedup(cursors, 10)
	if len(result) != 3 {
		t.Fatalf("expected 3 unique items, got %d (result=%v)", len(result), result)
	}
	// 期望：ID=1 (300) → ID=3 (200) → ID=2 (100)
	expected := []uint{1, 3, 2}
	for i, id := range expected {
		if result[i] != id {
			t.Fatalf("position %d: expected %d, got %d", i, id, result[i])
		}
	}
}

func TestMergeAndDedup_LimitLessThanTotal(t *testing.T) {
	cursors := []*streamCursor{
		{items: []VideoWithTime{
			{VideoID: 5, CreateTime: 500},
			{VideoID: 3, CreateTime: 300},
			{VideoID: 1, CreateTime: 100},
		}, source: "s1"},
		{items: []VideoWithTime{
			{VideoID: 4, CreateTime: 400},
			{VideoID: 2, CreateTime: 200},
		}, source: "s2"},
	}
	result := mergeAndDedup(cursors, 3)
	if len(result) != 3 {
		t.Fatalf("expected 3 items (limit), got %d", len(result))
	}
	expected := []uint{5, 4, 3}
	for i, id := range expected {
		if result[i] != id {
			t.Fatalf("position %d: expected %d, got %d", i, id, result[i])
		}
	}
}

func TestMergeAndDedup_LimitGreaterThanTotal(t *testing.T) {
	cursors := []*streamCursor{
		{items: []VideoWithTime{{VideoID: 2, CreateTime: 200}}, source: "s1"},
		{items: []VideoWithTime{{VideoID: 1, CreateTime: 100}}, source: "s2"},
	}
	result := mergeAndDedup(cursors, 100)
	if len(result) != 2 {
		t.Fatalf("expected 2 items (all available), got %d", len(result))
	}
}

// TestMergeAndDedup_StreamsOfDifferentLengths 验证不等长流能正确归并。
// 这是真实场景：inbox 可能有 50 条，某个大V只有 5 条，另一个大V有 30 条。
func TestMergeAndDedup_StreamsOfDifferentLengths(t *testing.T) {
	cursors := []*streamCursor{
		{items: []VideoWithTime{
			{VideoID: 10, CreateTime: 1000},
			{VideoID: 8, CreateTime: 800},
			{VideoID: 6, CreateTime: 600},
			{VideoID: 4, CreateTime: 400},
			{VideoID: 2, CreateTime: 200},
		}, source: "long"},
		{items: []VideoWithTime{
			{VideoID: 9, CreateTime: 900},
		}, source: "short"},
		{items: []VideoWithTime{
			{VideoID: 7, CreateTime: 700},
			{VideoID: 5, CreateTime: 500},
			{VideoID: 3, CreateTime: 300},
		}, source: "medium"},
	}
	result := mergeAndDedup(cursors, 9)
	if len(result) != 9 {
		t.Fatalf("expected 9 items, got %d", len(result))
	}
	// 完美交错降序
	expected := []uint{10, 9, 8, 7, 6, 5, 4, 3, 2}
	for i, id := range expected {
		if result[i] != id {
			t.Fatalf("position %d: expected %d, got %d (result=%v)", i, id, result[i], result)
		}
	}
}

func TestMergeAndDedup_ZeroLimit(t *testing.T) {
	cursors := []*streamCursor{
		{items: []VideoWithTime{{VideoID: 1, CreateTime: 100}}, source: "s1"},
	}
	result := mergeAndDedup(cursors, 0)
	if len(result) != 0 {
		t.Fatalf("expected empty result for limit=0, got %d", len(result))
	}
}

// TestMergeAndDedup_EqualTimestamps 验证相同时间戳时按入堆顺序输出（堆是稳定的最大堆）。
// 业务上时间戳来自 createTime 毫秒，理论上可能相同（同一毫秒内多个发布）。
func TestMergeAndDedup_EqualTimestamps(t *testing.T) {
	cursors := []*streamCursor{
		{items: []VideoWithTime{{VideoID: 1, CreateTime: 100}}, source: "s1"},
		{items: []VideoWithTime{{VideoID: 2, CreateTime: 100}}, source: "s2"},
		{items: []VideoWithTime{{VideoID: 3, CreateTime: 100}}, source: "s3"},
	}
	result := mergeAndDedup(cursors, 10)
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	// 所有 3 个 ID 都应出现，顺序不强制（堆对相等元素无稳定性保证）
	seen := map[uint]bool{}
	for _, id := range result {
		seen[id] = true
	}
	for _, expected := range []uint{1, 2, 3} {
		if !seen[expected] {
			t.Fatalf("missing expected ID %d in result %v", expected, result)
		}
	}
}

// ===== Benchmarks =====

// BenchmarkMergeAndDedup 模拟真实 Feed 场景：5 个流（1 inbox + 4 大V outbox），
// 每流 20 条数据，limit=10（典型分页大小）。
func BenchmarkMergeAndDedup(b *testing.B) {
	cursors := buildCursors(5, 20, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetCursors(cursors)
		_ = mergeAndDedup(cursors, 10)
	}
}

// BenchmarkMergeAndDedup_LargeStreams 模拟极端情况：10 流 × 100 条，含重复 ID。
// 用于评估 dedup 的开销以及堆操作的扩展性。
func BenchmarkMergeAndDedup_LargeStreams(b *testing.B) {
	cursors := buildCursors(10, 100, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetCursors(cursors)
		_ = mergeAndDedup(cursors, 50)
	}
}

// BenchmarkMergeAndDedup_ManyStreamsSmallLimit 模拟"很多大V，每人只取头部"的场景。
func BenchmarkMergeAndDedup_ManyStreamsSmallLimit(b *testing.B) {
	cursors := buildCursors(20, 10, false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetCursors(cursors)
		_ = mergeAndDedup(cursors, 5)
	}
}

func buildCursors(streamCount, perStream int, withDuplicates bool) []*streamCursor {
	cursors := make([]*streamCursor, streamCount)
	for i := range cursors {
		items := make([]VideoWithTime, perStream)
		for j := range items {
			id := uint(i*1000 + j + 1)
			if withDuplicates {
				// 让相邻流之间有部分重复 ID（模拟用户既关注大V又收到普通用户推送）
				id = uint((i*30 + j*3) + 1)
			}
			items[j] = VideoWithTime{
				VideoID:    id,
				CreateTime: int64(100000 - i*100 - j),
			}
		}
		cursors[i] = &streamCursor{items: items, source: "bench"}
	}
	return cursors
}

func resetCursors(cursors []*streamCursor) {
	for _, c := range cursors {
		c.pos = 0
	}
}
