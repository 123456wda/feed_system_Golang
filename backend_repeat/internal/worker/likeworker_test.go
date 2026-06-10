package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
)

// ===== process() JSON 解析与路由测试 =====
// process() 方法的前半段（JSON 解析、字段校验、Action 路由）不依赖数据库，
// 可以独立测试。applyLike/applyUnlike 依赖 GORM，需要集成测试环境。

func TestProcess_MalformedJSON(t *testing.T) {
	w := &LikeWorker{}
	// 不合法的 JSON → return nil（静默丢弃）
	if err := w.process(context.Background(), []byte("{invalid")); err != nil {
		t.Fatalf("expected nil for malformed JSON, got %v", err)
	}
}

func TestProcess_EmptyBody(t *testing.T) {
	w := &LikeWorker{}
	if err := w.process(context.Background(), []byte("")); err != nil {
		t.Fatalf("expected nil for empty body, got %v", err)
	}
}

func TestProcess_MissingUserID(t *testing.T) {
	w := &LikeWorker{}
	evt := rabbitmq.LikeEvent{
		Action:  "like",
		VideoID: 1,
	}
	body, _ := json.Marshal(evt)
	// user_id=0 → return nil（字段校验不通过，静默丢弃）
	if err := w.process(context.Background(), body); err != nil {
		t.Fatalf("expected nil for missing user_id, got %v", err)
	}
}

func TestProcess_MissingVideoID(t *testing.T) {
	w := &LikeWorker{}
	evt := rabbitmq.LikeEvent{
		Action: "like",
		UserID: 1,
	}
	body, _ := json.Marshal(evt)
	// video_id=0 → return nil
	if err := w.process(context.Background(), body); err != nil {
		t.Fatalf("expected nil for missing video_id, got %v", err)
	}
}

func TestProcess_UnknownAction(t *testing.T) {
	w := &LikeWorker{}
	evt := rabbitmq.LikeEvent{
		Action:  "unknown_action",
		UserID:  1,
		VideoID: 1,
	}
	body, _ := json.Marshal(evt)
	// 未知 action → return nil（default 分支，静默丢弃）
	if err := w.process(context.Background(), body); err != nil {
		t.Fatalf("expected nil for unknown action, got %v", err)
	}
}

func TestProcess_EmptyAction(t *testing.T) {
	w := &LikeWorker{}
	evt := rabbitmq.LikeEvent{
		UserID:  1,
		VideoID: 1,
	}
	body, _ := json.Marshal(evt)
	// action="" → return nil（default 分支）
	if err := w.process(context.Background(), body); err != nil {
		t.Fatalf("expected nil for empty action, got %v", err)
	}
}

// TestProcess_ValidLikeEvent 验证合法 like 事件会尝试调用 applyLike。
// 由于 applyLike 需要真实数据库，这里只验证 JSON 解析正确提取了字段。
func TestProcess_ValidLikeEvent_JSONParsing(t *testing.T) {
	// 直接测试 JSON 解析逻辑，不进入 applyLike
	evt := rabbitmq.LikeEvent{
		Action:     "like",
		UserID:     42,
		VideoID:    100,
		OccurredAt: time.Now(),
	}
	body, _ := json.Marshal(evt)

	var parsed rabbitmq.LikeEvent
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}
	if parsed.UserID != 42 {
		t.Errorf("expected user_id=42, got %d", parsed.UserID)
	}
	if parsed.VideoID != 100 {
		t.Errorf("expected video_id=100, got %d", parsed.VideoID)
	}
	if parsed.Action != "like" {
		t.Errorf("expected action=like, got %s", parsed.Action)
	}
}

// TestProcess_ValidUnlikeEvent_JSONParsing 验证 unlike 事件的 JSON 解析。
func TestProcess_ValidUnlikeEvent_JSONParsing(t *testing.T) {
	evt := rabbitmq.LikeEvent{
		Action:  "unlike",
		UserID:  42,
		VideoID: 100,
	}
	body, _ := json.Marshal(evt)

	var parsed rabbitmq.LikeEvent
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}
	if parsed.Action != "unlike" {
		t.Errorf("expected action=unlike, got %s", parsed.Action)
	}
}

// ===== LikeEvent JSON 序列化/反序列化往返测试 =====

func TestLikeEvent_RoundTrip(t *testing.T) {
	original := rabbitmq.LikeEvent{
		EventID:    "abc-123",
		Action:     "like",
		UserID:     999,
		VideoID:    888,
		OccurredAt: time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC),
	}

	body, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded rabbitmq.LikeEvent
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.EventID != original.EventID {
		t.Errorf("event_id mismatch: %s vs %s", decoded.EventID, original.EventID)
	}
	if decoded.Action != original.Action {
		t.Errorf("action mismatch: %s vs %s", decoded.Action, original.Action)
	}
	if decoded.UserID != original.UserID {
		t.Errorf("user_id mismatch: %d vs %d", decoded.UserID, original.UserID)
	}
	if decoded.VideoID != original.VideoID {
		t.Errorf("video_id mismatch: %d vs %d", decoded.VideoID, original.VideoID)
	}
}

// ===== Run() 初始化校验测试 =====

func TestRun_NilWorker(t *testing.T) {
	var w *LikeWorker
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("expected error for nil worker, got nil")
	}
}

func TestRun_NilRepo(t *testing.T) {
	w := &LikeWorker{rbq: &rabbitmq.RabbitMQ{}}
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("expected error for nil repos, got nil")
	}
}
