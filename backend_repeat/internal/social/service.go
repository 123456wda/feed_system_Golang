package social

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/middleware/rabbitmq"
)

// SocialService 处理关注/取关相关业务逻辑。
// 设计要点：
//   - 关注关系由 API 同步写入 MySQL（保证强一致性）
//   - MQ 只负责通知下游（如 timeline fanout），属于"尽力而为"的异步通知
//   - MQ 投递失败不影响核心业务，下游可通过其他方式补偿
type SocialService struct {
	repo        *SocialRepository
	accountRepo *account.AccountRepository
	socialMQ    *rabbitmq.SocialMQ
}

func NewSocialService(repo *SocialRepository, accountRepo *account.AccountRepository, socialMQ *rabbitmq.SocialMQ) *SocialService {
	return &SocialService{repo: repo, accountRepo: accountRepo, socialMQ: socialMQ}
}

// Follow 关注操作。
// 流程：校验双方账号存在 → 防止自关注 → 检查是否已关注 → 投 MQ 通知下游 → 同步写库。
//
// 注意：IsFollowed 预检查存在 TOCTOU 竞态窗口（两个并发请求可能同时通过检查），
// 但 uniqueIndex 会兜底防止重复插入。这里保留预检查是为了给用户明确的错误提示
// （"已经关注了" vs 数据库 1062 错误），属于用户体验优先的设计取舍。
func (s *SocialService) Follow(ctx context.Context, social *Social) error {
	// 校验关注者账号存在（防止关注不存在的用户）
	if _, err := s.accountRepo.FindByID(ctx, social.FollowerID); err != nil {
		return err
	}
	// 校验被关注者账号存在
	if _, err := s.accountRepo.FindByID(ctx, social.VloggerID); err != nil {
		return err
	}
	// 防止自己关注自己（业务规则）
	if social.FollowerID == social.VloggerID {
		return errors.New("can not follow self")
	}
	// 检查是否已关注（预检查，给用户友好提示）
	// 注意：此处存在 TOCTOU 窗口，但 uniqueIndex 兜底
	isFollowed, err := s.repo.IsFollowed(ctx, social)
	if err != nil {
		return err
	}
	if isFollowed {
		return errors.New("already followed")
	}
	// MQ 通知下游（如 timeline fanout），错误静默忽略。
	// 原因：关注关系已由下面的 repo.Follow 同步保证写入，
	// MQ 只是让下游（信息流）尽快感知到变化，失败了也不影响核心业务。
	if s.socialMQ != nil {
		_ = s.socialMQ.Follow(ctx, social.FollowerID, social.VloggerID)
	}
	// 同步写库：在 social 表插入关注记录
	return s.repo.Follow(ctx, social)
}

// Unfollow 取关操作。
// 流程：校验双方账号存在 → 检查是否已关注 → 投 MQ 通知下游 → 同步删库。
//
// 同样存在 TOCTOU 窗口，但取关操作本身是幂等的（删 0 行也不影响），
// 这里保留预检查是为了返回"还没关注"的友好提示。
func (s *SocialService) Unfollow(ctx context.Context, social *Social) error {
	// 校验双方账号存在
	if _, err := s.accountRepo.FindByID(ctx, social.FollowerID); err != nil {
		return err
	}
	if _, err := s.accountRepo.FindByID(ctx, social.VloggerID); err != nil {
		return err
	}
	// 检查是否确实关注过（预检查，给用户友好提示）
	isFollowed, err := s.repo.IsFollowed(ctx, social)
	if err != nil {
		return err
	}
	if !isFollowed {
		return errors.New("not followed")
	}
	// MQ 通知下游清理 timeline 等数据
	if s.socialMQ != nil {
		_ = s.socialMQ.UnFollow(ctx, social.FollowerID, social.VloggerID)
	}
	// 同步删库：删除 social 表中的关注记录
	return s.repo.Unfollow(ctx, social)
}

// GetAllFollowers 查询某博主的所有粉丝账号列表。
func (s *SocialService) GetAllFollowers(ctx context.Context, vloggerID uint) ([]*account.Account, error) {
	// 校验博主账号存在（防止查不存在的用户的粉丝）
	if _, err := s.accountRepo.FindByID(ctx, vloggerID); err != nil {
		return nil, err
	}
	return s.repo.GetAllFollowers(ctx, vloggerID)
}

// GetAllVloggers 查询某用户关注的所有博主账号列表。
func (s *SocialService) GetAllVloggers(ctx context.Context, followerID uint) ([]*account.Account, error) {
	// 校验用户账号存在
	if _, err := s.accountRepo.FindByID(ctx, followerID); err != nil {
		return nil, err
	}
	return s.repo.GetAllVloggers(ctx, followerID)
}

// IsFollowed 判断是否已关注。
func (s *SocialService) IsFollowed(ctx context.Context, social *Social) (bool, error) {
	// 校验双方账号存在
	if _, err := s.accountRepo.FindByID(ctx, social.FollowerID); err != nil {
		return false, err
	}
	if _, err := s.accountRepo.FindByID(ctx, social.VloggerID); err != nil {
		return false, err
	}
	return s.repo.IsFollowed(ctx, social)
}
