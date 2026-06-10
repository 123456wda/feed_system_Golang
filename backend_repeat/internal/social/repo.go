package social

import (
	"context"
	"feedsystem_video_go/internal/account"

	"gorm.io/gorm"
)

// SocialRepository 封装 social 表的数据库操作。
// 外部无法直接访问 db，所有操作通过公开方法完成，保持分层清晰。
type SocialRepository struct {
	db *gorm.DB
}

func NewSocialRepository(db *gorm.DB) *SocialRepository {
	return &SocialRepository{db: db}
}

// Follow 插入一条关注记录。
// 如果记录已存在（唯一索引冲突），MySQL 会返回 1062 错误，
// 由上层（service 或 worker）决定如何处理。
func (r *SocialRepository) Follow(ctx context.Context, social *Social) error {
	return r.db.WithContext(ctx).Create(social).Error
}

// Unfollow 根据 follower_id + vlogger_id 删除关注记录。
// 如果记录不存在，RowsAffected 为 0，但不会报错（GORM 的 Delete 行为）。
func (r *SocialRepository) Unfollow(ctx context.Context, social *Social) error {
	return r.db.WithContext(ctx).
		Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.VloggerID).
		Delete(&Social{}).Error
}

// GetAllFollowers 查询某博主的所有粉丝账号。
// 采用两步查询策略：
//   1. 从 social 表查出所有 follower_id（关注了该博主的用户 ID 列表）
//   2. 用 IN 批量查询 account 表，获取完整的账号信息
//
// 为什么不直接 JOIN：social 和 account 是不同业务域，直接 JOIN 会耦合表结构，
// 且 account 表可能有缓存层，分开查询更灵活。
func (r *SocialRepository) GetAllFollowers(ctx context.Context, vloggerID uint) ([]*account.Account, error) {
	// 第一步：查出所有关注了该博主的关系记录
	var relations []Social
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("vlogger_id = ?", vloggerID).
		Find(&relations).Error; err != nil {
		return nil, err
	}

	// 提取所有粉丝 ID
	followerIDs := make([]uint, 0, len(relations))
	for _, rel := range relations {
		followerIDs = append(followerIDs, rel.FollowerID)
	}
	// 没有粉丝，直接返回空列表（避免下面 IN 空数组的无效查询）
	if len(followerIDs) == 0 {
		return []*account.Account{}, nil
	}

	// 第二步：批量查询粉丝的账号信息
	var followers []*account.Account
	if err := r.db.WithContext(ctx).
		Model(&account.Account{}).
		Where("id IN ?", followerIDs).
		Find(&followers).Error; err != nil {
		return nil, err
	}
	return followers, nil
}

// GetAllVloggers 查询某用户关注的所有博主账号。
// 同样采用两步查询：先查关系表获取 vlogger_id 列表，再批量查 account 表。
func (r *SocialRepository) GetAllVloggers(ctx context.Context, followerID uint) ([]*account.Account, error) {
	// 第一步：查出该用户关注的所有关系记录
	var relations []Social
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("follower_id = ?", followerID).
		Find(&relations).Error; err != nil {
		return nil, err
	}

	// 提取所有博主 ID
	vloggerIDs := make([]uint, 0, len(relations))
	for _, rel := range relations {
		vloggerIDs = append(vloggerIDs, rel.VloggerID)
	}
	if len(vloggerIDs) == 0 {
		return []*account.Account{}, nil
	}

	// 第二步：批量查询博主的账号信息
	var vloggers []*account.Account
	if err := r.db.WithContext(ctx).
		Model(&account.Account{}).
		Where("id IN ?", vloggerIDs).
		Find(&vloggers).Error; err != nil {
		return nil, err
	}
	return vloggers, nil
}

// IsFollowed 判断是否已关注。
// 使用 COUNT 而非 First：COUNT 不需要反序列化整行，性能更好。
func (r *SocialRepository) IsFollowed(ctx context.Context, social *Social) (bool, error) {
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.VloggerID).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// GetAllFollowerIDs 查询某博主的所有粉丝 ID 列表（不查 account 表）。
// 用于 fanout worker 批量推送收件箱，只需 ID 不需要完整账号信息，性能更好。
func (r *SocialRepository) GetAllFollowerIDs(ctx context.Context, vloggerID uint) ([]uint, error) {
	var ids []uint
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("vlogger_id = ?", vloggerID).
		Pluck("follower_id", &ids).Error; err != nil {
		return nil, err
	}
	return ids, nil
}

// CountFollowers 查询某博主的粉丝数量。
// 用于 fanout worker 判断是否大V（粉丝数 >= 阈值）。
func (r *SocialRepository) CountFollowers(ctx context.Context, vloggerID uint) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("vlogger_id = ?", vloggerID).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}
