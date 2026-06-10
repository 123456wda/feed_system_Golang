package video

import (
	"context"
	"errors"

	"github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

type LikeRepository struct {
	db *gorm.DB
}

func NewLikeRepository(db *gorm.DB) *LikeRepository {
	return &LikeRepository{db: db}
}

// isDuplicateKey 判断是否是 MySQL 1062 唯一键冲突错误。
// 点赞的联合唯一索引 (video_id, account_id) 触发 1062 时，说明用户已点赞过。
func isDuplicateKey(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

func (r *LikeRepository) LikeIgnoreDuplicateInTx(ctx context.Context, tx *gorm.DB, like *Like) (created bool, err error) {
	if like == nil || like.VideoID == 0 || like.AccountID == 0 {
		return false, nil
	}
	err = tx.WithContext(ctx).Create(like).Error
	if err == nil {
		return true, nil
	}
	if isDuplicateKey(err) {
		// 已点赞，幂等返回 false，不抛错误
		return false, nil
	}
	return false, err
}

// DeleteByVideoAndAccount 按视频ID+用户ID 删除点赞记录。
// 返回 deleted=true 表示实际删除了记录，deleted=false 表示记录本就不存在（幂等）。
func (r *LikeRepository) DeleteByVideoAndAccount(ctx context.Context, videoID, accountID uint) (deleted bool, err error) {
	if videoID == 0 || accountID == 0 {
		return false, nil
	}
	res := r.db.WithContext(ctx).
		Where("video_id = ? AND account_id = ?", videoID, accountID).
		Delete(&Like{})
	return res.RowsAffected > 0, res.Error
}

// IsLiked 判断某用户是否已点赞某视频。
// 仅用于 /isLiked 查询接口，不在 Like/Unlike 写路径中使用（避免 TOCTOU）。
func (r *LikeRepository) IsLiked(ctx context.Context, videoID, accountID uint) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&Like{}).
		Where("video_id = ? AND account_id = ?", videoID, accountID).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// BatchGetLiked 批量查询某用户对一组视频的点赞状态。
// 返回 map[videoID]bool，用于 feed 流渲染时判断当前用户是否点赞过列表中的视频。
func (r *LikeRepository) BatchGetLiked(ctx context.Context, videoIDs []uint, accountID uint) (map[uint]bool, error) {
	likeMap := make(map[uint]bool)
	if len(videoIDs) == 0 || accountID == 0 {
		return likeMap, nil
	}
	var likes []Like
	err := r.db.WithContext(ctx).Model(&Like{}).
		Where("video_id IN ? AND account_id = ?", videoIDs, accountID).
		Find(&likes).Error
	if err != nil {
		return nil, err
	}
	for _, l := range likes {
		likeMap[l.VideoID] = true
	}
	return likeMap, nil
}

// ListLikedVideos 查询某用户点赞过的所有视频，按点赞时间倒序排列。
// 通过 JOIN likes 表获取完整视频信息。
// 注意：当前未做分页，后续若用户点赞量级增大需要增加 LIMIT/OFFSET。
func (r *LikeRepository) ListLikedVideos(ctx context.Context, accountID uint) ([]Video, error) {
	var videos []Video
	if accountID == 0 {
		return videos, nil
	}
	err := r.db.WithContext(ctx).
		Model(&Video{}).
		Joins("JOIN likes ON likes.video_id = videos.id").
		Where("likes.account_id = ?", accountID).
		Order("likes.created_at desc").
		Find(&videos).Error
	if err != nil {
		return nil, err
	}
	return videos, nil
}

// Transaction 开启一个数据库事务，将 *gorm.DB 注入回调函数 fn。
// service 层通过此方法获取事务对象，再调用各 repo 的事务版本方法，
// 避免 service 直接访问 repo.db（修复分层破坏问题）。
func (r *LikeRepository) Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error {
	return r.db.WithContext(ctx).Transaction(fn)
}

// LikeInTx 在已有事务中插入点赞记录，遇 1062 重复则返回"用户已点赞"错误。
// 用于 service fallback 路径的事务内操作。
func (r *LikeRepository) LikeInTx(ctx context.Context, tx *gorm.DB, like *Like) error {
	if err := tx.WithContext(ctx).Create(like).Error; err != nil {
		if isDuplicateKey(err) {
			return errors.New("user has liked this video")
		}
		return err
	}
	return nil
}

// DeleteByVideoAndAccountInTx 在已有事务中删除点赞记录，
// 若没有删到任何行则返回"用户未点赞"错误。
// 用于 service fallback 路径的事务内操作。
func (r *LikeRepository) DeleteByVideoAndAccountInTx(ctx context.Context, tx *gorm.DB, videoID, accountID uint) error {
	res := tx.WithContext(ctx).
		Where("video_id = ? AND account_id = ?", videoID, accountID).
		Delete(&Like{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("user has not liked this video")
	}
	return nil
}

// DeleteByVideoAndAccountTx 在已有事务中删除点赞记录，
// 返回 deleted=true 表示实际删除了记录，deleted=false 表示记录本就不存在（幂等）。
// 供 Worker 消费者在事务内使用，配合 created 判断是否需要更新计数。
func (r *LikeRepository) DeleteByVideoAndAccountTx(ctx context.Context, tx *gorm.DB, videoID, accountID uint) (deleted bool, err error) {
	if videoID == 0 || accountID == 0 {
		return false, nil
	}
	res := tx.WithContext(ctx).
		Where("video_id = ? AND account_id = ?", videoID, accountID).
		Delete(&Like{})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}
