package video

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

type CommentRepository struct {
	db *gorm.DB
}

func NewCommentRepository(db *gorm.DB) *CommentRepository {
	return &CommentRepository{db: db}
}

// CreateComment 在非事务场景下插入评论（供消费者使用，写评论后由 worker 单独调 ChangePopularity）。
func (r *CommentRepository) CreateComment(ctx context.Context, comment *Comment) error {
	return r.db.WithContext(ctx).Create(comment).Error
}

// DeleteComment 根据主键删除评论。
func (r *CommentRepository) DeleteComment(ctx context.Context, comment *Comment) error {
	return r.db.WithContext(ctx).Delete(comment).Error
}

// GetAllComments 查询某视频下的所有评论，按创建时间正序排列。
func (r *CommentRepository) GetAllComments(ctx context.Context, videoID uint) ([]Comment, error) {
	var comments []Comment
	err := r.db.WithContext(ctx).Where("video_id = ?", videoID).Order("created_at asc").Find(&comments).Error
	return comments, err
}

// GetCommentsByPage 分页查询某视频下的评论，按创建时间倒序（最新的在前）。
func (r *CommentRepository) GetCommentsByPage(ctx context.Context, videoID uint, offset, limit int) ([]Comment, error) {
	var comments []Comment
	err := r.db.WithContext(ctx).
		Where("video_id = ?", videoID).
		Order("created_at desc").
		Offset(offset).Limit(limit).
		Find(&comments).Error
	return comments, err
}

// GetByID 根据主键查询单条评论，不存在返回 (nil, nil)。
func (r *CommentRepository) GetByID(ctx context.Context, id uint) (*Comment, error) {
	var comment Comment
	if err := r.db.WithContext(ctx).First(&comment, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &comment, nil
}

// Transaction 开启数据库事务，将 *gorm.DB 注入回调函数。
// Service 层通过此方法获取事务对象，再调用各 repo 的 InTx 版本方法，
// 避免 service 直接访问 repo.db（保持分层清晰）。
func (r *CommentRepository) Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error {
	return r.db.WithContext(ctx).Transaction(fn)
}

// CreateCommentInTx 在已有事务中插入评论，供 service fallback 路径使用。
func (r *CommentRepository) CreateCommentInTx(ctx context.Context, tx *gorm.DB, comment *Comment) error {
	return tx.WithContext(ctx).Create(comment).Error
}

// DeleteCommentInTx 在已有事务中删除评论，不存在则返回错误。
func (r *CommentRepository) DeleteCommentInTx(ctx context.Context, tx *gorm.DB, comment *Comment) error {
	res := tx.WithContext(ctx).Delete(comment)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("comment not found")
	}
	return nil
}
