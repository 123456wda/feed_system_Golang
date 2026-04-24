package social

import "gorm.io/gorm"

// 把gorm的DB做封装 外部无法访问,对于这个结构体封装social表相关的操作方法
type SocialRepository struct {
	db *gorm.DB
}

func NewSocialRepository(db *gorm.DB) *SocialRepository {
	return &SocialRepository{db: db}
}
