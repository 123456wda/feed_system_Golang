package account

type Account struct {
	ID            uint   `gorm:"primaryKey" json:"id"`
	Username      string `gorm:"unique" json:"username"`
	Password      string `json:"-"`
	Token         string `json:"-"`
	FollowerCount int64  `gorm:"column:follower_count;not null;default:0" json:"follower_count"` // 粉丝数，用于判断大V
}

// 捕获注册请求的参数
type CreateAccountRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

// 捕获登录请求的参数
type LoginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

// 捕获修改密码对应请求的参数
type ChangePasswordRequest struct {
	Username    string `json:"username" binding:"required"`
	OldPassword string `json:"old_password" binding:"required"`
	NewPassword string `json:"new_password" binding:"required"`
}

// 捕获根据ID查找用户的参数
type FindByIDRequest struct {
	ID uint `json:"id" binding:"required"`
}

// 捕获根据用户名查找用户的参数
type FindByUsernameRequest struct {
	Username string `json:"username" binding:"required"`
}

// 捕获重命名请求参数
type RenameRequest struct {
	NewUsername string `json:"new_username" binding:"required"`
}
