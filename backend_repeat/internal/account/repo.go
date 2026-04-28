package account

import (
	"context"

	"gorm.io/gorm"
)

type AccountRepository struct {
	db *gorm.DB
}

func NewAccountRepository(db *gorm.DB) *AccountRepository {
	return &AccountRepository{db: db}
}

func (r *AccountRepository) CreateAccount(ctx context.Context, account *Account) error {
	return r.db.WithContext(ctx).Create(account).Error
}

func (r *AccountRepository) FindByUsername(ctx context.Context, Username string) (*Account, error) {
	var account Account
	err := r.db.WithContext(ctx).Where("username=?", Username).First(&account).Error
	return &account, err
}

func (r *AccountRepository) FindByID(ctx context.Context, id uint) (*Account, error) {
	var account Account
	err := r.db.WithContext(ctx).Where("id=?", id).First(&account).Error
	return &account, err
}

func (r *AccountRepository) Login(ctx context.Context, account *Account) error {
	return r.db.WithContext(ctx).Model(&Account{}).Where("id=?", account.ID).Update("token", account.Token).Error
}

func (r *AccountRepository) Logout(ctx context.Context, id uint) error {
	return r.db.Model(&Account{}).Where("id=?", id).Update("Token", "").Error
}

func (r *AccountRepository) ChangePassword(ctx context.Context, id uint, newPassword string) error {
	return r.db.WithContext(ctx).Model(&Account{}).Where("id = ?", id).Update("password", newPassword).Error
}

func (r *AccountRepository) RenameWithToken(ctx context.Context, accountID uint, newUsername string, token string) error {
	return r.db.Model(&Account{}).Where("id=?", accountID).Updates(map[string]interface{}{"username": newUsername, "token": token}).Error
}
