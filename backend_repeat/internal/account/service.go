package account

import (
	"context"
	"fmt"
	"log"
	"time"

	"feedsystem_video_go/internal/auth"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"golang.org/x/crypto/bcrypt"
)

type AccountService struct {
	accountRepository *AccountRepository
	cache             *rediscache.Client
}

func NewAccountService(accountRepository *AccountRepository, cache *rediscache.Client) *AccountService {
	return &AccountService{accountRepository, cache}
}

func (s *AccountService) CreateAccount(ctx context.Context, account *Account) error {
	// 先对密码加密
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(account.Password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	// 密码加密
	account.Password = string(passwordHash)
	if err := s.accountRepository.CreateAccount(ctx, account); err != nil {
		return err
	}
	return nil
}

func (s *AccountService) FindByUsername(ctx context.Context, username string) (*Account, error) {
	var account *Account
	var err error
	if account, err = s.accountRepository.FindByUsername(ctx, username); err != nil {
		return nil, err
	}
	return account, nil
}

func (s *AccountService) Login(ctx context.Context, req *LoginRequest) (string, error) {
	// 查找一下对应用户
	account, err := s.FindByUsername(ctx, req.Username)
	if err != nil {
		return "", err
	}
	// 对比密码和数据库里面的对应哈希值
	if err := bcrypt.CompareHashAndPassword([]byte(account.Password), []byte(req.Password)); err != nil {
		return "", err
	}
	// 制作一个Token
	tokenstring, err := auth.GenerateToken(account.ID, account.Username)
	if err != nil {
		return "", err
	}
	// MySql存储Token
	account.Token = tokenstring
	if err := s.accountRepository.Login(ctx, account); err != nil {
		return "", err
	}
	// redis存储Token
	if s.cache != nil {
		// 缓存操作上下文
		cancelctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		// 设置redistoken缓存
		if err := s.cache.SetBytes(cancelctx, fmt.Sprintf("account:%d", account.ID),
			[]byte(tokenstring), 24*time.Hour); err != nil {
			log.Printf("failed to set cache(login token): %v", err)
		}
	}

	return tokenstring, err
}

func (s *AccountService) ChangePassword(ctx context.Context, req *ChangePasswordRequest) error {
	// 找到对应账户
	account, err := s.FindByUsername(ctx, req.Username)
	if err != nil {
		return err
	}
	// 校验密码
	if err := bcrypt.CompareHashAndPassword([]byte(account.Password), []byte(req.OldPassword)); err != nil {
		return err
	}
	// 生成新的密码哈希值
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	// 入库修改
	if err := s.accountRepository.ChangePassword(ctx, account.ID, string(passwordHash)); err != nil {
		return err
	}
	// 默认修改密码直接登出,其实就是修改Token为""
	if err := s.Logout(ctx, account.ID); err != nil {
		return err
	}

	return nil
}

func (s *AccountService) FindByID(ctx context.Context, id uint) (*Account, error) {
	if account, err := s.accountRepository.FindByID(ctx, id); err != nil {
		return nil, err
	} else {
		return account, nil
	}
}

func (s *AccountService) Logout(ctx context.Context, id uint) error {
	account, err := s.FindByID(ctx, id)
	if err != nil {
		return err
	}
	if account.Token == "" {
		return nil
	}
	// 清除缓存里面的token
	if s.cache != nil {
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		if err := s.cache.Del(cacheCtx, fmt.Sprintf("account:%d", account.ID)); err != nil {
			log.Printf("failed to del cache: %v", err)
		}
	}
	// 清除数据库里面的Token
	return s.accountRepository.Logout(ctx, account.ID)
}
