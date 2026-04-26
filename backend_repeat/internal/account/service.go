package account

import rediscache "feedsystem_video_go/internal/middleware/redis"

type AccountService struct {
	accountRepository *AccountRepository
	cache             *rediscache.Client
}

func NewAccountService(accountRepository *AccountRepository, cache *rediscache.Client) *AccountService {
	return &AccountService{accountRepository, cache}
}
