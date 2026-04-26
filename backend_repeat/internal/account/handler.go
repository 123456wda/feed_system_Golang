package account

import "github.com/gin-gonic/gin"

type AccountHandler struct {
	accountService *AccountService
}

func NewAccountHandler(accountService *AccountService) *AccountHandler {
	return &AccountHandler{accountService: accountService}
}

func (h *AccountHandler) CreateAccount(c *gin.Context) {
}

func (h *AccountHandler) Login(c *gin.Context) {
}

func (h *AccountHandler) ChangePassword(c *gin.Context) {
}

func (h *AccountHandler) FindByID(c *gin.Context) {
}

func (h *AccountHandler) FindByUsername(c *gin.Context) {
}
