package server

import (
	"charlie/utils"
	"time"
)

type TokenType int

var AccessTokenType TokenType = 0x1
var RefreshTokenType TokenType = 0x2

type AuthToken struct {
	AccessToken           string `json:"access_token"`
	AccessTokenExpiration int64  `json:"access_expiration"`

	RefreshToken           string `json:"refresh_token"`
	RefreshTokenExpiration int64  `json:"refresh_expiration"`
}

type Aloha struct {
	DeviceId  string     `json:"device_id"`
	AuthToken *AuthToken `json:"auth_token"`
}

func NewAuthToken() (*AuthToken, error) {
	accessToken, err := utils.RandomToken(64)
	if err != nil {
		return nil, err
	}

	refreshToken, err := utils.RandomToken(128)
	if err != nil {
		return nil, err
	}

	authToken := &AuthToken{
		AccessToken:           accessToken,
		AccessTokenExpiration: time.Now().Add(24 * 60 * time.Minute).UnixMilli(),

		RefreshToken:           refreshToken,
		RefreshTokenExpiration: time.Now().Add(7 * 24 * 60 * time.Minute).UnixMilli(),
	}

	return authToken, nil
}

func NewAloha(deviceId string) (*Aloha, error) {
	authToken, err := NewAuthToken()
	if err != nil {
		return nil, err
	}

	return &Aloha{
		DeviceId:  deviceId,
		AuthToken: authToken,
	}, nil
}
