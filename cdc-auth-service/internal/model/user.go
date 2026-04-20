package model

import "time"

type User struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Username  string    `gorm:"column:username;uniqueIndex;not null" json:"username"`
	Email     string    `gorm:"column:email;uniqueIndex;not null" json:"email"`
	Password  string    `gorm:"column:password;not null" json:"-"` // never expose in JSON
	FullName  string    `gorm:"column:full_name" json:"full_name"`
	Role      string    `gorm:"column:role;default:operator" json:"role"` // admin, operator
	IsActive  bool      `gorm:"column:is_active;default:true" json:"is_active"`
	CreatedAt time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (User) TableName() string { return "auth_users" }
