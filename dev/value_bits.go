package dev

import "github.com/atopos31/go-velisearch/dev/util"

const (
	GenderL = iota + 1
	VipL
	WeekActiveL
)

type User struct {
	Id     int
	Gender bool // true 男 false 女
	Vip    bool
	Active int    // 几天内活跃
	Bits   uint64 // 离散化属性
}

func (u *User) SetMale(gender bool) {
	u.Gender = gender
	if gender {
		u.Bits = util.SetBit1(u.Bits, GenderL)
	} else {
		u.Bits = util.ClearBit1(u.Bits, GenderL)
	}
}

func (u *User) SetVip(vip bool) {
	u.Vip = vip
	if vip {
		u.Bits = util.SetBit1(u.Bits, VipL)
	} else {
		u.Bits = util.ClearBit1(u.Bits, VipL)
	}
}

func (u *User) SetActive(day int) {
	u.Active = day
	if day < 7 {
		u.Bits = util.SetBit1(u.Bits, WeekActiveL)
	} else {
		u.Bits = util.ClearBit1(u.Bits, WeekActiveL)
	}
}

// 判断3个条件是否同时满足
func (u *User) Check(male bool, vip bool, weekActive bool) bool {
	if male && !u.Gender {
		return false
	}

	if vip && !u.Vip {
		return false
	}

	if weekActive && u.Active > 7 {
		return false
	}

	return true
}

// 判断N个条件是否同时满足
func (u *User) Filter2(on uint64) bool {
	return u.Bits&on == on
}
