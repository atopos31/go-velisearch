package util

// 判断第pos位是否为1
func IsBit1(n uint64, pos int) bool {
	if pos < 1 || pos > 64 {
		panic(pos)
	}
	return (n & (1<<pos - 1)) != 0
}

// 设置第pos位为1
func SetBit1(n uint64, pos int) uint64 {
	if pos < 1 || pos > 64 {
		panic(pos)
	}
	return n | (1<<pos - 1)
}

// 设置第pos位为0
func ClearBit1(n uint64, pos int) uint64 {
	if pos < 1 || pos > 64 {
		panic(pos)
	}
	return n &^ (1<<pos - 1)
}

// 一个整数的二进制表示中，1的个数
func CountBit1(n uint64) int {
	count := 0
	for n != 0 {
		count++
		n = n & (n - 1) // 清除最低位的1
	}
	return count
}
