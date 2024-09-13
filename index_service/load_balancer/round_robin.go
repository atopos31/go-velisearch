package loadbalancer

import "sync/atomic"

type RoundRobin struct {
	acc int64 // 记录累计请求次数
}

// Take 选择一个Endpoint，根据轮询算法
func (b *RoundRobin) Take(endpoints []string) string {
	if len(endpoints) == 0 {
		return ""
	}
	n := atomic.AddInt64(&b.acc, 1)
	index := int(n % int64(len(endpoints)))
	return endpoints[index]
}
