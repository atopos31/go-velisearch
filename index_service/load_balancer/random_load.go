package loadbalancer

import "math/rand"

type RandomSelect struct{}

// Take 选择一个Endpoint，根据随机选择算法
func (b *RandomSelect) Take(endpoints []string) string {
	if len(endpoints) == 0 {
		return ""
	}
	index := rand.Intn(len(endpoints))
	return endpoints[index]
}
