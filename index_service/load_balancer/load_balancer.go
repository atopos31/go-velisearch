package loadbalancer

type LoadBalancer interface {
	// Take returns a node to be used
	Take(endpoints []string) string
}
