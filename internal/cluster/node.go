package cluster

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseOrdinal extracts the StatefulSet ordinal from a pod name like "tinycache-2".
func ParseOrdinal(podName string) (int, error) {
	idx := strings.LastIndex(podName, "-")
	if idx < 0 || idx == len(podName)-1 {
		return 0, fmt.Errorf("invalid pod name: %q", podName)
	}
	ordinal, err := strconv.Atoi(podName[idx+1:])
	if err != nil {
		return 0, fmt.Errorf("invalid ordinal in pod name %q: %w", podName, err)
	}
	return ordinal, nil
}

// BuildPeerList builds the full list of peer DNS names for a StatefulSet.
// Each peer has FQDN: {serviceName}-{ordinal}.{serviceName}.{namespace}.svc.cluster.local
func BuildPeerList(replicas int, serviceName, namespace string) []NodeInfo {
	peers := make([]NodeInfo, replicas)
	for i := range replicas {
		name := fmt.Sprintf("%s-%d", serviceName, i)
		fqdn := fmt.Sprintf(
			"%s-%d.%s.%s.svc.cluster.local",
			serviceName, i, serviceName, namespace,
		)
		peers[i] = NodeInfo{
			Name: name,
			Addr: fmt.Sprintf("%s:%d", fqdn, 11311),
		}
	}
	return peers
}
