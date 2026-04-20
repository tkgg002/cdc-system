package idgen

import (
	"fmt"
	"net"
	"sync"

	"github.com/sony/sonyflake"
	"go.uber.org/zap"
)

var (
	sf   *sonyflake.Sonyflake
	once sync.Once
)

// Init initializes the Sonyflake ID generator.
// MachineID is derived from the last 16 bits of the outbound IP address,
// which ensures uniqueness across K8s pods without a central coordinator.
func Init(logger *zap.Logger) error {
	var initErr error
	once.Do(func() {
		settings := sonyflake.Settings{
			MachineID: func() (uint16, error) {
				ip, err := outboundIP()
				if err != nil {
					logger.Warn("failed to get outbound IP, using fallback machine ID 0", zap.Error(err))
					return 0, nil
				}
				machineID := uint16(ip[2])<<8 + uint16(ip[3])
				logger.Info("sonyflake initialized", zap.Uint16("machineID", machineID), zap.String("ip", ip.String()))
				return machineID, nil
			},
		}
		sf = sonyflake.NewSonyflake(settings)
		if sf == nil {
			initErr = fmt.Errorf("sonyflake: failed to create instance")
		}
	})
	return initErr
}

// NextID generates a new unique 64-bit ID.
// Safe for concurrent use from multiple goroutines.
func NextID() (uint64, error) {
	if sf == nil {
		return 0, fmt.Errorf("sonyflake: not initialized, call Init() first")
	}
	return sf.NextID()
}

// outboundIP returns the preferred outbound IP of this machine.
func outboundIP() (net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.To4(), nil
}
