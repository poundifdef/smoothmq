package defaultmanager

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
)

type DefaultTenantManager struct {
	keys map[string]string
}

func (tm *DefaultTenantManager) GetTenant(r *http.Request) (int64, error) {
	return 1, nil
}

func (tm *DefaultTenantManager) GetAWSSecretKey(accessKey string, region string) (int64, string, error) {
	secretKey, ok := tm.keys[accessKey]
	if !ok {
		return 0, "", errors.New("invalid key")
	}

	return int64(1), secretKey, nil
}

func NewDefaultTenantManager(cfg []config.AWSKey) models.TenantManager {
	keys := make(map[string]string)
	for _, key := range cfg {
		keys[key.AccessKey] = key.SecretKey

		if key.AccessKey == "DEV_ACCESS_KEY_ID" {
			fmt.Println()
			fmt.Println("Development SQS credentials:")
			fmt.Println("    Access Key: " + key.AccessKey)
			fmt.Println("    Secret Key: " + key.SecretKey)
			fmt.Println()
		}
	}

	rc := &DefaultTenantManager{
		keys: keys,
	}
	return rc
}
