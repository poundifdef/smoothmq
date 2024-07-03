package defaultmanager

import (
	"fmt"
	"q/models"
	"q/utils"
)

type DefaultTenantManager struct{}

func (tm *DefaultTenantManager) GetTenant() int64 {
	return 1
}

func (tm *DefaultTenantManager) GetAWSSecretKey(accessKey string, region string) (int64, string, error) {
	envMap, err := utils.ParseEnvFile()
	if err != nil {
		return 0, "", fmt.Errorf("error parsing .env file: %v", err)
	}

	fmt.Println("Environment variables:")
	for key, value := range envMap {
		fmt.Printf("%s: %s\n", key, value)
	}

	secretKey, ok := envMap["AWS_SECRET_ACCESS_KEY"]
	if !ok {
		return 0, "", fmt.Errorf("AWS_SECRET_ACCESS_KEY not found in .env file")
	}

	return int64(1), secretKey, nil
}

func NewDefaultTenantManager() models.TenantManager {
	return &DefaultTenantManager{}
}
