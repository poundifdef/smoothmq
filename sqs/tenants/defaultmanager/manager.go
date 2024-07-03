package defaultmanager

import (
	"q/models"
)

type DefaultTenantManager struct{}

func (tm *DefaultTenantManager) GetTenant() int64 {
	return 1
}

func (tm *DefaultTenantManager) GetAWSSecretKey(accessKey string, region string) (int64, string, error) {
	return int64(1), "YOUR_SECRET_ACCESS_KEY2", nil
	/*
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if secretKey == "" {
			secretKey = "AWS_SECRET_ACCESS_KEY2"
			// print warning
			fmt.Println("AWS_SECRET_ACCESS_KEY WAS NOT FOUND")
			// exit 1
			os.Exit(1)
		}
		return int64(1), secretKey, nil
	*/
}

func NewDefaultTenantManager() models.TenantManager {
	return &DefaultTenantManager{}
}
