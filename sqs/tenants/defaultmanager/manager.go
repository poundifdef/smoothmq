package defaultmanager

import "q/models"

type DefaultTenantManager struct{}

func (tm *DefaultTenantManager) GetTenant() int64 {
	return 1
}

func (tm *DefaultTenantManager) GetAWSSecretKey(accessKey string, region string) (int64, string, error) {
	return int64(1), "YOUR_SECRET_ACCESS_KEY", nil
}

func NewDefaultTenantManager() models.TenantManager {
	return &DefaultTenantManager{}
}
