package models

type TenantManager interface {
	GetTenant() int64
	GetAWSSecretKey(tenantID int64, accessKey string, region string) (string, error)
}
