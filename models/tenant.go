package models

type TenantManager interface {
	GetTenant() int64
	GetAWSSecretKey(accessKey string, region string) (tenantId int64, secretKey string, err error)
}
