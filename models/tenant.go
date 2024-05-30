package models

import "net/http"

type TenantManager interface {
	GetTenant() int64
	GetTenantFromAWSRequest(r *http.Request) int64
}
