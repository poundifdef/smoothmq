package defaultmanager

import (
	"fmt"
	"io/ioutil"
	"q/models"
)

type DefaultTenantManager struct{}

func (tm *DefaultTenantManager) GetTenant() int64 {
	return 1
}

func (tm *DefaultTenantManager) GetAWSSecretKey(accessKey string, region string) (int64, string, error) {
	// Read and print the contents of /etc/.env
	content, err := ioutil.ReadFile("/etc/.env")
	if err != nil {
		fmt.Printf("Error reading /etc/.env: %v\n", err)
	} else {
		fmt.Printf("Contents of /etc/.env:\n%s\n", string(content))
	}

	// Return the hardcoded value as before
	return int64(1), "YOUR_SECRET_ACCESS_KEY2", nil
}

func NewDefaultTenantManager() models.TenantManager {
	return &DefaultTenantManager{}
}
