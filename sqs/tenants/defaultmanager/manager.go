package defaultmanager

import (
	"bufio"
	"fmt"
	"os"
	"q/models"
	"strings"
)

type DefaultTenantManager struct{}

func (tm *DefaultTenantManager) GetTenant() int64 {
	return 1
}

func (tm *DefaultTenantManager) GetAWSSecretKey(accessKey string, region string) (int64, string, error) {
	envMap, err := parseEnvFile()
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

func parseEnvFile() (map[string]string, error) {
	envMap := make(map[string]string)

	file, err := os.Open("/etc/.env")
	if err != nil {
		return nil, fmt.Errorf("error opening .env file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		envMap[key] = value
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading .env file: %v", err)
	}

	return envMap, nil
}
