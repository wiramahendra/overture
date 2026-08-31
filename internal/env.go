package internal

import "os"

// GetEnv returns the value of the OVERTURE env var named key, falling back to
// the legacy IGRIS_ counterpart for backward compatibility.
// Keys should be passed as the canonical OVERTURE_ name (e.g. OVERTURE_RUNTIME_TIMEOUT).
// If neither is set, defaultVal is returned.
func GetEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	// Fallback to legacy IGRIS_ prefix
	if len(key) > 9 && key[:9] == "OVERTURE_" {
		legacy := "IGRIS_" + key[9:]
		if v := os.Getenv(legacy); v != "" {
			return v
		}
	}
	return defaultVal
}

// GetEnvRaw checks OVERTURE_ key then legacy IGRIS_ key, returns value and whether set.
func GetEnvRaw(overtureKey string) (string, bool) {
	if v := os.Getenv(overtureKey); v != "" {
		return v, true
	}
	if len(overtureKey) > 9 && overtureKey[:9] == "OVERTURE_" {
		legacy := "IGRIS_" + overtureKey[9:]
		if v := os.Getenv(legacy); v != "" {
			return v, true
		}
	}
	return "", false
}

// EnvOrLegacy returns OVERTURE_ if set, otherwise legacy IGRIS_ value.
func EnvOrLegacy(overtureKey, legacyKey string) string {
	if v := os.Getenv(overtureKey); v != "" {
		return v
	}
	if v := os.Getenv(legacyKey); v != "" {
		return v
	}
	return ""
}
