package redis

import (
	"time"

	"github.com/go-redis/redis"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/redis", new(REDIS))
}

// REDIS is the k6 Redis extension.
type REDIS struct{}

type ScanResult struct {
	Cursor uint64 `json:"cursor"`
	Keys   []string `json:"keys"`
}

// NewClient creates a new Redis client
func (*REDIS) NewClient(addr string, password string, bd int) *redis.Client {
	if addr == "" {
		addr = "localhost:6379"
	}
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password, // no password set
		DB:       bd,       // use default DB
	})
}

// Set adds a key/value
func (*REDIS) Set(client *redis.Client, key string, value interface{}, expiration time.Duration) {
	// TODO: Make expiration configurable. Or document somewhere the unit.
	err := client.Set(key, value, expiration*time.Second).Err()
	if err != nil {
		ReportError(err, "Failed to set the specified key/value pair")
	}
}

// Get gets a key/value
func (*REDIS) Get(client *redis.Client, key string) string {
	val, err := client.Get(key).Result()
	if err != nil {
		ReportError(err, "Failed to get the specified key")
	}
	return val
}

// Del removes a key/value
func (*REDIS) Del(client *redis.Client, key string) {
	err := client.Del(key).Err()
	if err != nil {
		ReportError(err, "Failed to remove the specified key")
	}
}

// Do runs arbitrary/custom commands
func (*REDIS) Do(client *redis.Client, cmd string, key string) string {
	val, err := client.Do(cmd, key).Result()
	if err != nil {
		if err == redis.Nil {
			ReportError(err, "Key does not exist")
		} else {
			ReportError(err, "Failed to do command")
		}
	}
	// TODO: Support more types, not only strings.
	return val.(string)
}

// Scan scan keys for match
func (*REDIS) Scan(client *redis.Client, cursor uint64, match string, count int64) *ScanResult {
	keys, cursor, err := client.Scan(cursor, match, count).Result()
	if err != nil {
		ReportError(err, "Failed to scan keys")
	}
	val := &ScanResult{
		Cursor: cursor,
		Keys:   keys,
	}
	return val
}

// Sets field in the hash stored at key to value.
func (*REDIS) HSet(client *redis.Client, key string, field string, value string) {
	err := client.HSet(key, field, value).Err()
	if err != nil {
		ReportError(err, "Failed to save field:value for specified key")
	}
}

// Returns the value associated with field in the hash stored at key.
func (*REDIS) HGet(client *redis.Client, key string, field string) string {
	val, err := client.HGet(key, field).Result()
	if err != nil {
		ReportError(err, "Failed to get field for specified key")
	}
	return val
}

// Returns all fields and values of the hash stored at key.
func (*REDIS) HGetAll(client *redis.Client, key string) map[string]string {
	val, err := client.HGetAll(key).Result()
	if err != nil {
		ReportError(err, "Failed to get fields, values for specified key")
	}
	return val
}

// Delete fields from hash stored at key.
func (*REDIS) HDel(client *redis.Client, key string, fields ...string) {
	err := client.HDel(key, fields...).Err()
	if err != nil {
		ReportError(err, "Failed to delete specified fields from key")
	}
}
