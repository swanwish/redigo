package redis

import (
	"errors"
	"time"
)

type RedisClient struct {
	pool *Pool
}

func GetRedisClient(addr, pass string) *RedisClient {
	//logs.Debugf("The addr is %s", addr)
	if addr != "" {
		//logs.Debugf("The addr is %s, password is [%s]", addr, strings.Repeat("*", len(pass)))
		var options []DialOption
		if pass != "" {
			options = append(options, DialPassword(pass))
		}
		pool := &Pool{
			MaxIdle:   80,
			MaxActive: 12000, // max number of connections
			Dial: func() (Conn, error) {
				//c, err := Dial("tcp", "redis-10616.c15.us-east-1-4.ec2.cloud.redislabs.com:10616", DialPassword("NaQlEWBSz6ZQ8lXPJ329EUjZK12NvzaG"))
				c, err := Dial("tcp", addr, options...)
				if err != nil {
					panic(err.Error())
				}
				return c, err
			},
		}
		return &RedisClient{pool: pool}
	}
	//logs.Errorf("The addr is empty")
	return nil
}

var ErrInternalError = errors.New("Internal error")

const (
	RedisStatusOK = "OK"
)

// Hash
const (
	HDel         = "HDEL"
	HExists      = "HEXISTS"
	HGet         = "HGET"
	HGetAll      = "HGETALL"
	HIncrBy      = "HINCRBY"
	HIncrByFloat = "HINCRBYFLOAT"
	HKeys        = "HKEYS"
	HLen         = "HLEN"
	HMGet        = "HMGET"
	HMSet        = "HMSET"
	HSet         = "HSET"
	HSetNX       = "HSETNX"
	HStrLen      = "HSTRLEN"
	HVals        = "HVALS"
)

// Sorted CmdSet
const (
	ZAdd             = "ZADD"
	ZCard            = "ZCARD"
	ZCount           = "ZCOUNT"
	ZLexCount        = "ZLEXCOUNT"
	ZIncBy           = "ZINCRBY"
	ZInterStore      = "ZINTERSTORE"
	ZPopMax          = "ZPOPMAX"
	ZPopMin          = "ZPOPMIN"
	ZRange           = "ZRANGE"
	ZRangeByLex      = "ZRANGEBYLEX"
	ZRangeByScore    = "ZRANGEBYSCORE"
	ZRank            = "ZRANK"
	ZRem             = "ZREM"
	ZRemRangeByLex   = "ZREMRANGEBYLEX"
	ZRemRangeByRank  = "ZREMRANGEBYRANK"
	ZRemRangeByScore = "ZREMRANGEBYSCORE"
	ZRevRange        = "ZREVRANGE"
	ZRevRangeByLex   = "ZREVRANGEBYLEX"
	ZRevRangeByScore = "ZREVRANGEBYSCORE"
	ZRevRank         = "ZREVRANK"
	ZScore           = "ZSCORE"
	ZUnionStore      = "ZUNIONSTORE"
)

// String
const (
	CmdAppend      = "APPEND"
	CmdBitCount    = "BITCOUNT"
	CmdBitOp       = "BITOP"
	CmdBitPos      = "BITPOS"
	CmdDecr        = "DECR"
	CmdDecrBy      = "DECRBY"
	CmdGet         = "GET"
	CmdGetBit      = "GETBIT"
	CmdGetRange    = "GETRANGE"
	CmdGetSet      = "GETSET"
	CmdIncr        = "INCR"
	CmdIncrBy      = "INCRBY"
	CmdIncrByFloat = "INCRBYFLOAT"
	CmdMGet        = "MGET"
	CmdMSet        = "MSET"
	CmdMSetNX      = "MSETNX"
	CmdSet         = "SET"
	CmdSetBit      = "SETBIT"
	CmdSetNX       = "SETNX"
	CmdSetRange    = "SETRANGE"
	CmdStrLen      = "STRLEN"
)

// Set
const (
	CmdSAdd        = "SADD"
	CmdSCard       = "SCARD"
	CmdSDiff       = "SDIFF"
	CmdSDiffStore  = "SDIFFSTORE"
	CmdSInter      = "SINTER"
	CmdSInterStore = "SINTERSTORE"
	CmdSIsMember   = "SISMEMBER"
	CmdSMembers    = "SMEMBERS"
	CmdSMove       = "SMOVE"
	CmdSPop        = "SPOP"
	CmdSRandMember = "SRANDMEMBER"
	CmdSRem        = "SREM"
	CmdSUnion      = "SUNION"
	CmdSUnionStore = "SUNIONSTORE"
)

// List
const (
	CmdBLPop      = "BLPOP"
	CmdBRPop      = "BRPOP"
	CmdBRPopLPush = "BRPOPLPUSH"
	CmdLIndex     = "LINDEX"
	CmdLInsert    = "LINSERT"
	CmdLLen       = "LLEN"
	CmdLPop       = "LPOP"
	CmdLPush      = "LPUSH"
	CmdLPushX     = "LPUSHX"
	CmdLRange     = "LRANGE"
	CmdLRem       = "LREM"
	CmdLSet       = "LSET"
	CmdLTrim      = "LTRIM"
	CmdRPop       = "RPOP"
	CmdRPopLPush  = "RPOPLPUSH"
	CmdRPush      = "RPUSH"
	CmdRPushX     = "RPUSHX"
)

// Database
const (
	CmdDel       = "DEL"
	CmdExists    = "EXISTS"
	CmdRandomKey = "RANDOMKEY"
	CmdRename    = "RENAME"
	CmdRenameNX  = "RENAMENX"
	CmdKeys      = "KEYS"
)

const (
	ParamXX         = "XX"
	ParamNX         = "NX"
	ParamCH         = "CH"
	ParamINCR       = "INCR"
	ParamWeights    = "WEIGHTS"
	ParamAggregate  = "AGGREGATE"
	ParamWithScores = "WITHSCORES"
	ParamLimit      = "LIMIT"
	ParamAnd        = "AND"
	ParamOr         = "OR"
	ParamXOR        = "XOR"
	ParamNot        = "NOT"
	ParamPX         = "PX"
	ParamEX         = "EX"
	ParamBefore     = "BEFORE"
	ParamAfter      = "AFTER"
	ParamMinimum    = "-inf"
	ParamMaximum    = "+inf"
)

func (client *RedisClient) getConn() (Conn, error) {
	if client == nil {
		//logs.Errorf("The client is nil")
		return nil, ErrInternalError
	}
	if client.pool == nil {
		//logs.Errorf("The connection pool does not exists")
		return nil, ErrInternalError
	}
	return client.pool.Get(), nil
}

func (client *RedisClient) Int64(commandName string, args ...interface{}) (int64, error) {
	conn, err := client.getConn()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return Int64(conn.Do(commandName, args...))
}

func (client *RedisClient) Float64(commandName string, args ...interface{}) (float64, error) {
	conn, err := client.getConn()
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	return Float64(conn.Do(commandName, args...))
}

func (client *RedisClient) StringSlice(commandName string, args ...interface{}) ([]string, error) {
	conn, err := client.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return Strings(conn.Do(commandName, args...))
}

func (client *RedisClient) StringSliceWithTimeout(timeout time.Duration, commandName string, args ...interface{}) ([]string, error) {
	conn, err := client.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return Strings(DoWithTimeout(conn, timeout, commandName, args...))
}

func (client *RedisClient) StringMap(commandName string, args ...interface{}) (map[string]string, error) {
	conn, err := client.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return StringMap(conn.Do(commandName, args...))
}

func (client *RedisClient) Bool(commandName string, args ...interface{}) (bool, error) {
	conn, err := client.getConn()
	if err != nil {
		return false, err
	}
	defer conn.Close()

	return Bool(conn.Do(commandName, args...))
}

func (client *RedisClient) String(commandName string, args ...interface{}) (string, error) {
	conn, err := client.getConn()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	return String(conn.Do(commandName, args...))
}

func (client *RedisClient) StringWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (string, error) {
	conn, err := client.getConn()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	return String(DoWithTimeout(conn, timeout, commandName, args...))
}

func (client *RedisClient) Values(commandName string, args ...interface{}) ([]interface{}, error) {
	conn, err := client.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return Values(conn.Do(commandName, args...))
}

func usePrecise(dur time.Duration) bool {
	return dur < time.Second || dur%time.Second != 0
}

func formatMs(dur time.Duration) int64 {
	//if dur > 0 && dur < time.Millisecond {
	//	logs.Debugf(
	//		"specified duration is %s, but minimal supported value is %s",
	//		dur, time.Millisecond,
	//	)
	//}
	return int64(dur / time.Millisecond)
}

func formatSec(dur time.Duration) int64 {
	//if dur > 0 && dur < time.Second {
	//	logs.Debugf(
	//		"specified duration is %s, but minimal supported value is %s",
	//		dur, time.Second,
	//	)
	//}
	return int64(dur / time.Second)
}

// ---------------------------Database---------------------------

func (client *RedisClient) Del(keys ...string) (int64, error) {
	var args []interface{}
	for _, key := range keys {
		args = append(args, key)
	}
	return client.Int64(CmdDel, args...)
}

//func (client *RedisClient) Unlink(keys ...string) (int64, error) {
//	args := make([]interface{}, 1+len(keys))
//	args[0] = "unlink"
//	for i, key := range keys {
//		args[1+i] = key
//	}
//	cmd := NewIntCmd(args...)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) Dump(key string) (string, error) {
//	cmd := NewStringCmd("dump", key)
//	c.process(cmd)
//	return cmd
//}

func (client *RedisClient) Exists(keys ...string) (int64, error) {
	var args []interface{}
	for _, key := range keys {
		args = append(args, key)
	}
	return client.Int64(CmdExists, args...)
}

//func (client *RedisClient) Expire(key string, expiration time.Duration) (bool, error) {
//	cmd := NewBoolCmd("expire", key, formatSec(expiration))
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) ExpireAt(key string, tm time.Time) (bool, error) {
//	cmd := NewBoolCmd("expireat", key, tm.Unix())
//	c.process(cmd)
//	return cmd
//}

func (client *RedisClient) Keys(pattern string) ([]string, error) {
	return client.StringSlice(CmdKeys, pattern)
}

//
//func (client *RedisClient) Migrate(host, port, key string, db int64, timeout time.Duration) *StatusCmd {
//	cmd := NewStatusCmd(
//		"migrate",
//		host,
//		port,
//		key,
//		db,
//		formatMs(timeout),
//	)
//	cmd.setReadTimeout(readTimeout(timeout))
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) Move(key string, db int64) (bool, error) {
//	cmd := NewBoolCmd("move", key, db)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) ObjectRefCount(key string) (int64, error) {
//	cmd := NewIntCmd("object", "refcount", key)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) ObjectEncoding(key string) (string, error) {
//	cmd := NewStringCmd("object", "encoding", key)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) ObjectIdleTime(key string) *DurationCmd {
//	cmd := NewDurationCmd(time.Second, "object", "idletime", key)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) Persist(key string) (bool, error) {
//	cmd := NewBoolCmd("persist", key)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) PExpire(key string, expiration time.Duration) (bool, error) {
//	cmd := NewBoolCmd("pexpire", key, formatMs(expiration))
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) PExpireAt(key string, tm time.Time) (bool, error) {
//	cmd := NewBoolCmd(
//		"pexpireat",
//		key,
//		tm.UnixNano()/int64(time.Millisecond),
//	)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) PTTL(key string) *DurationCmd {
//	cmd := NewDurationCmd(time.Millisecond, "pttl", key)
//	c.process(cmd)
//	return cmd
//}

func (client *RedisClient) RandomKey() (string, error) {
	return client.String(CmdRandomKey)
}

func (client *RedisClient) Rename(key, newkey string) (string, error) {
	return client.String(CmdRename, key, newkey)
}

func (client *RedisClient) RenameNX(key, newkey string) (bool, error) {
	return client.Bool(CmdRenameNX, key, newkey)
}

//func (client *RedisClient) Restore(key string, ttl time.Duration, value string) *StatusCmd {
//	cmd := NewStatusCmd(
//		"restore",
//		key,
//		formatMs(ttl),
//		value,
//	)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) RestoreReplace(key string, ttl time.Duration, value string) *StatusCmd {
//	cmd := NewStatusCmd(
//		"restore",
//		key,
//		formatMs(ttl),
//		value,
//		"replace",
//	)
//	c.process(cmd)
//	return cmd
//}
//
//type Sort struct {
//	By            string
//	Offset, Count int64
//	Get           []string
//	Order         string
//	Alpha         bool
//}
//
//func (sort *Sort) args(key string) []interface{} {
//	args := []interface{}{"sort", key}
//	if sort.By != "" {
//		args = append(args, "by", sort.By)
//	}
//	if sort.Offset != 0 || sort.Count != 0 {
//		args = append(args, "limit", sort.Offset, sort.Count)
//	}
//	for _, get := range sort.Get {
//		args = append(args, "get", get)
//	}
//	if sort.Order != "" {
//		args = append(args, sort.Order)
//	}
//	if sort.Alpha {
//		args = append(args, "alpha")
//	}
//	return args
//}
//
//func (client *RedisClient) Sort(key string, sort *Sort) ([]string, error) {
//	cmd := NewStringSliceCmd(sort.args(key)...)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) SortStore(key, store string, sort *Sort) (int64, error) {
//	args := sort.args(key)
//	if store != "" {
//		args = append(args, "store", store)
//	}
//	cmd := NewIntCmd(args...)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) SortInterfaces(key string, sort *Sort) *SliceCmd {
//	cmd := NewSliceCmd(sort.args(key)...)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) Touch(keys ...string) (int64, error) {
//	args := make([]interface{}, len(keys)+1)
//	args[0] = "touch"
//	for i, key := range keys {
//		args[i+1] = key
//	}
//	cmd := NewIntCmd(args...)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) TTL(key string) *DurationCmd {
//	cmd := NewDurationCmd(time.Second, "ttl", key)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) Type(key string) *StatusCmd {
//	cmd := NewStatusCmd("type", key)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) Scan(cursor uint64, match string, count int64) *ScanCmd {
//	args := []interface{}{"scan", cursor}
//	if match != "" {
//		args = append(args, "match", match)
//	}
//	if count > 0 {
//		args = append(args, "count", count)
//	}
//	cmd := NewScanCmd(c.process, args...)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) SScan(key string, cursor uint64, match string, count int64) *ScanCmd {
//	args := []interface{}{"sscan", key, cursor}
//	if match != "" {
//		args = append(args, "match", match)
//	}
//	if count > 0 {
//		args = append(args, "count", count)
//	}
//	cmd := NewScanCmd(c.process, args...)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) HScan(key string, cursor uint64, match string, count int64) *ScanCmd {
//	args := []interface{}{"hscan", key, cursor}
//	if match != "" {
//		args = append(args, "match", match)
//	}
//	if count > 0 {
//		args = append(args, "count", count)
//	}
//	cmd := NewScanCmd(c.process, args...)
//	c.process(cmd)
//	return cmd
//}
//
//func (client *RedisClient) ZScan(key string, cursor uint64, match string, count int64) *ScanCmd {
//	args := []interface{}{"zscan", key, cursor}
//	if match != "" {
//		args = append(args, "match", match)
//	}
//	if count > 0 {
//		args = append(args, "count", count)
//	}
//	cmd := NewScanCmd(c.process, args...)
//	c.process(cmd)
//	return cmd
//}

// ---------------------------Hash---------------------------

func (client *RedisClient) HDel(key string, fields ...string) (int64, error) {
	args := []interface{}{key}
	for _, field := range fields {
		args = append(args, field)
	}
	return client.Int64(HDel, args...)
}

func (client *RedisClient) HExists(key, field string) (bool, error) {
	return client.Bool(HExists, key, field)
}

func (client *RedisClient) HGet(key, field string) (string, error) {
	return client.String(HGet, key, field)
}

func (client *RedisClient) HGetAll(key string) (map[string]string, error) {
	return client.StringMap(HGetAll, key)
}

func (client *RedisClient) HGetAllToStruct(dest interface{}, key string) error {
	values, err := client.Values(HGetAll, key)
	if err != nil {
		//logs.Errorf("Failed to get values, the error is %#v", err)
		return err
	}
	if len(values) == 0 {
		return ErrNil
	}
	return ScanStruct(values, dest)
}

func (client *RedisClient) HIncrBy(key, field string, incr int64) (int64, error) {
	return client.Int64(HIncrBy, key, field, incr)
}

func (client *RedisClient) HIncrByFloat(key, field string, incr float64) (float64, error) {
	return client.Float64(HIncrByFloat, key, field, incr)
}

func (client *RedisClient) HKeys(key string) ([]string, error) {
	return client.StringSlice(HKeys, key)
}

func (client *RedisClient) HLen(key string) (int64, error) {
	return client.Int64(HLen, key)
}

func (client *RedisClient) HMGet(key string, fields ...string) ([]string, error) {
	args := []interface{}{key}
	for _, field := range fields {
		args = append(args, field)
	}
	return client.StringSlice(HMGet, args...)
}

func (client *RedisClient) HMGetToStruct(dest interface{}, key string, fields ...string) error {
	args := []interface{}{key}
	for _, field := range fields {
		args = append(args, field)
	}
	values, err := client.Values(HMGet, args...)
	if err != nil {
		//logs.Errorf("Failed to get values, the error is %#v", err)
		return err
	}
	return ScanStruct(values, dest)
}

func (client *RedisClient) HMSet(key string, fields map[string]interface{}) (string, error) {
	args := []interface{}{key}
	for k, v := range fields {
		args = append(args, k, v)
	}
	return client.String(HMSet, args...)
}

func (client *RedisClient) HMSetObject(key string, object interface{}) (string, error) {
	return client.String(HMSet, Args{key}.AddFlat(object)...)
}

func (client *RedisClient) HSet(key, field string, value interface{}) (bool, error) {
	return client.Bool(HSet, key, field, value)
}

func (client *RedisClient) HSetNX(key, field string, value interface{}) (bool, error) {
	return client.Bool(HSetNX, key, field, value)
}

func (client *RedisClient) HVals(key string) ([]string, error) {
	return client.StringSlice(HVals, key)
}

// ---------------------------List---------------------------

func (client *RedisClient) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	var args []interface{}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, formatSec(timeout))
	return client.StringSliceWithTimeout(timeout, CmdBLPop, args...)
}

func (client *RedisClient) BRPop(timeout time.Duration, keys ...string) ([]string, error) {
	var args []interface{}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, formatSec(timeout))
	return client.StringSliceWithTimeout(timeout, CmdBRPop, args...)
}

func (client *RedisClient) BRPopLPush(source, destination string, timeout time.Duration) (string, error) {
	return client.StringWithTimeout(timeout, CmdBRPopLPush, source, destination, formatSec(timeout))
}

func (client *RedisClient) LIndex(key string, index int64) (string, error) {
	return client.String(CmdLIndex, key, index)
}

func (client *RedisClient) LInsert(key, op string, pivot, value interface{}) (int64, error) {
	return client.Int64(CmdLInsert, key, op, pivot, value)
}

func (client *RedisClient) LInsertBefore(key string, pivot, value interface{}) (int64, error) {
	return client.Int64(CmdLInsert, key, ParamBefore, pivot, value)
}

func (client *RedisClient) LInsertAfter(key string, pivot, value interface{}) (int64, error) {
	return client.Int64(CmdLInsert, key, ParamAfter, pivot, value)
}

func (client *RedisClient) LLen(key string) (int64, error) {
	return client.Int64(CmdLLen, key)
}

func (client *RedisClient) LPop(key string) (string, error) {
	return client.String(CmdLPop, key)
}

func (client *RedisClient) LPush(key string, values ...interface{}) (int64, error) {
	args := []interface{}{key}
	for _, value := range values {
		args = append(args, value)
	}
	return client.Int64(CmdLPush, args...)
}

func (client *RedisClient) LPushX(key string, value interface{}) (int64, error) {
	return client.Int64(CmdLPushX, key, value)
}

func (client *RedisClient) LRange(key string, start, stop int64) ([]string, error) {
	return client.StringSlice(CmdLRange, key, start, stop)
}

func (client *RedisClient) LRem(key string, count int64, value interface{}) (int64, error) {
	return client.Int64(CmdLRem, key, count, value)
}

func (client *RedisClient) LSet(key string, index int64, value interface{}) (string, error) {
	return client.String(CmdLSet, key, index, value)
}

func (client *RedisClient) LTrim(key string, start, stop int64) (string, error) {
	return client.String(CmdLTrim, key, start, stop)
}

func (client *RedisClient) RPop(key string) (string, error) {
	return client.String(CmdRPop, key)
}

func (client *RedisClient) RPopLPush(source, destination string) (string, error) {
	return client.String(CmdRPopLPush, source, destination)
}

func (client *RedisClient) RPush(key string, values ...interface{}) (int64, error) {
	args := []interface{}{key}
	for _, value := range values {
		args = append(args, value)
	}
	return client.Int64(CmdRPush, args...)
}

func (client *RedisClient) RPushX(key string, value interface{}) (int64, error) {
	return client.Int64(CmdRPushX, key, value)
}

// ---------------------------Set---------------------------

func (client *RedisClient) SAdd(key string, members ...interface{}) (int64, error) {
	args := []interface{}{key}
	args = append(args, members...)
	return client.Int64(CmdSAdd, args...)
}

func (client *RedisClient) SCard(key string) (int64, error) {
	return client.Int64(CmdSCard, key)
}

func (client *RedisClient) SDiff(keys ...string) ([]string, error) {
	var args []interface{}
	for _, key := range keys {
		args = append(args, key)
	}
	return client.StringSlice(CmdSDiff, args...)
}

func (client *RedisClient) SDiffStore(destination string, keys ...string) (int64, error) {
	args := []interface{}{destination}
	for _, key := range keys {
		args = append(args, key)
	}
	return client.Int64(CmdSDiffStore, args...)
}

func (client *RedisClient) SInter(keys ...string) ([]string, error) {
	var args []interface{}
	for _, key := range keys {
		args = append(args, key)
	}
	return client.StringSlice(CmdSInter, args...)
}

func (client *RedisClient) SInterStore(destination string, keys ...string) (int64, error) {
	args := []interface{}{destination}
	for _, key := range keys {
		args = append(args, key)
	}
	return client.Int64(CmdSInterStore, args...)
}

func (client *RedisClient) SIsMember(key string, member interface{}) (bool, error) {
	return client.Bool(CmdSIsMember, key, member)
}

// Redis `SMEMBERS key` command output as a slice
func (client *RedisClient) SMembers(key string) ([]string, error) {
	return client.StringSlice(CmdSMembers, key)
}

// TODO need investigate this function
//// Redis `SMEMBERS key` command output as a map
//func (client *RedisClient) SMembersMap(key string) *StringStructMapCmd {
//	cmd := NewStringStructMapCmd("smembers", key)
//	c.process(cmd)
//	return cmd
//}

func (client *RedisClient) SMove(source, destination string, member interface{}) (bool, error) {
	return client.Bool(CmdSMove, source, destination, member)
}

// Redis `SPOP key` command.
func (client *RedisClient) SPop(key string) (string, error) {
	return client.String(CmdSPop, key)
}

// Redis `SPOP key count` command.
func (client *RedisClient) SPopN(key string, count int64) ([]string, error) {
	return client.StringSlice(CmdSPop, key, count)
}

// Redis `SRANDMEMBER key` command.
func (client *RedisClient) SRandMember(key string) (string, error) {
	return client.String(CmdSRandMember, key)
}

// Redis `SRANDMEMBER key count` command.
func (client *RedisClient) SRandMemberN(key string, count int64) ([]string, error) {
	return client.StringSlice(CmdSRandMember, key, count)
}

func (client *RedisClient) SRem(key string, members ...interface{}) (int64, error) {
	args := []interface{}{key}
	for _, member := range members {
		args = append(args, member)
	}
	return client.Int64(CmdSRem, args...)
}

func (client *RedisClient) SUnion(keys ...string) ([]string, error) {
	var args []interface{}
	for _, key := range keys {
		args = append(args, key)
	}
	return client.StringSlice(CmdSUnion, args...)
}

func (client *RedisClient) SUnionStore(destination string, keys ...string) (int64, error) {
	args := []interface{}{destination}
	for _, key := range keys {
		args = append(args, key)
	}
	return client.Int64(CmdSUnionStore, args...)
}

// ---------------------------Sorted Set---------------------------

// Z represents sorted set member.
type Z struct {
	Score  float64
	Member interface{}
}

// ZStore is used as an arg to ZInterStore and ZUnionStore.
type ZStore struct {
	Weights []float64
	// Can be SUM, MIN or MAX.
	Aggregate string
}

func (client *RedisClient) zAdd(zcmd string, args []interface{}, members ...Z) (int64, error) {
	for _, member := range members {
		args = append(args, member.Score, member.Member)
	}

	return client.Int64(zcmd, args...)
}

func (client *RedisClient) ZAdd(key string, members ...Z) (int64, error) {
	args := []interface{}{key}
	return client.zAdd(ZAdd, args, members...)
}

// Redis `ZADD key NX score member [score member ...]` command.
func (client *RedisClient) ZAddNX(key string, members ...Z) (int64, error) {
	args := []interface{}{key, ParamNX}
	return client.zAdd(ZAdd, args, members...)
}

// Redis `ZADD key XX score member [score member ...]` command.
func (client *RedisClient) ZAddXX(key string, members ...Z) (int64, error) {
	args := []interface{}{key, ParamXX}
	return client.zAdd(ZAdd, args, members...)
}

// Redis `ZADD key CH score member [score member ...]` command.
func (client *RedisClient) ZAddCh(key string, members ...Z) (int64, error) {
	args := []interface{}{key, ParamCH}
	return client.zAdd(ZAdd, args, members...)
}

// Redis `ZADD key NX CH score member [score member ...]` command.
func (client *RedisClient) ZAddNXCh(key string, members ...Z) (int64, error) {
	args := []interface{}{key, ParamNX, ParamCH}
	return client.zAdd(ZAdd, args, members...)
}

// Redis `ZADD key XX CH score member [score member ...]` command.
func (client *RedisClient) ZAddXXCh(key string, members ...Z) (int64, error) {
	args := []interface{}{key, ParamXX, ParamCH}
	return client.zAdd(ZAdd, args, members...)
}

func (client *RedisClient) zIncr(zcmd string, args []interface{}, members ...Z) (float64, error) {
	for _, member := range members {
		args = append(args, member.Score, member.Member)
	}

	return client.Float64(zcmd, args...)
}

// Redis `ZADD key INCR score member` command.
func (client *RedisClient) ZIncr(key string, member Z) (float64, error) {
	args := []interface{}{key, ParamINCR}
	return client.zIncr(ZAdd, args, member)
}

// Redis `ZADD key NX INCR score member` command.
func (client *RedisClient) ZIncrNX(key string, member Z) (float64, error) {
	args := []interface{}{key, ParamINCR, ParamNX}
	return client.zIncr(ZAdd, args, member)
}

// Redis `ZADD key XX INCR score member` command.
func (client *RedisClient) ZIncrXX(key string, member Z) (float64, error) {
	args := []interface{}{key, ParamINCR, ParamXX}
	return client.zIncr(ZAdd, args, member)
}

func (client *RedisClient) ZCard(key string) (int64, error) {
	args := []interface{}{key}
	return client.Int64(ZCard, args...)
}

func (client *RedisClient) ZCount(key, min, max string) (int64, error) {
	args := []interface{}{key, min, max}
	return client.Int64(ZCount, args...)
}

func (client *RedisClient) ZLexCount(key, min, max string) (int64, error) {
	args := []interface{}{key, min, max}
	return client.Int64(ZLexCount, args...)
}

func (client *RedisClient) ZIncrBy(key string, increment float64, member string) (float64, error) {
	args := []interface{}{key, increment, member}
	return client.Float64(ZIncBy, args...)
}

func (client *RedisClient) ZInterStore(destination string, store ZStore, keys ...string) (int64, error) {
	args := []interface{}{destination, len(keys)}
	for _, key := range keys {
		args = append(args, key)
	}
	if len(store.Weights) > 0 {
		args = append(args, ParamWeights)
		for _, weight := range store.Weights {
			args = append(args, weight)
		}
	}
	if store.Aggregate != "" {
		args = append(args, ParamAggregate, store.Aggregate)
	}
	return client.Int64(ZInterStore, args...)
}

// TODO when withScores, the return data should not be StringSlice
func (client *RedisClient) zRange(key string, start, stop int64, withScores bool) ([]string, error) {
	args := []interface{}{
		key,
		start,
		stop,
	}
	if withScores {
		args = append(args, ParamWithScores)
	}
	return client.StringSlice(ZRange, args...)
}

func (client *RedisClient) ZRange(key string, start, stop int64) ([]string, error) {
	return client.zRange(key, start, stop, false)
}

// TODO should I use string map or int map
func (client *RedisClient) ZRangeWithScores(key string, start, stop int64) (map[string]string, error) {
	args := []interface{}{key, start, stop, ParamWithScores}
	return client.StringMap(ZRange, args...)
}

type ZRangeBy struct {
	Min, Max      string
	Offset, Count int64
}

func (client *RedisClient) zRangeBy(zcmd, key string, opt ZRangeBy, withScores bool) ([]string, error) {
	args := []interface{}{key, opt.Min, opt.Max}
	if withScores {
		args = append(args, ParamWithScores)
	}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(args, ParamLimit, opt.Offset, opt.Count)
	}
	return client.StringSlice(zcmd, args...)
}

func (client *RedisClient) ZRangeByScore(key string, opt ZRangeBy) ([]string, error) {
	return client.zRangeBy(ZRangeByScore, key, opt, false)
}

func (client *RedisClient) ZRangeByLex(key string, opt ZRangeBy) ([]string, error) {
	return client.zRangeBy(ZRangeByLex, key, opt, false)
}

// TODO should I use string map or int map
func (client *RedisClient) ZRangeByScoreWithScores(key string, opt ZRangeBy) (map[string]string, error) {
	args := []interface{}{key, opt.Min, opt.Max, "withscores"}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			ParamLimit,
			opt.Offset,
			opt.Count,
		)
	}
	return client.StringMap(ZRangeByScore, args...)
}

func (client *RedisClient) ZRank(key, member string) (int64, error) {
	args := []interface{}{key, member}
	return client.Int64(ZRank, args...)
}

func (client *RedisClient) ZRem(key string, members ...string) (int64, error) {
	args := []interface{}{key}
	for _, member := range members {
		args = append(args, member)
	}
	return client.Int64(ZRem, args...)
}

func (client *RedisClient) ZRemRangeByLex(key, min, max string) (int64, error) {
	args := []interface{}{key, min, max}
	return client.Int64(ZRemRangeByLex, args...)
}

func (client *RedisClient) ZRemRangeByRank(key string, start, stop int64) (int64, error) {
	args := []interface{}{key, start, stop}
	return client.Int64(ZRemRangeByRank, args...)
}

func (client *RedisClient) ZRemRangeByScore(key, min, max string) (int64, error) {
	args := []interface{}{key, min, max}
	return client.Int64(ZRemRangeByScore, args...)
}

func (client *RedisClient) ZRevRange(key string, start, stop int64) ([]string, error) {
	args := []interface{}{key, start, stop}
	return client.StringSlice(ZRevRange, args...)
}

// TODO should I use string map or int map
func (client *RedisClient) ZRevRangeWithScores(key string, start, stop int64) (map[string]string, error) {
	args := []interface{}{key, start, stop, ParamWithScores}
	return client.StringMap(ZRevRange, args...)
}

func (client *RedisClient) zRevRangeBy(zcmd, key string, opt ZRangeBy) ([]string, error) {
	args := []interface{}{key, opt.Max, opt.Min}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(args, "LIMIT", opt.Offset, opt.Count)
	}
	return client.StringSlice(zcmd, args...)
}

func (client *RedisClient) ZRevRangeByLex(key string, opt ZRangeBy) ([]string, error) {
	return client.zRevRangeBy(ZRevRangeByLex, key, opt)
}

func (client *RedisClient) ZRevRangeByScore(key string, opt ZRangeBy) ([]string, error) {
	return client.zRevRangeBy(ZRevRangeByScore, key, opt)
}

func (client *RedisClient) ZRevRangeByScoreWithScores(key string, opt ZRangeBy) (map[string]string, error) {
	args := []interface{}{key, opt.Max, opt.Min, ParamWithScores}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			ParamLimit,
			opt.Offset,
			opt.Count,
		)
	}
	return client.StringMap(ZRevRangeByScore, args...)
}

func (client *RedisClient) ZRevRank(key, member string) (int64, error) {
	args := []interface{}{key, member}
	return client.Int64(ZRevRank, args...)
}

func (client *RedisClient) ZScore(key, member string) (float64, error) {
	args := []interface{}{key, member}
	return client.Float64(ZScore, args...)
}

func (client *RedisClient) ZUnionStore(dest string, store ZStore, keys ...string) (int64, error) {
	args := []interface{}{dest, len(keys)}
	for _, key := range keys {
		args = append(args, key)
	}
	if len(store.Weights) > 0 {
		args = append(args, ParamWeights)
		for _, weight := range store.Weights {
			args = append(args, weight)
		}
	}
	if store.Aggregate != "" {
		args = append(args, ParamAggregate, store.Aggregate)
	}
	return client.Int64(ZUnionStore, args...)
}

// ---------------------------String---------------------------

func (client RedisClient) Append(key, value string) (int64, error) {
	return client.Int64(CmdAppend, key, value)
}

type BitCount struct {
	Start, End int64
}

func (client RedisClient) BitCount(key string, bitCount *BitCount) (int64, error) {
	args := []interface{}{key}
	if bitCount != nil {
		args = append(
			args,
			bitCount.Start,
			bitCount.End,
		)
	}
	return client.Int64(CmdBitCount, args...)
}

func (client RedisClient) bitOp(op, destKey string, keys ...string) (int64, error) {
	args := []interface{}{op, destKey}
	for _, key := range keys {
		args = append(args, key)
	}
	return client.Int64(CmdBitOp, args...)
}

func (client RedisClient) BitOpAnd(destKey string, keys ...string) (int64, error) {
	return client.bitOp(ParamAnd, destKey, keys...)
}

func (client RedisClient) BitOpOr(destKey string, keys ...string) (int64, error) {
	return client.bitOp(ParamOr, destKey, keys...)
}

func (client RedisClient) BitOpXor(destKey string, keys ...string) (int64, error) {
	return client.bitOp(ParamXOR, destKey, keys...)
}

func (client RedisClient) BitOpNot(destKey string, key string) (int64, error) {
	return client.bitOp(ParamNot, destKey, key)
}

func (client RedisClient) BitPos(key string, bit int64, pos ...int64) (int64, error) {
	args := []interface{}{key, bit}
	if len(pos) > 2 {
		//logs.Errorf("Too many arguments, the length of pos is %d large then 2", len(pos))
	}
	for index, item := range pos {
		if index > 2 {
			break
		}
		args = append(args, item)
	}
	return client.Int64(CmdBitPos, args...)
}

func (client RedisClient) Decr(key string) (int64, error) {
	return client.Int64(CmdDecr, key)
}

func (client RedisClient) DecrBy(key string, decrement int64) (int64, error) {
	return client.Int64(CmdDecrBy, key, decrement)
}

// Redis `GET key` command. It returns Nil error when key does not exist.
func (client RedisClient) Get(key string) (string, error) {
	return client.String(CmdGet, key)
}

func (client RedisClient) GetInt64(key string) (int64, error) {
	return client.Int64(CmdGet, key)
}

func (client RedisClient) GetBit(key string, offset int64) (int64, error) {
	return client.Int64(CmdGetBit, key, offset)
}

func (client RedisClient) GetRange(key string, start, end int64) (string, error) {
	return client.String(CmdGetRange, key, start, end)
}

func (client RedisClient) GetSet(key string, value interface{}) (string, error) {
	return client.String(CmdGetSet, key, value)
}

func (client RedisClient) Incr(key string) (int64, error) {
	return client.Int64(CmdIncr, key)
}

func (client RedisClient) IncrBy(key string, value int64) (int64, error) {
	return client.Int64(CmdIncrBy, key, value)
}

func (client RedisClient) IncrByFloat(key string, value float64) (float64, error) {
	return client.Float64(CmdIncrByFloat, key, value)
}

func (client RedisClient) MGet(keys ...string) ([]string, error) {
	var args []interface{}
	for _, key := range keys {
		args = append(args, key)
	}
	return client.StringSlice(CmdMGet, args...)
}

func (client RedisClient) MSet(pairs ...interface{}) (string, error) {
	return client.String(CmdMSet, pairs...)
}

func (client RedisClient) MSetNX(pairs ...interface{}) (bool, error) {
	return client.Bool(CmdMSetNX, pairs...)
}

// Redis `SET key value [expiration]` command.
//
// Use expiration for `SETEX`-like behavior.
// Zero expiration means the key has no expiration time.
func (client RedisClient) Set(key string, value interface{}, expiration time.Duration) (string, error) {
	args := []interface{}{key, value}
	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, ParamPX, formatMs(expiration))
		} else {
			args = append(args, ParamEX, formatSec(expiration))
		}
	}
	return client.String(CmdSet, args...)
}

func (client RedisClient) SetInt64(key string, value int64) (string, error) {
	return client.Set(key, value, 0)
}

func (client RedisClient) SetBit(key string, offset int64, value int) (int64, error) {
	return client.Int64(CmdSetBit, key, offset, value)
}

// Redis `SET key value [expiration] NX` command.
//
// Zero expiration means the key has no expiration time.
func (client RedisClient) SetNX(key string, value interface{}, expiration time.Duration) (bool, error) {
	if expiration == 0 {
		// Use old `SETNX` to support old Redis versions.
		return client.Bool(CmdSetNX, key, value)
	} else {
		if usePrecise(expiration) {
			return client.Bool(CmdSet, key, value, ParamPX, formatMs(expiration), ParamNX)
		} else {
			return client.Bool(CmdSet, key, value, ParamEX, formatSec(expiration), ParamNX)
		}
	}
}

// Redis `SET key value [expiration] XX` command.
//
// Zero expiration means the key has no expiration time.
func (client RedisClient) SetXX(key string, value interface{}, expiration time.Duration) (string, error) {
	if expiration == 0 {
		return client.String(CmdSet, key, value, ParamXX)
	} else {
		if usePrecise(expiration) {
			return client.String(CmdSet, key, value, ParamPX, formatMs(expiration), ParamXX)
		} else {
			return client.String(CmdSet, key, value, ParamEX, formatSec(expiration), ParamXX)
		}
	}
}

func (client RedisClient) SetRange(key string, offset int64, value string) (int64, error) {
	return client.Int64(CmdSetRange, key, offset, value)
}

func (client RedisClient) StrLen(key string) (int64, error) {
	return client.Int64(CmdStrLen, key)
}
