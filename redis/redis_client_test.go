package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	FieldName1 = "field1"
	FieldName2 = "field2"
	FieldName0 = "field0"
)

type TestStruct struct {
	Field1 string `redis:"field1"`
	Field2 string `redis:"field2"`
	Field3 string `redis:"field3"`
}

const (
	addr = ":6379"
	pass = ""
)

func getClient() *RedisClient {
	return GetRedisClient(addr, pass)
}

func isArraysEqual(array1, array2 []string) bool {
	if len(array1) != len(array2) {
		//logs.Errorf("The length of array1 is %d is not equal with array2 %d", len(array1), len(array2))
		return false
	}
	for _, item1 := range array1 {
		found := false
		for _, item2 := range array2 {
			if item1 == item2 {
				found = true
				break
			}
		}
		if !found {
			//logs.Errorf("Failed to find %s in array %#v", item1, array2)
			return false
		}
	}
	return true
}

func isArraysEqualWithSameOrder(array1, array2 []string) bool {
	if len(array1) != len(array2) {
		//logs.Errorf("The length of array1 is %d is not equal with array2 %d", len(array1), len(array2))
		return false
	}
	for index := 0; index < len(array1); index++ {
		if array1[index] != array2[index] {
			return false
		}
	}
	return true
}

// Hash

func TestFVRedisClient_HDel(t *testing.T) {
	client := getClient()

	key := "tk_hdel"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, "foo")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	count, err := client.HDel(key, FieldName1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.HDel(key, FieldName1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if count != 0 {
		t.Errorf("The count is not correct")
	}

	client.Del(key)
}

func TestFVRedisClient_HExists(t *testing.T) {
	client := getClient()

	key := "tk_hexists"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, "foo")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	exists, err := client.HExists(key, FieldName1)
	if err != nil {
		t.Errorf("Failed to check exists for key %s, the error is %#v", key, err)
	}
	if !exists {
		t.Errorf("The result of hexists is not correct")
	}

	exists, err = client.HExists(key, FieldName2)
	if err != nil {
		t.Errorf("Failed to check exists for key %s, the error is %#v", key, err)
	}
	if exists {
		t.Errorf("The result of hexists is not correct")
	}

	client.Del(key)
}

func TestFVRedisClient_HGet(t *testing.T) {
	client := getClient()

	key := "tk_hget"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, "foo")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	value, err := client.HGet(key, FieldName1)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	}
	if value != "foo" {
		t.Errorf("The value is not correct")
	}

	value, err = client.HGet(key, FieldName2)
	if err != ErrNil {
		t.Errorf("Failed to get %s, the error is %#v, ErrNil is expected", key, err)
	}

	client.Del(key)
}

func TestFVRedisClient_HGetAll(t *testing.T) {
	client := getClient()

	key := "tk_hgetall"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	isSet, err = client.HSet(key, FieldName2, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	values, err := client.HGetAll(key)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if len(values) != 2 {
		t.Errorf("The values is not correct")
	} else {
		for k, v := range values {
			fmt.Printf("The key is %s and value is %s", k, v)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_HGetAllToStruct(t *testing.T) {
	client := getClient()

	key := "tk_hgetalltostruct"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	obj := TestStruct{
		Field1: "Hello",
		Field2: "World",
	}

	result, err := client.HMSetObject(key, obj)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if result != "OK" {
		t.Errorf("Failed to save object, the result is %s", result)
	}

	newObj := TestStruct{}
	err = client.HGetAllToStruct(&newObj, key)
	if err != nil {
		t.Errorf("Failed to get all to struct for key %s, the error is %#v", key, err)
	}
	if newObj.Field1 != "Hello" && newObj.Field2 != "World" {
		t.Errorf("Failed to getAllToStruct, the new object is %#v", newObj)
	}

	client.Del(key)
}

func TestFVRedisClient_HIncrBy(t *testing.T) {
	client := getClient()

	key := "tk_hincrby"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, 5)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	value, err := client.HIncrBy(key, FieldName1, 1)
	if err != nil {
		t.Errorf("Failed to call HIncrBy, the error is %#v", err)
	} else if value != 6 {
		t.Errorf("The result of HIncrBy is not correct")
	}

	value, err = client.HIncrBy(key, FieldName1, -1)
	if err != nil {
		t.Errorf("Failed to call HIncrBy, the error is %#v", err)
	} else if value != 5 {
		t.Errorf("The result of HIncrBy is not correct")
	}

	value, err = client.HIncrBy(key, FieldName1, -10)
	if err != nil {
		t.Errorf("Failed to call HIncrBy, the error is %#v", err)
	} else if value != -5 {
		t.Errorf("The result of HIncrBy is not correct")
	}

	client.Del(key)
}

func TestFVRedisClient_HIncrByFloat(t *testing.T) {
	client := getClient()

	key := "tk_hincrbyfloat"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, 10.50)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	value, err := client.HIncrByFloat(key, FieldName1, 0.1)
	if err != nil {
		t.Errorf("Failed to call HIncrByFloat, the error is %#v", err)
	} else if value != 10.6 {
		t.Errorf("The result of HIncrByFloat is not correct")
	}

	value, err = client.HIncrByFloat(key, FieldName1, -5)
	if err != nil {
		t.Errorf("Failed to call HIncrByFloat, the error is %#v", err)
	} else if value != 5.6 {
		t.Errorf("The result of HIncrByFloat is not correct")
	}

	isSet, err = client.HSet(key, FieldName1, 5.0e3)
	if err != nil {
		t.Errorf("Failed to call HIncrByFloat, the error is %#v", err)
	} else if isSet {
		t.Errorf("The result of HSet is not correct")
	}

	value, err = client.HIncrByFloat(key, FieldName1, 2.0e2)
	if err != nil {
		t.Errorf("Failed to call HIncrByFloat, the error is %#v", err)
	} else if value != 5200 {
		t.Errorf("The result of HIncrByFloat is not correct")
	}

	client.Del(key)
}

func TestFVRedisClient_HKeys(t *testing.T) {
	client := getClient()

	key := "tk_hkeys"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	isSet, err = client.HSet(key, FieldName2, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	values, err := client.HKeys(key)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if !isArraysEqual(values, []string{"field1", "field2"}) {
		t.Errorf("The values is not correct")
	}

	client.Del(key)
}

func TestFVRedisClient_HLen(t *testing.T) {
	client := getClient()

	key := "tk_hlen"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	isSet, err = client.HSet(key, FieldName2, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	length, err := client.HLen(key)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if length != 2 {
		t.Errorf("The values is not correct")
	}

	client.Del(key)
}

// nil will return as empty string
func TestFVRedisClient_HMGet(t *testing.T) {
	client := getClient()

	key := "tk_hmget"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	isSet, err = client.HSet(key, FieldName2, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	values, err := client.HMGet(key, FieldName1, FieldName2, FieldName0)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if len(values) != 3 {
		t.Errorf("The values is not correct")
	} else {
		for _, item := range values {
			fmt.Printf("The item is %s", item)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_HMGetToStruct(t *testing.T) {
	client := getClient()

	key := "tk_hmgettostruct"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	obj := TestStruct{
		Field1: "Hello",
		Field2: "World",
		Field3: "Third",
	}

	result, err := client.HMSetObject(key, obj)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if result != "OK" {
		t.Errorf("Failed to save object, the result is %s", result)
	}

	newObj := TestStruct{}
	err = client.HMGetToStruct(&newObj, key, FieldName1, FieldName2)
	if err != nil {
		t.Errorf("Failed to get all to struct for key %s, the error is %#v", key, err)
	}
	if newObj.Field1 != "Hello" && newObj.Field2 != "World" && newObj.Field3 != "" {
		t.Errorf("Failed to getAllToStruct, the new object is %#v", newObj)
	}

	client.Del(key)
}

func TestFVRedisClient_HMSet(t *testing.T) {
	client := getClient()

	key := "tk_hmset"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	result, err := client.HMSet(key, map[string]interface{}{FieldName1: "Hello", FieldName2: "World"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if result != "OK" {
		t.Errorf("The hmset result is false")
	}

	value, err := client.HGet(key, FieldName1)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if value != "Hello" {
		t.Errorf("The values is not correct")
	}

	value, err = client.HGet(key, FieldName2)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if value != "World" {
		t.Errorf("The values is not correct")
	}

	client.Del(key)
}

func TestFVRedisClient_HMSetObject(t *testing.T) {
	client := getClient()

	key := "tk_hmsetobject"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	isSet, err = client.HSet(key, FieldName2, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	values, err := client.HMGet(key, FieldName1, FieldName2, FieldName0)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if len(values) != 3 {
		t.Errorf("The values is not correct")
	} else {
		for _, item := range values {
			fmt.Printf("The item is %s", item)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_HSet(t *testing.T) {
	client := getClient()

	key := "tk_hset"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	value, err := client.HGet(key, FieldName1)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if value != "Hello" {
		t.Errorf("The values is not correct")
	}

	client.Del(key)
}

func TestFVRedisClient_HSetNX(t *testing.T) {
	client := getClient()

	key := "tk_hsetnx"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSetNX(key, FieldName1, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hsetnx result is false")
	}

	isSet, err = client.HSetNX(key, FieldName1, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if isSet {
		t.Errorf("The hsetnx result is false")
	}

	value, err := client.HGet(key, FieldName1)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if value != "Hello" {
		t.Errorf("The values is not correct")
	}

	client.Del(key)
}

func TestFVRedisClient_HVals(t *testing.T) {
	client := getClient()

	key := "tk_hvals"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	isSet, err := client.HSet(key, FieldName1, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	isSet, err = client.HSet(key, FieldName2, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if !isSet {
		t.Errorf("The hset result is false")
	}

	values, err := client.HVals(key)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if !isArraysEqual(values, []string{"Hello", "World"}) {
		t.Errorf("The values is not correct")
	}

	client.Del(key)
}

// ---------------------------List---------------------------

func TestRedisClient_BLPop(t *testing.T) {

}

func TestRedisClient_BRPop(t *testing.T) {

}

func TestRedisClient_BRPopLPush(t *testing.T) {

}

func TestRedisClient_LIndex(t *testing.T) {
	client := getClient()

	key := "tk_lindex"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.LPush(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.LPush(key, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call zadd")
	}

	item, err := client.LIndex(key, 0)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if item != "Hello" {
		t.Errorf("The item `%s` is not correct", item)
	}

	item, err = client.LIndex(key, -1)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if item != "World" {
		t.Errorf("The item `%s` is not correct", item)
	}

	item, err = client.LIndex(key, 3)
	if err != ErrNil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	}

	client.Del(key)
}

func TestRedisClient_LInsert(t *testing.T) {
	client := getClient()

	key := "tk_linsert"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.LInsertBefore(key, "World", "There")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 3 {
		t.Errorf("Failed to call linsertbefore")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to call lrange for key %s, the error is %#v", key, err)
	} else if !isArraysEqualWithSameOrder(list, []string{"Hello", "There", "World"}) {
		t.Errorf("Failed to call lrange, the list is %#v", list)
	}

	client.Del(key)
}

func TestRedisClient_LInsertAfter(t *testing.T) {
	client := getClient()

	key := "tk_linsertafter"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.LInsertAfter(key, "Hello", "There")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 3 {
		t.Errorf("Failed to call linsertbefore")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to call lrange for key %s, the error is %#v", key, err)
	} else if !isArraysEqualWithSameOrder(list, []string{"Hello", "There", "World"}) {
		t.Errorf("Failed to call lrange, the list is %#v", list)
	}

	client.Del(key)
}

func TestRedisClient_LInsertBefore(t *testing.T) {
	client := getClient()

	key := "tk_linsertbefore"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.LInsertBefore(key, "World", "There")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 3 {
		t.Errorf("Failed to call linsertbefore")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to call lrange for key %s, the error is %#v", key, err)
	} else if !isArraysEqualWithSameOrder(list, []string{"Hello", "There", "World"}) {
		t.Errorf("Failed to call lrange, the list is %#v", list)
	}

	client.Del(key)
}

func TestRedisClient_LLen(t *testing.T) {
	client := getClient()

	key := "tk_llen"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.LPush(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.LPush(key, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.LLen(key)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("The length %d is not correct", num)
	}

	client.Del(key)
}

func TestRedisClient_LPop(t *testing.T) {
	client := getClient()

	key := "tk_lpop"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "one")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "two")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 3 {
		t.Errorf("Failed to call rpush")
	}

	item, err := client.LPop(key)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if item != "one" {
		t.Errorf("The item %s is not correct", item)
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"two", "three"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key)
}

func TestRedisClient_LPush(t *testing.T) {
	client := getClient()

	key := "tk_lpush"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.LPush(key, "world")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.LPush(key, "hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"hello", "world"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key)
}

func TestRedisClient_LPushX(t *testing.T) {
	client := getClient()

	key := "tk_lpushx"
	key2 := "tk_lpushx2"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	_, err = client.Del(key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.LPush(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.LPushX(key, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.LPushX(key2, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 0 {
		t.Errorf("Failed to call rpush")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"Hello", "World"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	list, err = client.LRange(key2, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key)
	client.Del(key2)
}

func TestRedisClient_LRange(t *testing.T) {
	client := getClient()

	key := "tk_lrange"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "one")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "two")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 3 {
		t.Errorf("Failed to call rpush")
	}

	list, err := client.LRange(key, 0, 0)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"one"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	list, err = client.LRange(key, -3, 2)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"one", "two", "three"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	list, err = client.LRange(key, -100, 100)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"one", "two", "three"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	list, err = client.LRange(key, 5, 10)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key)
}

func TestRedisClient_LRem(t *testing.T) {
	client := getClient()

	key := "tk_lrem"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "world")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "foo")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 3 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 4 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.LRem(key, -2, "hello")
	if err != nil {
		t.Errorf("Failed to rem item from %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call lrem")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"world", "foo"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key)
}

func TestRedisClient_LSet(t *testing.T) {
	client := getClient()

	key := "tk_lset"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "one")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "two")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 3 {
		t.Errorf("Failed to call rpush")
	}

	status, err := client.LSet(key, 0, "four")
	if err != nil {
		t.Errorf("Failed to set item with key %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("Failed to call lset")
	}

	status, err = client.LSet(key, -2, "five")
	if err != nil {
		t.Errorf("Failed to set item with key %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("Failed to call lset")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"four", "five", "three"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key)
}

func TestRedisClient_LTrim(t *testing.T) {
	client := getClient()

	key := "tk_ltrim"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "one")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "two")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 3 {
		t.Errorf("Failed to call rpush")
	}

	status, err := client.LTrim(key, 1, -1)
	if err != nil {
		t.Errorf("Failed to trim item with key %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("Failed to call ltrim")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"two", "three"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key)
}

func TestRedisClient_RPop(t *testing.T) {
	client := getClient()

	key := "tk_rpop"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "one")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "two")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 3 {
		t.Errorf("Failed to call rpush")
	}

	item, err := client.RPop(key)
	if err != nil {
		t.Errorf("Failed to trim item with key %s, the error is %#v", key, err)
	} else if item != "three" {
		t.Errorf("Failed to call ltrim")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"one", "two"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key)
}

func TestRedisClient_RPopLPush(t *testing.T) {
	client := getClient()

	key := "tk_rpoplpush"
	key2 := "tk_rpoplpush2"

	_, err := client.Del(key, key2)
	if err != nil {
		t.Errorf("Failed to delete key %s, %s", key, key2)
		return
	}

	num, err := client.RPush(key, "one")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "two")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 3 {
		t.Errorf("Failed to call rpush")
	}

	item, err := client.RPopLPush(key, key2)
	if err != nil {
		t.Errorf("Failed to trim item with key %s, the error is %#v", key, err)
	} else if item != "three" {
		t.Errorf("Failed to call ltrim")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"one", "two"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	list, err = client.LRange(key2, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"three"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key, key2)
}

func TestRedisClient_RPush(t *testing.T) {
	client := getClient()

	key := "tk_rpush"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPush(key, "world")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpush")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"hello", "world"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key)
}

func TestRedisClient_RPushX(t *testing.T) {
	client := getClient()

	key := "tk_rpushx"
	key2 := "tk_rpushx2"

	_, err := client.Del(key, key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.RPush(key, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to call rpush")
	}

	num, err = client.RPushX(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to call rpushx")
	}

	num, err = client.RPushX(key2, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 0 {
		t.Errorf("Failed to call rpushx")
	}

	list, err := client.LRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"Hello", "World"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	list, err = client.LRange(key2, 0, -1)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key, key2)
}

// ---------------------------Set---------------------------

func TestRedisClient_SAdd(t *testing.T) {
	client := getClient()
	key := "tk_sadd"
	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}
	count, err := client.SAdd(key, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 0 {
		t.Errorf("The count is not correct")
	}

	client.Del(key)
}

func TestRedisClient_SCard(t *testing.T) {
	client := getClient()
	key := "tk_scard"
	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}
	count, err := client.SAdd(key, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SCard(key)
	if err != nil {
		t.Errorf("Failed to call scard for key %s, the error is %#v", key, err)
	}
	if count != 2 {
		t.Errorf("The count is not correct")
	}

	client.Del(key)
}

func TestRedisClient_SDiff(t *testing.T) {
	client := getClient()

	key1 := "tk_sdiff_key1"
	key2 := "tk_sdiff_key2"

	_, err := client.Del(key1)
	if err != nil {
		t.Errorf("Failed to delete key %s", key1)
		return
	}

	_, err = client.Del(key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key2)
		return
	}

	count, err := client.SAdd(key1, "a")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "b")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "d")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "e")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	diffs, err := client.SDiff(key1, key2)
	if err != nil {
		t.Errorf("Failed to call sdiff, the error is %#v", err)
	}
	if len(diffs) != 2 {
		t.Errorf("The length of the diffs is %d not equal 2", len(diffs))
	} else {
		for _, item := range diffs {
			fmt.Printf("The diff is %s", item)
		}
	}

	client.Del(key1)
	client.Del(key2)
}

func TestRedisClient_SDiffStore(t *testing.T) {
	client := getClient()

	key := "tk_sdiffstore"
	key1 := "tk_sdiffstore_key1"
	key2 := "tk_sdiffstore_key2"

	_, err := client.Del(key1)
	if err != nil {
		t.Errorf("Failed to delete key %s", key1)
		return
	}

	_, err = client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	_, err = client.Del(key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key2)
		return
	}

	count, err := client.SAdd(key1, "a")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "b")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "d")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "e")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SDiffStore(key, key1, key2)
	if err != nil {
		t.Errorf("Failed to call sdiff, the error is %#v", err)
	}
	if count != 2 {
		t.Errorf("The count is %d is not equal to 2", count)
	} else {
		members, err := client.SMembers(key)
		if err != nil {
			t.Errorf("Failed to get members, the error is %#v", err)
		} else {
			for _, member := range members {
				fmt.Printf("The member is %s", member)
			}
		}
	}

	client.Del(key1)
	client.Del(key2)
	client.Del(key)
}

func TestRedisClient_SInter(t *testing.T) {
	client := getClient()

	key1 := "tk_sinter_key1"
	key2 := "tk_sinter_key2"

	_, err := client.Del(key1)
	if err != nil {
		t.Errorf("Failed to delete key %s", key1)
		return
	}

	_, err = client.Del(key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key2)
		return
	}

	count, err := client.SAdd(key1, "a")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "b")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "d")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "e")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	inters, err := client.SInter(key1, key2)
	if err != nil {
		t.Errorf("Failed to call sdiff, the error is %#v", err)
	}
	if len(inters) != 1 {
		t.Errorf("The length of inter is %d, which is not 1", len(inters))
	} else {
		if inters[0] != "c" {
			t.Errorf("The inter is not correct")
		}
	}

	client.Del(key1)
	client.Del(key2)
}

func TestRedisClient_SInterStore(t *testing.T) {
	client := getClient()

	key := "tk_sinterstore"
	key1 := "tk_sinterstore_key1"
	key2 := "tk_sinterstore_key2"

	_, err := client.Del(key1)
	if err != nil {
		t.Errorf("Failed to delete key %s", key1)
		return
	}

	_, err = client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	_, err = client.Del(key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key2)
		return
	}

	count, err := client.SAdd(key1, "a")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "b")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "d")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "e")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SInterStore(key, key1, key2)
	if err != nil {
		t.Errorf("Failed to call sdiff, the error is %#v", err)
	}
	if count != 1 {
		t.Errorf("The count is %d is not equal to 2", count)
	} else {
		members, err := client.SMembers(key)
		if err != nil {
			t.Errorf("Failed to get members, the error is %#v", err)
		} else {
			for _, member := range members {
				fmt.Printf("The member is %s", member)
			}
		}
	}

	client.Del(key1)
	client.Del(key2)
	client.Del(key)
}

func TestRedisClient_SIsMember(t *testing.T) {
	client := getClient()
	key := "tk_sismember"
	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}
	count, err := client.SAdd(key, "one")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	isMember, err := client.SIsMember(key, "one")
	if err != nil {
		t.Errorf("Failed to check ismember, the error is %#v", err)
	}
	if !isMember {
		t.Errorf("one should true for is member")
	}

	isMember, err = client.SIsMember(key, "two")
	if err != nil {
		t.Errorf("Failed to check ismember, the error is %#v", err)
	}
	if isMember {
		t.Errorf("two should false for is member")
	}

	client.Del(key)
}

func TestRedisClient_SMembers(t *testing.T) {
	client := getClient()
	key := "tk_smembers"
	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	count, err := client.SAdd(key, "Hello")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	members, err := client.SMembers(key)
	if err != nil {
		t.Errorf("Failed to get members, the error is %#v", err)
	}
	if len(members) != 2 {
		t.Errorf("The length of members is not equal to 2")
	} else {
		for _, member := range members {
			fmt.Printf("The member is %s", member)
		}
	}

	client.Del(key)
}

func TestRedisClient_SMove(t *testing.T) {
	client := getClient()

	key1 := "tk_smove_key1"
	key2 := "tk_smove_key2"

	_, err := client.Del(key1)
	if err != nil {
		t.Errorf("Failed to delete key %s", key1)
		return
	}

	_, err = client.Del(key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key2)
		return
	}

	count, err := client.SAdd(key1, "one")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "two")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	moved, err := client.SMove(key1, key2, "two")
	if err != nil {
		t.Errorf("Failed to call sdiff, the error is %#v", err)
	}
	if !moved {
		t.Errorf("The result moved should be true")
	}

	members1, err := client.SMembers(key1)
	if err != nil {
		t.Errorf("Failed to get members for key %s, the error is %#v", key1, err)
	} else {
		if !isArraysEqual(members1, []string{"one"}) {
			t.Errorf("The array is not as expected")
		}
	}

	members2, err := client.SMembers(key2)
	if err != nil {
		t.Errorf("Failed to get members for key %s, the error is %#v", key2, err)
	} else {
		if !isArraysEqual(members2, []string{"two", "three"}) {
			t.Errorf("The array is not as expected")
		}
	}

	client.Del(key1)
	client.Del(key2)
}

func TestRedisClient_SPop(t *testing.T) {
	client := getClient()
	key := "tk_spop"
	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	count, err := client.SAdd(key, "one")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "two")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	item, err := client.SPop(key)
	if err != nil {
		t.Errorf("Failed to get members, the error is %#v", err)
	} else {
		fmt.Printf("The pop item is %s", item)
	}

	members, err := client.SMembers(key)
	if err != nil {
		t.Errorf("Failed to get members, the error is %#v", err)
	} else {
		if len(members) != 2 {
			t.Errorf("The members is not correct")
		}
	}

	count, err = client.SAdd(key, "four")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "five")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	pops, err := client.SPopN(key, 3)
	if err != nil {
		t.Errorf("Failed to get popn, the error is %#v", err)
	} else {
		if len(pops) != 3 {
			t.Errorf("The pops is not correct")
		}
	}

	client.Del(key)
}

func TestRedisClient_SPopN(t *testing.T) {
	client := getClient()
	key := "tk_spopn"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	count, err := client.SAdd(key, "one")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "two")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "four")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	pops, err := client.SPopN(key, 3)
	if err != nil {
		t.Errorf("Failed to get popn, the error is %#v", err)
	} else {
		if len(pops) != 3 {
			t.Errorf("The pops is not correct")
		}
	}

	client.Del(key)
}

func TestRedisClient_SRandMember(t *testing.T) {
	client := getClient()
	key := "tk_srandmember"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	count, err := client.SAdd(key, "one", "two", "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 3 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "four")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	randMember, err := client.SRandMember(key)
	if err != nil {
		t.Errorf("Failed to get rand member, the error is %#v", err)
	} else {
		if randMember == "" {
			t.Errorf("Failed to get rand member")
		} else {
			fmt.Printf("The rand member is %s", randMember)
		}
	}

	client.Del(key)
}

func TestRedisClient_SRandMemberN(t *testing.T) {
	client := getClient()
	key := "tk_srandmembern"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	count, err := client.SAdd(key, "one", "two", "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 3 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key, "four")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	randMembers, err := client.SRandMemberN(key, 2)
	if err != nil {
		t.Errorf("Failed to get rand members, the error is %#v", err)
	} else {
		if len(randMembers) != 2 {
			t.Errorf("The rand members length is not 2")
		} else {
			for _, member := range randMembers {
				fmt.Printf("The rand member is %s", member)
			}
		}
	}

	randMembers, err = client.SRandMemberN(key, -5)
	if err != nil {
		t.Errorf("Failed to get rand members, the error is %#v", err)
	} else {
		if len(randMembers) != 5 {
			t.Errorf("The rand members length is not 2")
		} else {
			for _, member := range randMembers {
				fmt.Printf("The rand member is %s", member)
			}
		}
	}

	client.Del(key)

}

func TestRedisClient_SRem(t *testing.T) {
	client := getClient()
	key := "tk_srem"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	count, err := client.SAdd(key, "one", "two", "three")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if count != 3 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SRem(key, "one")
	if err != nil {
		t.Errorf("Failed to remove item, the error is %#v", err)
	} else if count != 1 {
		fmt.Printf("The count is not 1")
	}

	count, err = client.SRem(key, "four")
	if err != nil {
		t.Errorf("Failed to remove item, the error is %#v", err)
	} else if count != 0 {
		fmt.Printf("The count is not 1")
	}

	members, err := client.SMembers(key)
	if err != nil {
		t.Errorf("Failed to get members, the error is %#v", err)
	} else {
		if len(members) != 2 {
			t.Errorf("The members is not correct")
		}
	}

	client.Del(key)
}

func TestRedisClient_SUnion(t *testing.T) {
	client := getClient()

	key1 := "tk_sunion_key1"
	key2 := "tk_sunion_key2"

	_, err := client.Del(key1)
	if err != nil {
		t.Errorf("Failed to delete key %s", key1)
		return
	}

	_, err = client.Del(key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key2)
		return
	}

	count, err := client.SAdd(key1, "a")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "b")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "d")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "e")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	unions, err := client.SUnion(key1, key2)
	if err != nil {
		t.Errorf("Failed to call sdiff, the error is %#v", err)
	}
	if len(unions) != 5 {
		t.Errorf("The length of inter is %d, which is not 5", len(unions))
	} else {
		if !isArraysEqual(unions, []string{"a", "b", "c", "d", "e"}) {
			t.Errorf("The result of union is not correct")
		}
	}

	client.Del(key1)
	client.Del(key2)
}

func TestRedisClient_SUnionStore(t *testing.T) {
	client := getClient()

	key := "tk_sunionstore"
	key1 := "tk_sunionstore_key1"
	key2 := "tk_sunionstore_key2"

	_, err := client.Del(key1)
	if err != nil {
		t.Errorf("Failed to delete key %s", key1)
		return
	}

	_, err = client.Del(key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key2)
		return
	}

	_, err = client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	count, err := client.SAdd(key1, "a")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "b")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key1, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "c")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "d")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SAdd(key2, "e")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if count != 1 {
		t.Errorf("The count is not correct")
	}

	count, err = client.SUnionStore(key, key1, key2)
	if err != nil {
		t.Errorf("Failed to call suninstore, the error is %#v", err)
	}
	if count != 5 {
		t.Errorf("The count is %d is not equal to 2", count)
	} else {
		members, err := client.SMembers(key)
		if err != nil {
			t.Errorf("Failed to get members, the error is %#v", err)
		} else {
			if !isArraysEqual(members, []string{"a", "b", "c", "d", "e"}) {
				t.Errorf("The result of union is not correct")
			}
		}
	}

	client.Del(key1)
	client.Del(key2)
	client.Del(key)
}

// ---------------------------Sorted Set---------------------------

func TestFVRedisClient_ZAdd(t *testing.T) {
	client := getClient()

	key := "tk_zadd"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{1, "uno"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	// The last one won't be counted, so the return value should be 2
	num, err = client.ZAdd(key, Z{2, "two"}, Z{3, "three"}, Z{4, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 2 {
		t.Errorf("Failed to call zadd")
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 4 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZAddCh(t *testing.T) {
	client := getClient()

	key := "tk_zaddch"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAddCh(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	// The score is not changed, so it won't be counted
	num, err = client.ZAddCh(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 0 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAddCh(key, Z{1, "uno"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	// The last one should be counted, because of ch parameter
	num, err = client.ZAddCh(key, Z{2, "two"}, Z{3, "three"}, Z{4, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 3 {
		t.Errorf("Failed to call zadd")
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 4 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZAddNX(t *testing.T) {
	client := getClient()

	key := "tk_zaddnx"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAddNX(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAddNX(key, Z{1, "uno"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	// The last one should be counted, because of ch parameter
	num, err = client.ZAddNX(key, Z{2, "two"}, Z{3, "three"}, Z{4, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 2 {
		t.Errorf("Failed to call zadd")
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 4 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZAddNXCh(t *testing.T) {
	client := getClient()

	key := "tk_zaddnxch"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAddNXCh(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAddNXCh(key, Z{1, "uno"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	// The last one should be counted, because of ch parameter
	num, err = client.ZAddNXCh(key, Z{2, "two"}, Z{3, "three"}, Z{4, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 2 {
		t.Errorf("Failed to call zadd")
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 4 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZAddXX(t *testing.T) {
	client := getClient()

	key := "tk_zaddnxch"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAddXX(key, Z{1, "uno"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 0 {
		t.Errorf("Failed to call zadd")
	}

	// The last one should be counted, because of ch parameter
	num, err = client.ZAddXX(key, Z{2, "two"}, Z{3, "three"}, Z{4, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 0 {
		t.Errorf("Failed to call zadd, the number is %d", num)
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 1 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZAddXXCh(t *testing.T) {
	client := getClient()

	key := "tk_zaddnxch"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAddXXCh(key, Z{1, "uno"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 0 {
		t.Errorf("Failed to call zadd")
	}

	// The last one should be counted, because of ch parameter
	num, err = client.ZAddXXCh(key, Z{2, "two"}, Z{3, "three"}, Z{4, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd, the number is %d", num)
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 1 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZCard(t *testing.T) {
	client := getClient()

	key := "tk_zcard"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	number, err := client.ZCard(key)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if number != 2 {
		t.Errorf("The number is not 2")
	}

	client.Del(key)
}

func TestFVRedisClient_ZCount(t *testing.T) {
	client := getClient()

	key := "tk_zcount"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	number, err := client.ZCount(key, ParamMinimum, ParamMaximum)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if number != 3 {
		t.Errorf("The number is not 3")
	}

	number, err = client.ZCount(key, "(1", "3")
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if number != 2 {
		t.Errorf("The number is not 2")
	}

	client.Del(key)
}

func TestFVRedisClient_ZIncr(t *testing.T) {
	client := getClient()

	key := "tk_zincr"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	result, err := client.ZIncr(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if result != 2 {
		t.Errorf("Failed to call zadd by incr")
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 1 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZIncrNX(t *testing.T) {
	client := getClient()

	key := "tk_zincrnx"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	result, err := client.ZIncrNX(key, Z{1, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if result != 1 {
		t.Errorf("Failed to call zadd by incr")
	}

	result, err = client.ZIncrNX(key, Z{1, "one"})
	if err != ErrNil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 2 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZIncrXX(t *testing.T) {
	client := getClient()

	key := "tk_zincrxx"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	result, err := client.ZIncrXX(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if result != 2 {
		t.Errorf("Failed to call zadd by incr, the result is %f", result)
	}

	result, err = client.ZIncrXX(key, Z{1, "two"})
	if err != ErrNil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 1 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZIncrBy(t *testing.T) {
	client := getClient()

	key := "tk_zincrby"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	number, err := client.ZIncrBy(key, 2, "one")
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if number != 3 {
		t.Errorf("The number is not 3")
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 2 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZInterStore(t *testing.T) {
	client := getClient()

	key := "tk_zinterstore"
	key1 := "tk_zinterstore1"
	key2 := "tk_zinterstore2"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	_, err = client.Del(key1)
	if err != nil {
		t.Errorf("Failed to delete key %s", key1)
		return
	}

	_, err = client.Del(key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key2)
		return
	}

	num, err := client.ZAdd(key1, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key1, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key2, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key2, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key2, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	store := ZStore{Weights: []float64{2, 3}}
	number, err := client.ZInterStore(key, store, key1, key2)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if number != 2 {
		t.Errorf("The number is not 2")
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 2 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
	client.Del(key1)
	client.Del(key2)
}

func TestFVRedisClient_ZLexCount(t *testing.T) {
	client := getClient()

	key := "tk_zlexcount"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(
		key,
		Z{0, "a"},
		Z{0, "b"},
		Z{0, "c"},
		Z{0, "d"},
		Z{0, "e"},
	)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 5 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(
		key,
		Z{0, "f"},
		Z{0, "g"},
	)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 2 {
		t.Errorf("Failed to call zadd")
	}

	number, err := client.ZLexCount(key, "-", "+")
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if number != 7 {
		t.Errorf("The number is not 3")
	}

	number, err = client.ZLexCount(key, "[b", "[f")
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if number != 5 {
		t.Errorf("The number is not 3")
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 7 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRange(t *testing.T) {
	client := getClient()

	key := "tk_zrange"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	list, err := client.ZRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"one", "two", "three"}) {
		t.Errorf("The members %#v is not correct", list)
	}

	list, err = client.ZRange(key, 2, 3)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"three"}) {
		t.Errorf("The members %#v is not correct", list)
	}

	list, err = client.ZRange(key, -2, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"two", "three"}) {
		t.Errorf("The members %#v is not correct", list)
	}

	members, err := client.ZRangeWithScores(key, 0, 1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 2 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRangeByLex(t *testing.T) {
	client := getClient()

	key := "tk_zlexcount"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(
		key,
		Z{0, "a"},
		Z{0, "b"},
		Z{0, "c"},
		Z{0, "d"},
		Z{0, "e"},
		Z{0, "f"},
		Z{0, "g"},
	)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 7 {
		t.Errorf("Failed to call zadd")
	}

	opt := ZRangeBy{Min: "-", Max: "[c"}
	list, err := client.ZRangeByLex(key, opt)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"a", "b", "c"}) {
		t.Errorf("The list %#v is not as expected", list)
	}

	opt = ZRangeBy{Min: "-", Max: "(c"}
	list, err = client.ZRangeByLex(key, opt)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"a", "b"}) {
		t.Errorf("The list %#v is not as expected", list)
	}

	opt = ZRangeBy{Min: "[aaa", Max: "(g"}
	list, err = client.ZRangeByLex(key, opt)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"b", "c", "d", "e", "f"}) {
		t.Errorf("The list %#v is not as expected", list)
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 7 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRangeByScore(t *testing.T) {
	client := getClient()

	key := "tk_zrangebyscore"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	opt := ZRangeBy{Min: ParamMinimum, Max: ParamMaximum}
	list, err := client.ZRangeByScore(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"one", "two", "three"}) {
		t.Errorf("The members %#v is not correct", list)
	}

	opt = ZRangeBy{Min: "1", Max: "2"}
	list, err = client.ZRangeByScore(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"one", "two"}) {
		t.Errorf("The members %#v is not correct", list)
	}

	opt = ZRangeBy{Min: "(1", Max: "2"}
	list, err = client.ZRangeByScore(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"two"}) {
		t.Errorf("The members %#v is not correct", list)
	}

	opt = ZRangeBy{Min: "(1", Max: "(2"}
	list, err = client.ZRangeByScore(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{}) {
		t.Errorf("The members %#v is not correct", list)
	}

	members, err := client.ZRangeWithScores(key, 0, 1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 2 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRangeByScoreWithScores(t *testing.T) {
	client := getClient()

	key := "tk_zrangebyscorewithscore"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	opt := ZRangeBy{Min: ParamMinimum, Max: ParamMaximum}
	members, err := client.ZRangeByScoreWithScores(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if len(members) != 3 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRangeWithScores(t *testing.T) {
	client := getClient()

	key := "tk_zrangebyscorewithscore"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	opt := ZRangeBy{Min: ParamMinimum, Max: ParamMaximum}
	members, err := client.ZRangeByScoreWithScores(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if len(members) != 3 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRank(t *testing.T) {
	client := getClient()

	key := "tk_zrank"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	rank, err := client.ZRank(key, "three")
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if rank != 2 {
		fmt.Printf("Failed to get rank")
	}

	rank, err = client.ZRank(key, "four")
	if err != ErrNil {
		t.Errorf("The err %#v is not ErrNil", err)
	}

	client.Del(key)
}

func TestFVRedisClient_ZRem(t *testing.T) {
	client := getClient()

	key := "tk_zrem"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZRem(key, "two")
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to rem item")
	}

	members, err := client.ZRangeWithScores(key, 0, 1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 2 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRemRangeByLex(t *testing.T) {
	client := getClient()

	key := "tk_zremrangebylex"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(
		key,
		Z{0, "aaaa"},
		Z{0, "b"},
		Z{0, "c"},
		Z{0, "d"},
		Z{0, "e"},
	)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 5 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(
		key,
		Z{0, "foo"},
		Z{0, "zap"},
		Z{0, "zip"},
		Z{0, "ALPHA"},
		Z{0, "alpha"},
	)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 5 {
		t.Errorf("Failed to call zadd")
	}

	list, err := client.ZRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"aaaa", "b", "c", "d", "e", "foo", "zap", "zip", "ALPHA", "alpha"}) {
		t.Errorf("The list %#v is not as expected", list)
	}

	num, err = client.ZRemRangeByLex(key, "[alpha", "[omega")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 6 {
		t.Errorf("Failed to call zadd")
	}

	list, err = client.ZRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"ALPHA", "aaaa", "zap", "zip"}) {
		t.Errorf("The list %#v is not as expected", list)
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 4 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRemRangeByRank(t *testing.T) {
	client := getClient()

	key := "tk_zremrangebyrank"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZRemRangeByRank(key, 0, 1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if num != 2 {
		t.Errorf("Failed to rem item")
	}

	members, err := client.ZRangeWithScores(key, 0, 1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 1 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRemRangeByScore(t *testing.T) {
	client := getClient()

	key := "tk_zremrangebyscore"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZRemRangeByScore(key, ParamMinimum, "(2")
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if num != 1 {
		t.Errorf("Failed to rem item")
	}

	members, err := client.ZRangeWithScores(key, 0, 1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 2 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRevRange(t *testing.T) {
	client := getClient()

	key := "tk_zrevrange"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	list, err := client.ZRevRange(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"one", "two", "three"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	list, err = client.ZRevRange(key, 2, 3)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"one"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	list, err = client.ZRevRange(key, -2, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqual(list, []string{"one", "two"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 3 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRevRangeByLex(t *testing.T) {
	client := getClient()

	key := "tk_zrevrangebylex"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(
		key,
		Z{0, "a"},
		Z{0, "b"},
		Z{0, "c"},
		Z{0, "d"},
		Z{0, "e"},
		Z{0, "f"},
		Z{0, "g"},
	)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 7 {
		t.Errorf("Failed to call zadd")
	}

	opt := ZRangeBy{Min: "-", Max: "[c"}
	list, err := client.ZRevRangeByLex(key, opt)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if !isArraysEqualWithSameOrder(list, []string{"c", "b", "a"}) {
		t.Errorf("The list %#v is not as expected", list)
	}

	opt = ZRangeBy{Min: "-", Max: "(c"}
	list, err = client.ZRevRangeByLex(key, opt)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if !isArraysEqualWithSameOrder(list, []string{"b", "a"}) {
		t.Errorf("The list %#v is not as expected", list)
	}

	opt = ZRangeBy{Min: "[aaa", Max: "(g"}
	list, err = client.ZRevRangeByLex(key, opt)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if !isArraysEqualWithSameOrder(list, []string{"f", "e", "d", "c", "b"}) {
		t.Errorf("The list %#v is not as expected", list)
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 7 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRevRangeByScore(t *testing.T) {
	client := getClient()

	key := "tk_zrevrangebyscore"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	opt := ZRangeBy{Min: ParamMinimum, Max: ParamMaximum}
	list, err := client.ZRevRangeByScore(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqualWithSameOrder(list, []string{"three", "two", "one"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	opt = ZRangeBy{Min: "1", Max: "2"}
	list, err = client.ZRevRangeByScore(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqualWithSameOrder(list, []string{"two", "one"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	opt = ZRangeBy{Min: "(1", Max: "2"}
	list, err = client.ZRevRangeByScore(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqualWithSameOrder(list, []string{"one"}) {
		t.Errorf("The list %#v is not correct", list)
	}

	opt = ZRangeBy{Min: "(1", Max: "(2"}
	list, err = client.ZRevRangeByScore(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if !isArraysEqualWithSameOrder(list, []string{""}) {
		t.Errorf("The list %#v is not correct", list)
	}

	client.Del(key)
}

func TestFVRedisClient_ZRevRangeByScoreWithScores(t *testing.T) {
	client := getClient()

	key := "tk_zrevrangebyscore"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	opt := ZRangeBy{Min: ParamMinimum, Max: ParamMaximum}
	members, err := client.ZRevRangeByScoreWithScores(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if len(members) != 3 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	opt = ZRangeBy{Min: "1", Max: "2"}
	members, err = client.ZRevRangeByScoreWithScores(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if len(members) != 2 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	opt = ZRangeBy{Min: "(1", Max: "2"}
	members, err = client.ZRevRangeByScoreWithScores(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if len(members) != 1 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	opt = ZRangeBy{Min: "(1", Max: "(2"}
	members, err = client.ZRevRangeByScoreWithScores(key, opt)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if len(members) != 0 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRevRangeWithScores(t *testing.T) {
	client := getClient()

	key := "tk_zrevrangewithscore"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	members, err := client.ZRevRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if len(members) != 3 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
}

func TestFVRedisClient_ZRevRank(t *testing.T) {
	client := getClient()

	key := "tk_zrevrank"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	rank, err := client.ZRevRank(key, "one")
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if rank != 2 {
		t.Errorf("Failed to get rank")
	}

	rank, err = client.ZRank(key, "four")
	if err != ErrNil {
		t.Errorf("The err %#v is not ErrNil", err)
	}

	client.Del(key)
}

func TestFVRedisClient_ZScore(t *testing.T) {
	client := getClient()

	key := "tk_zscore"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.ZAdd(key, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	score, err := client.ZScore(key, "one")
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	} else if score != 1 {
		t.Errorf("Failed to get score")
	}

	client.Del(key)
}

func TestFVRedisClient_ZUnionStore(t *testing.T) {
	client := getClient()

	key := "tk_zunionstore"
	key1 := "tk_zunionstore1"
	key2 := "tk_zunionstore2"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	_, err = client.Del(key1)
	if err != nil {
		t.Errorf("Failed to delete key %s", key1)
		return
	}

	_, err = client.Del(key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key2)
		return
	}

	num, err := client.ZAdd(key1, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key1, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key1, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key2, Z{1, "one"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key2, Z{2, "two"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	num, err = client.ZAdd(key2, Z{3, "three"})
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key2, err)
	}
	if num != 1 {
		t.Errorf("Failed to call zadd")
	}

	store := ZStore{Weights: []float64{2, 3}}
	number, err := client.ZUnionStore(key, store, key1, key2)
	if err != nil {
		t.Errorf("Failed to call zcard for key %s, the error is %#v", key, err)
	} else if number != 3 {
		t.Errorf("The number is not 2")
	}

	members, err := client.ZRangeWithScores(key, 0, -1)
	if err != nil {
		t.Errorf("Failed to del %s, the error is %#v", key, err)
	}
	if len(members) != 3 {
		t.Errorf("The member length is not correct, the length is %d and members is %#v", len(members), members)
	} else {
		for _, item := range members {
			fmt.Printf("The key is %s, value is %f", item.Member, item.Score)
		}
	}

	client.Del(key)
	client.Del(key1)
	client.Del(key2)
}

// ---------------------------String---------------------------

func TestRedisClient_Append(t *testing.T) {
	client := getClient()

	key := "tk_append"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.Exists(key)
	if err != nil {
		t.Errorf("Failed to check exists for key %s, the error is %#v", key, err)
	} else if num != 0 {
		t.Errorf("Failed to call exists")
	}

	num, err = client.Append(key, "Hello")
	if err != nil {
		t.Errorf("Failed to append %s, the error is %#v", key, err)
	} else if num != 5 {
		t.Errorf("Failed to call append")
	}

	num, err = client.Append(key, " World")
	if err != nil {
		t.Errorf("Failed to append %s, the error is %#v", key, err)
	} else if num != 11 {
		t.Errorf("Failed to call append")
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if value != "Hello World" {
		t.Errorf("Failed to call get")
	}

	client.Del(key)
}

func TestRedisClient_BitPos(t *testing.T) {
	client := getClient()

	key := "tk_bitpos"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.Set(key, "\xff\xf0\x00", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("Failed to call set")
	}

	pos, err := client.BitPos(key, 0)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if pos != 12 {
		t.Errorf("The result is incorrect")
	}

	status, err = client.Set(key, "\x00\xff\xf0", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("Failed to call set")
	}

	pos, err = client.BitPos(key, 1, 0)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if pos != 8 {
		t.Errorf("The result is incorrect")
	}

	pos, err = client.BitPos(key, 1, 2)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if pos != 16 {
		t.Errorf("The result is incorrect")
	}

	status, err = client.Set(key, "\x00\x00\x00", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("Failed to call set")
	}

	pos, err = client.BitPos(key, 1)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if pos != -1 {
		t.Errorf("The result %d is incorrect", pos)
	}

	client.Del(key)
}

func TestRedisClient_BitOpXor(t *testing.T) {

}

func TestRedisClient_BitOpOr(t *testing.T) {

}

func TestRedisClient_BitOpNot(t *testing.T) {

}

func TestRedisClient_BitOpAnd(t *testing.T) {

}

func TestRedisClient_BitCount(t *testing.T) {

}

func TestRedisClient_Decr(t *testing.T) {
	client := getClient()

	key := "tk_decr"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.Set(key, "10", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("Failed to call set")
	}

	num, err := client.Decr(key)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 9 {
		t.Errorf("The result is incorrect")
	}

	status, err = client.Set(key, "234293482390480948029348230948", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("Failed to call set")
	}

	num, err = client.Decr(key)
	if err != nil {
		if err.Error() != "ERR value is not an integer or out of range" {
			t.Errorf("Failed to test decr, the error is %#v", err)
		}
	} else {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_DecrBy(t *testing.T) {
	client := getClient()

	key := "tk_decrby"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.Set(key, "10", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("Failed to call set")
	}

	num, err := client.DecrBy(key, 3)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 7 {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_Get(t *testing.T) {
	client := getClient()

	key := "tk_get"
	key2 := "tk_get_nonexisting"

	_, err := client.Del(key, key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	_, err = client.Get(key2)
	if err != ErrNil {
		t.Errorf("The result is not correct, the err is not ErrNil")
	}

	status, err := client.Set(key, "Hello", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("Failed to call set")
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if value != "Hello" {
		t.Errorf("The result is incorrect")
	}

	client.Del(key, key2)
}

func TestRedisClient_GetBit(t *testing.T) {
	client := getClient()

	key := "tk_getbit"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	num, err := client.SetBit(key, 7, 1)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if num != 0 {
		t.Errorf("The result is incorrect")
	}

	num, err = client.GetBit(key, 0)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 0 {
		t.Errorf("The result is incorrect")
	}

	num, err = client.GetBit(key, 7)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 1 {
		t.Errorf("The result is incorrect")
	}

	num, err = client.GetBit(key, 100)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 0 {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_GetInt64(t *testing.T) {
	client := getClient()

	key := "tk_getint64"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.Set(key, 10, 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	num, err := client.GetInt64(key)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 10 {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_GetRange(t *testing.T) {
	client := getClient()

	key := "tk_getrange"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.Set(key, "This is a string", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	value, err := client.GetRange(key, 0, 3)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if value != "This" {
		t.Errorf("The result is incorrect")
	}

	value, err = client.GetRange(key, -3, -1)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if value != "ing" {
		t.Errorf("The result is incorrect")
	}

	value, err = client.GetRange(key, 0, -1)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if value != "This is a string" {
		t.Errorf("The result is incorrect")
	}

	value, err = client.GetRange(key, 10, 100)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if value != "string" {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_GetSet(t *testing.T) {
	client := getClient()

	key := "tk_getset"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.Set(key, "Hello", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	value, err := client.GetSet(key, "World")
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if value != "Hello" {
		t.Errorf("The result is incorrect")
	}

	value, err = client.Get(key)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if value != "World" {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_Incr(t *testing.T) {
	client := getClient()

	key := "tk_incr"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.Set(key, "10", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	num, err := client.Incr(key)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 11 {
		t.Errorf("The result is incorrect")
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if value != "11" {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_IncrBy(t *testing.T) {
	client := getClient()

	key := "tk_incrby"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.Set(key, "10", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	num, err := client.IncrBy(key, 5)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 15 {
		t.Errorf("The result is incorrect")
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if value != "15" {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_IncrByFloat(t *testing.T) {
	client := getClient()

	key := "tk_incrbyfloat"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.Set(key, 10.50, 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	num, err := client.IncrByFloat(key, 0.1)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 10.6 {
		t.Errorf("The result is incorrect")
	}

	num, err = client.IncrByFloat(key, -5)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 5.6 {
		t.Errorf("The result is incorrect")
	}

	status, err = client.Set(key, 5.0e3, 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	num, err = client.IncrByFloat(key, 2.0e2)
	if err != nil {
		t.Errorf("The error is %#v", err)
	} else if num != 5200 {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_MGet(t *testing.T) {
	client := getClient()

	key := "tk_mget1"
	key2 := "tk_mget2"
	key3 := "tk_nonexisting"

	_, err := client.Del(key, key2, key3)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.Set(key, "Hello", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	status, err = client.Set(key2, "World", 0)
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	list, err := client.MGet(key, key2, key3)
	if err != nil {
		t.Errorf("Failed to get list, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"Hello", "World", ""}) {
		t.Errorf("The list %#v is incorrect", list)
	}

	client.Del(key, key2, key3)
}

func TestRedisClient_MSet(t *testing.T) {
	client := getClient()

	key := "tk_mset"
	key2 := "tk_mset2"

	_, err := client.Del(key, key2)
	if err != nil {
		t.Errorf("Failed to delete key %s", key)
		return
	}

	status, err := client.MSet(key, "Hello", key2, "World")
	if err != nil {
		t.Errorf("Failed to set %s, the error is %#v", key, err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if value != "Hello" {
		t.Errorf("The result is incorrect")
	}

	value, err = client.Get(key2)
	if err != nil {
		t.Errorf("Failed to get %s, the error is %#v", key, err)
	} else if value != "World" {
		t.Errorf("The result is incorrect")
	}

	client.Del(key, key2)
}

func TestRedisClient_MSetNX(t *testing.T) {
	client := getClient()

	key1 := "tk_msetnx1"
	key2 := "tk_msetnx2"
	key3 := "tk_msetnx3"

	_, err := client.Del(key1, key2, key3)
	if err != nil {
		t.Errorf("Failed to delete keys")
		return
	}

	isSet, err := client.MSetNX(key1, "Hello", key2, "World")
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if !isSet {
		t.Errorf("The result is incorrect")
	}

	isSet, err = client.MSetNX(key2, "there", key3, "world")
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if isSet {
		t.Errorf("The result is incorrect")
	}

	list, err := client.MGet(key1, key2, key3)
	if err != nil {
		t.Errorf("Failed to get value, the error is %#v", err)
	} else if !isArraysEqualWithSameOrder(list, []string{"Hello", "World", ""}) {
		t.Errorf("The result is incorrect")
	}

	client.Del(key1, key2, key3)
}

func TestRedisClient_Set(t *testing.T) {
	client := getClient()

	key := "tk_set"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete keys")
		return
	}

	status, err := client.Set(key, "Hello", 0)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("Failed to get value, the error is %#v", err)
	} else if value != "Hello" {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_SetBit(t *testing.T) {
	client := getClient()

	key := "tk_setbit"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete keys")
		return
	}

	num, err := client.SetBit(key, 7, 1)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if num != 0 {
		t.Errorf("The result %d is incorrect", num)
	}

	num, err = client.SetBit(key, 7, 0)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if num != 1 {
		t.Errorf("The result %d is incorrect", num)
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("Failed to get value, the error is %#v", err)
	} else if value != "\u0000" {
		t.Errorf("The result %s is incorrect", value)
	}

	client.Del(key)
}

func TestRedisClient_SetInt64(t *testing.T) {
	client := getClient()

	key := "tk_setint64"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete keys")
		return
	}

	status, err := client.SetInt64(key, 10)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if status != "OK" {
		t.Errorf("The result %s is incorrect", status)
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("Failed to get value, the error is %#v", err)
	} else if value != "10" {
		t.Errorf("The result %s is incorrect", value)
	}

	client.Del(key)
}

func TestRedisClient_SetNX(t *testing.T) {
	client := getClient()

	key := "tk_setnx"

	_, err := client.Del(key)
	if err != nil {
		t.Errorf("Failed to delete keys")
		return
	}

	isSet, err := client.SetNX(key, "Hello", 0)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if !isSet {
		t.Errorf("The result is incorrect")
	}

	isSet, err = client.SetNX(key, "World", 0)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if isSet {
		t.Errorf("The result is incorrect")
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("Failed to get value, the error is %#v", err)
	} else if value != "Hello" {
		t.Errorf("The result is incorrect")
	}

	client.Del(key)
}

func TestRedisClient_SetRange(t *testing.T) {
	client := getClient()

	key := "tk_setrange"
	key2 := "tk_setrange2"

	_, err := client.Del(key, key2)
	if err != nil {
		t.Errorf("Failed to delete keys")
		return
	}

	status, err := client.Set(key, "Hello", 0)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	num, err := client.SetRange(key, 6, "Redis")
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if num != 11 {
		t.Errorf("The result is incorrect")
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("Failed to get value, the error is %#v", err)
	} else if value != "Hello\u0000Redis" {
		t.Errorf("The result %s is incorrect", value)
	}

	num, err = client.SetRange(key2, 6, "Redis")
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if num != 11 {
		t.Errorf("The result is incorrect")
	}

	value, err = client.Get(key2)
	if err != nil {
		t.Errorf("Failed to get value, the error is %#v", err)
	} else if value != "\u0000\u0000\u0000\u0000\u0000\u0000Redis" {
		t.Errorf("The result %s is incorrect", value)
	}

	client.Del(key, key2)
}

func TestRedisClient_SetXX(t *testing.T) {
	client := getClient()

	key := "tk_setxx"
	key2 := "tk_setxx2"

	_, err := client.Del(key, key2)
	if err != nil {
		t.Errorf("Failed to delete keys")
		return
	}

	status, err := client.Set(key, "Hello", 0)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	status, err = client.SetXX(key, "World", 0)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	_, err = client.SetXX(key2, "World", 0)
	if err != ErrNil {
		t.Errorf("Failed to set value, the error is %#v", err)
	}

	value, err := client.Get(key)
	if err != nil {
		t.Errorf("Failed to get value, the error is %#v", err)
	} else if value != "World" {
		t.Errorf("The result is incorrect")
	}

	client.Del(key, key2)
}

func TestRedisClient_StrLen(t *testing.T) {
	client := getClient()

	key := "tk_setxx"
	key2 := "tk_setxx2"

	_, err := client.Del(key, key2)
	if err != nil {
		t.Errorf("Failed to delete keys")
		return
	}

	status, err := client.Set(key, "Hello world", 0)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if status != "OK" {
		t.Errorf("The result is incorrect")
	}

	num, err := client.StrLen(key)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if num != 11 {
		t.Errorf("The result is incorrect")
	}

	num, err = client.StrLen(key2)
	if err != nil {
		t.Errorf("Failed to set value, the error is %#v", err)
	} else if num != 0 {
		t.Errorf("The result is incorrect")
	}

	client.Del(key, key2)
}

//redis> SET mykey "Hello"
//"OK"
//redis> EXPIRE mykey 10
//(integer) 1
//redis> TTL mykey
//(integer) 10
//redis>
func TestRedisClient_TTL(t *testing.T) {
	client := getClient()
	key := "tk_ttlkey"
	result, err := client.Set(key, "Hello", 0)
	if err != nil {
		t.Errorf("Set test key failed, the error is %#v", err)
	}
	if result != "OK" {
		t.Errorf("Invalid set result %s, expected OK", result)
	} else {
		er, err := client.Expire(key, 10*time.Second)
		if err != nil {
			t.Errorf("Failed to set expiration, the error is %#v", err)
		}
		if er != 1 {
			t.Errorf("Invalid result for set expiration, the result is not 1")
		} else {
			ttl, err := client.TTL(key)
			if err != nil {
				t.Errorf("Failed to get ttl, the error is %#v", err)
				return
			}
			if ttl < 0 || ttl > 10 {
				t.Errorf("Invalid ttl %d", ttl)
			}
		}
	}
	client.Del(key)
}

func TestRedisClient_Scan(t *testing.T) {
	client := getClient()
	var (
		cursor uint64 = 0
		list   []string
		err    error
	)
	for {
		cursor, list, err = client.Scan(cursor, "*", 0)
		if err != nil {
			//logs.Errorf("Failed to scan from redis, the error is %#v", err)
			assert.Fail(t, "Scan failed")
			return
		}
		if cursor == 0 {
			break
		}
		fmt.Printf("The cursor is %d", cursor)
		for _, key := range list {
			//logs.Debugf("The item is %s", key)
			fmt.Printf("The key is %s", key)
		}
	}
}

func TestRedisClient_Type(t *testing.T) {
	client := getClient()
	testings := []struct {
		key    string
		result string
		err    error
	}{{"system.sh.610", "hash", nil}}
	for _, test := range testings {
		result, err := client.Type(test.key)
		assert.Equal(t, test.result, result)
		assert.Equal(t, test.err, err)
	}
}
