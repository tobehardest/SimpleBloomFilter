package BloomFilter

import (
	"context"
	"fmt"
	"github.com/demdxx/gocast"
)

// 布隆过滤器服务
type BloomService struct {
	m, k      int32
	encryptor *Encryptor
	client    *RedisClient
}

// m -> bitmap 的长度；k -> hash 函数的个数；
// client -> redis 客户端；encryptor -> hash 映射器
func NewBloomService(m, k int32, client *RedisClient, encrytor *Encryptor) *BloomService {
	return &BloomService{
		m:         m,
		k:         k,
		client:    client,
		encryptor: encrytor,
	}
}

// key -> 布隆过滤器 bitmap 对应的 key   val -> 基于 hash 映射到 bitmap 中的值
func (b *BloomService) Exist(ctx context.Context, key, val string) (bool, error) {
	// 映射对应的 bit 位
	keyAndArgs := make([]interface{}, 0, b.k+2)
	keyAndArgs = append(keyAndArgs, key, b.k)
	for _, encrypted := range b.getKEncrypted(val) {
		keyAndArgs = append(keyAndArgs, encrypted)
	}

	rawResp, err := b.client.Eval(ctx, LuaBloomBatchGetBits, 1, keyAndArgs)
	if err != nil {
		return false, err
	}

	resp := gocast.ToInt(rawResp)
	if resp == 1 {
		return true, nil
	}
	return false, nil
}

func (b *BloomService) getKEncrypted(val string) []int32 {
	encrypteds := make([]int32, 0, b.k)
	origin := val
	for i := 0; int32(i) < b.k; i++ {
		encrypted := b.encryptor.Encrypt(origin)
		encrypteds = append(encrypteds, encrypted)
		if int32(i) == b.k-1 {
			break
		}
		origin = gocast.ToString(encrypted)
	}
	return encrypteds
}

const LuaBloomBatchGetBits = `
  local bloomKey = KEYS[1]
  local bitsCnt = ARGV[1]
  for i=1,bitsCnt,1 do
    local offset = ARGV[1+i]
    local reply = redis.call('getbit',bloomKey,offset)
    if (not reply) then
        error('FAIL')
        return 0
    end
    if (reply == 0) then
        return 0
    end
  end
  return 1
`

func (b *BloomService) Set(ctx context.Context, key, val string) error {
	// 映射对应的 bit 位
	keyAndArgs := make([]interface{}, 0, b.k+2)
	keyAndArgs = append(keyAndArgs, key, b.k)
	for _, encrypted := range b.getKEncrypted(val) {
		keyAndArgs = append(keyAndArgs, encrypted)
	}

	rawResp, err := b.client.Eval(ctx, LuaBloomBatchSetBits, 1, keyAndArgs)
	if err != nil {
		return err
	}

	resp := gocast.ToInt(rawResp)
	if resp != 1 {
		return fmt.Errorf("resp: %d", resp)
	}
	return nil
}

const LuaBloomBatchSetBits = `
  local bloomKey = KEYS[1]
  local bitsCnt = ARGV[1]


  for i=1,bitsCnt,1 do
    local offset = ARGV[1+i]
    redis.call('setbit',bloomKey,offset,1)
  end
  return 1
`
