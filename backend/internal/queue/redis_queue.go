package queue

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	keyPrefix     = "taskforge"
	keyReady      = "taskforge:ready:"          // + priority, ZSET score=seq / -seq(head)
	keyHead       = "taskforge:head"            // HASH taskID -> 1
	keySeq        = "taskforge:seq"             // STRING monotonic enqueue sequence
	keyDelay      = "taskforge:delay"           // ZSET taskID -> runAt unix nano
	keyDelayPri   = "taskforge:delay:pri"       // HASH taskID -> priority
	keyInflExp    = "taskforge:inflight:exp"    // ZSET taskID -> lease expires unix nano
	keyInflWorker = "taskforge:inflight:worker" // HASH taskID -> workerID
	keyInflPri    = "taskforge:inflight:pri"    // HASH taskID -> priority
)

// RedisQueue is the production Queue backed by Redis (ZSETs + hashes, all
// pop/move operations are atomic Lua scripts).
type RedisQueue struct {
	rdb *redis.Client
}

func NewRedis(addr, password string, db int) *RedisQueue {
	return &RedisQueue{rdb: redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})}
}

func (q *RedisQueue) Ping(ctx context.Context) error { return q.rdb.Ping(ctx).Err() }

func (q *RedisQueue) Reset(ctx context.Context) error {
	var cursor uint64
	for {
		keys, next, err := q.rdb.Scan(ctx, cursor, keyPrefix+":*", 200).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := q.rdb.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// readyPop pops the earliest due task. Keys: readyKey, headHash, seqKey.
// Normal tasks score = positive seq; head tasks score = -seq, so heads
// always sort first.
var readyPopScript = redis.NewScript(`
local ready = KEYS[1]
local head = KEYS[2]
local entries = redis.call('ZRANGE', ready, 0, 0)
if #entries == 0 then return false end
local id = entries[1]
redis.call('ZREMRANGEBYRANK', ready, 0, 0)
local isHead = redis.call('HDEL', head, id)
return {id, isHead}
`)

func (q *RedisQueue) EnqueueReady(ctx context.Context, taskID string, priority int, head bool) error {
	seq, err := q.rdb.Incr(ctx, keySeq).Result()
	if err != nil {
		return err
	}
	score := float64(seq)
	if head {
		score = -float64(seq)
		if err := q.rdb.HSet(ctx, keyHead, taskID, 1).Err(); err != nil {
			return err
		}
	}
	return q.rdb.ZAdd(ctx, keyReady+strconv.Itoa(priority), redis.Z{
		Score:  score,
		Member: taskID,
	}).Err()
}

func (q *RedisQueue) ReadyDepth(ctx context.Context) (map[int]int64, error) {
	out := make(map[int]int64, 5)
	for p := 0; p < 5; p++ {
		n, err := q.rdb.ZCard(ctx, keyReady+strconv.Itoa(p)).Result()
		if err != nil {
			return nil, err
		}
		out[p] = n
	}
	return out, nil
}

// ClaimOne pops from priority order 0..4, skipping 0/1 when forceLow.
func (q *RedisQueue) ClaimOne(ctx context.Context, forceLow bool, now time.Time) (*Claim, error) {
	start := 0
	if forceLow {
		start = 2
	}
	for p := start; p < 5; p++ {
		res, err := readyPopScript.Run(ctx, q.rdb,
			[]string{keyReady + strconv.Itoa(p), keyHead}).Slice()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return nil, err
		}
		if len(res) < 2 {
			continue
		}
		// Lua returns false for an empty set; go-redis gives nil.
		id, _ := res[0].(string)
		if id == "" {
			continue
		}
		headFlag, _ := res[1].(int64)
		return &Claim{TaskID: id, Priority: p, Head: headFlag == 1}, nil
	}
	return nil, nil
}

func (q *RedisQueue) AddInflight(ctx context.Context, workerID, taskID string, priority int, leaseExpiresAt time.Time) error {
	pipe := q.rdb.TxPipeline()
	pipe.ZAdd(ctx, keyInflExp, redis.Z{Score: float64(leaseExpiresAt.UnixNano()), Member: taskID})
	pipe.HSet(ctx, keyInflWorker, taskID, workerID)
	pipe.HSet(ctx, keyInflPri, taskID, strconv.Itoa(priority))
	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) RenewInflight(ctx context.Context, workerID, taskID string, leaseExpiresAt time.Time) (bool, error) {
	exists, err := q.rdb.ZScore(ctx, keyInflExp, taskID).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_ = exists
	if err := q.rdb.ZAdd(ctx, keyInflExp, redis.Z{Score: float64(leaseExpiresAt.UnixNano()), Member: taskID}).Err(); err != nil {
		return false, err
	}
	return true, nil
}

func (q *RedisQueue) RemoveInflight(ctx context.Context, workerID, taskID string) error {
	pipe := q.rdb.TxPipeline()
	pipe.ZRem(ctx, keyInflExp, taskID)
	pipe.HDel(ctx, keyInflWorker, taskID)
	pipe.HDel(ctx, keyInflPri, taskID)
	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) inflightBatch(ctx context.Context, ids []string) []Inflight {
	if len(ids) == 0 {
		return nil
	}
	workers, _ := q.rdb.HMGet(ctx, keyInflWorker, ids...).Result()
	pris, _ := q.rdb.HMGet(ctx, keyInflPri, ids...).Result()
	out := make([]Inflight, 0, len(ids))
	for i, id := range ids {
		in := Inflight{TaskID: id}
		if w, ok := workers[i].(string); ok {
			in.WorkerID = w
		}
		if p, ok := pris[i].(string); ok {
			in.Priority, _ = strconv.Atoi(p)
		}
		out = append(out, in)
	}
	return out
}

func (q *RedisQueue) ExpiredInflights(ctx context.Context, now time.Time) ([]Inflight, error) {
	ids, err := q.rdb.ZRangeByScore(ctx, keyInflExp, &redis.ZRangeBy{
		Min: "0", Max: strconv.FormatInt(now.UnixNano(), 10),
	}).Result()
	if err != nil {
		return nil, err
	}
	return q.inflightBatch(ctx, ids), nil
}

func (q *RedisQueue) AllInflights(ctx context.Context) ([]Inflight, error) {
	ids, err := q.rdb.ZRange(ctx, keyInflExp, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return q.inflightBatch(ctx, ids), nil
}

var requeueInflightScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], KEYS[4])
redis.call('HDEL', KEYS[2], KEYS[4])
redis.call('HDEL', KEYS[3], KEYS[4])
local seq = redis.call('INCR', KEYS[6])
redis.call('ZADD', KEYS[5], -seq, KEYS[4])
redis.call('HSET', KEYS[7], KEYS[4], 1)
`)

// RequeueInflight removes the lease and inserts the task at the head of
// its ready queue (preempted/recovered tasks go first).
func (q *RedisQueue) RequeueInflight(ctx context.Context, workerID, taskID string, priority int) error {
	// persist priority for inflight if not present
	_ = q.rdb.HSet(ctx, keyInflPri, taskID, strconv.Itoa(priority)).Err()
	return requeueInflightScript.Run(ctx, q.rdb, []string{
		keyInflExp, keyInflWorker, keyInflPri, taskID,
		keyReady + strconv.Itoa(priority), keySeq, keyHead,
	}).Err()
}

func (q *RedisQueue) AddDelay(ctx context.Context, taskID string, priority int, runAt time.Time) error {
	pipe := q.rdb.TxPipeline()
	pipe.ZAdd(ctx, keyDelay, redis.Z{Score: float64(runAt.UnixNano()), Member: taskID})
	pipe.HSet(ctx, keyDelayPri, taskID, strconv.Itoa(priority))
	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) RemoveDelay(ctx context.Context, taskID string) error {
	pipe := q.rdb.TxPipeline()
	pipe.ZRem(ctx, keyDelay, taskID)
	pipe.HDel(ctx, keyDelayPri, taskID)
	_, err := pipe.Exec(ctx)
	return err
}

var popDueScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, 200)
for _, id in ipairs(ids) do
	redis.call('ZREM', KEYS[1], id)
end
return ids
`)

// PopDue returns and removes at most 200 due delayed tasks; priority is
// read from the auxiliary hash.
func (q *RedisQueue) PopDue(ctx context.Context, now time.Time) ([]Claim, error) {
	ids, err := popDueScript.Run(ctx, q.rdb, []string{keyDelay}, now.UnixNano()).StringSlice()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]Claim, 0, len(ids))
	for _, id := range ids {
		pri, _ := q.rdb.HGet(ctx, keyDelayPri, id).Result()
		q.rdb.HDel(ctx, keyDelayPri, id)
		p, _ := strconv.Atoi(pri)
		out = append(out, Claim{TaskID: id, Priority: p})
	}
	return out, nil
}

func (q *RedisQueue) DelaySize(ctx context.Context) (int64, error) {
	return q.rdb.ZCard(ctx, keyDelay).Result()
}
