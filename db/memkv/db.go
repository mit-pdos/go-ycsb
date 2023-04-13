package memkv

import (
	"context"
	"strconv"

	kv "github.com/mit-pdos/gokv/memkv"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/magiconair/properties"
	// "github.com/pingcap/go-ycsb/pkg/prop"
	// "github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/mit-pdos/gokv/connman"
)

type kvDB struct {
	cl *kv.KVClerk
}

func (g *kvDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {

	var res []byte
	if len(fields) != 1 {
		panic("gokv: read must have a single field")
	}
	k, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		panic(err)
	}
	res = g.cl.Get(k)

	return (map[string][]byte{ key: []byte(res) }), nil
}

func (g *kvDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	panic("gokv: should always use update instead of insert")
}

func (g *kvDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	if len(values) != 1 {
		panic("gokv: update must have a single value")
	}

	panic("This should use the table value, or else redis should stop using it")
	var data []byte
	for _, v := range values {
		data = v
	}

	// XXX: the key must be an integer formatted as a string; it's a bit silly that a
	// uint64 gets converted to a string by the workload generator, then back to
	// a uint64 here, but changing that would require changing the DB interface.
	k, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		panic(err)
	}

	g.cl.Put(k, data)
	return nil
}

func (_ *kvDB) Delete(ctx context.Context, table string, key string) error {
	panic("gokv: delete unimplemented ")
}

func (_ *kvDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	panic("gokv: scan not supported")
}

func (_ *kvDB) Close() error {
	return nil
}

func (_ *kvDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (_ *kvDB) CleanupThread(_ context.Context) {
}

type kvCreator struct{}

func (r kvCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	// cl := kv.MakeKVClerkPool(1, uint64(p.GetInt(memkvNumClients, 100)))
	cl := kv.MakeKVClerk(grove_ffi.MakeAddress(p.GetString(memkvCoord, "")), connman.MakeConnMan())
	return &kvDB{cl}, nil
}

func init() {
	ycsb.RegisterDBCreator("memkv", kvCreator{})
}

const (
	memkvNumClients = "memkv.clients"
	memkvCoord = "memkv.coord"
)
