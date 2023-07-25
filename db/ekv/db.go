package ekv

import (
	"context"

	"github.com/magiconair/properties"
	"github.com/mit-pdos/gokv/grove_ffi"
	"github.com/mit-pdos/gokv/simplepb/apps/kvee"

	// "github.com/pingcap/go-ycsb/pkg/prop"
	// "github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type kvDB struct {
	cl *kvee.Clerk
}

func (g *kvDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var res []byte
	if len(fields) != 1 {
		panic("gokv: read must have a single field")
	}
	res = g.cl.Get([]byte(table + "/" + key))

	return (map[string][]byte{ key: []byte(res) }), nil
}

func (g *kvDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	if len(values) != 1 {
		panic("pb_kv: insert must have a single value")
	}

	var data []byte
	for _, v := range values {
		data = v
	}

	g.cl.Put([]byte(table + "/" + key), data)
	return nil
}

func (g *kvDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	if len(values) != 1 {
		panic("pb_kv: update must have a single value")
	}

	var data []byte
	for _, v := range values {
		data = v
	}

	// log.Println("KV put with ", key, data)
	g.cl.Put([]byte(table + "/" + key), data)
	return nil
}

func (_ *kvDB) Delete(ctx context.Context, table string, key string) error {
	panic("pbkv: delete unimplemented ")
}

func (_ *kvDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	panic("pbkv: scan not supported")
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
	fmt.Printf("Create")
	configAddr := grove_ffi.MakeAddress(p.GetString(pbkvConfig, ""))
	cl := kv.MakeClerkPool(configAddr)
	return &kvDB{cl}, nil
}

func init() {
	ycsb.RegisterDBCreator("pbkv", kvCreator{})
}

const (
	pbkvConfig = "pbkv.configAddr"
)
