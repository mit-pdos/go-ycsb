package gokv

import (
	"context"
	"strconv"

	gokv "github.com/mit-pdos/gokv/fastkv"
	"github.com/magiconair/properties"
	// "github.com/pingcap/go-ycsb/pkg/prop"
	// "github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type gokvDB struct {
	cl *gokv.GooseKVClerkPool
}

func (g *gokvDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {

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

func (g *gokvDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	panic("gokv: should always use update instead of insert")
}

func (g *gokvDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	if len(values) != 1 {
		panic("gokv: update must have a single value")
	}

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

func (_ *gokvDB) Delete(ctx context.Context, table string, key string) error {
	panic("gokv: delete unimplemented ")
}

func (_ *gokvDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	panic("gokv: scan not supported")
}

func (_ *gokvDB) Close() error {
	return nil
}

func (_ *gokvDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (_ *gokvDB) CleanupThread(_ context.Context) {
}

type gokvCreator struct{}

func (r gokvCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	cl := gokv.MakeGooseKVClerkPool(1, uint64(p.GetInt(gokvNumClients, 100)))
	return &gokvDB{cl}, nil
}

func init() {
	ycsb.RegisterDBCreator("gokv", gokvCreator{})
}

const (
	gokvNumClients = "gokv.clients"
)
