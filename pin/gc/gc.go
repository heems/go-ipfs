package gc

import (
	bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	key "github.com/ipfs/go-ipfs/blocks/key"
	bserv "github.com/ipfs/go-ipfs/blockservice"
	offline "github.com/ipfs/go-ipfs/exchange/offline"
	dag "github.com/ipfs/go-ipfs/merkledag"
	pin "github.com/ipfs/go-ipfs/pin"

	context "github.com/ipfs/go-ipfs/Godeps/_workspace/src/golang.org/x/net/context"
	eventlog "github.com/ipfs/go-ipfs/thirdparty/eventlog"
)

var log = eventlog.Logger("gc")

type GCSet struct {
	keys map[key.Key]struct{}
}

func NewGCSet() *GCSet {
	return &GCSet{make(map[key.Key]struct{})}
}

func (gcs *GCSet) Add(k key.Key) {
	gcs.keys[k] = struct{}{}
}

func (gcs *GCSet) Has(k key.Key) bool {
	_, has := gcs.keys[k]
	return has
}

func (gcs *GCSet) AddDag(ds dag.DAGService, root key.Key) error {
	ctx := context.Background()
	nd, err := ds.Get(ctx, root)
	if err != nil {
		return err
	}

	gcs.Add(root)

	for _, lnk := range nd.Links {
		k := key.Key(lnk.Hash)
		err := gcs.AddDag(ds, k)
		if err != nil {
			return err
		}
	}
	return nil
}

func GC(ctx context.Context, bs bstore.Blockstore, pn pin.Pinner) (<-chan key.Key, error) {
	bsrv, err := bserv.New(bs, offline.Exchange(bs))
	if err != nil {
		return nil, err
	}
	ds := dag.NewDAGService(bsrv)

	gcs := NewGCSet()
	for _, k := range pn.RecursiveKeys() {
		gcs.AddDag(ds, k)
	}
	for _, k := range pn.DirectKeys() {
		gcs.Add(k)
	}

	keychan, err := bs.AllKeysChan(ctx)
	if err != nil {
		return nil, err
	}

	output := make(chan key.Key)
	go func() {
		defer close(output)
		for {
			select {
			case k, ok := <-keychan:
				if !ok {
					return
				}
				if !gcs.Has(k) {
					err := bs.DeleteBlock(k)
					if err != nil {
						log.Debugf("Error removing key from blockstore: %s", err)
						return
					}
					select {
					case output <- k:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return output, nil
}
