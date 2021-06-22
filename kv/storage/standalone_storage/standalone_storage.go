package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	Kv     *badger.DB
	KvPath string
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (reader *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}
func (reader *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}
func (reader *StandAloneReader) Close() {
	reader.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	KvPath := conf.DBPath
	return &StandAloneStorage{
		KvPath: KvPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.KvPath
	opts.ValueDir = opts.Dir
	if err := os.MkdirAll(s.KvPath, os.ModePerm); err != nil {
		return err
	}
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.Kv = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.Kv.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneReader{
		txn: s.Kv.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)

	for _, item := range batch {
		switch item.Data.(type) {
		case storage.Put:
			wb.SetCF(item.Cf(), item.Key(), item.Value())
		case storage.Delete:
			wb.DeleteCF(item.Cf(), item.Key())
		}
	}
	return wb.WriteToDB(s.Kv)
}
