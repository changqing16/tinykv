package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	dbPath string
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	if conf == nil {
		return nil
	}
	return &StandAloneStorage{dbPath: conf.DBPath}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	options := badger.DefaultOptions
	options.Dir = s.dbPath
	options.ValueDir = s.dbPath
	db, err := badger.Open(options)
	if err == nil {
		s.db = db
	}

	return err
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.db == nil {
		return nil, errors.New("db is nil")
	}
	txn := s.db.NewTransaction(false)
	return NewReaderImpl(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if s.db == nil {
		return errors.New("db is nil")
	}
	return s.db.Update(func(txn *badger.Txn) error {
		for _, item := range batch {
			var err error
			switch item.Data.(type) {
			case storage.Put:
				err = txn.Set(engine_util.KeyWithCF(item.Cf(), item.Key()), item.Value())
			case storage.Delete:
				err = txn.Delete(engine_util.KeyWithCF(item.Cf(), item.Key()))
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type readerImpl struct {
	txn *badger.Txn
}

func NewReaderImpl(txn *badger.Txn) storage.StorageReader {
	return &readerImpl{txn: txn}
}

func (r *readerImpl) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = nil
	}
	return value, err
}

func (r *readerImpl) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *readerImpl) Close() {
	r.txn.Discard()
}
