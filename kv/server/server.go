package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (resp *kvrpcpb.GetResponse, err error) {
	// Your Code Here (4B).
	resp = new(kvrpcpb.GetResponse)
	defer func() {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			err = nil
		}
	}()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.GetVersion())

	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}
	if lock != nil && lock.Ts < req.GetVersion() {
		resp.Error = &kvrpcpb.KeyError{Locked: lock.Info(req.Key)}
		return resp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return resp, err
	}
	if len(value) == 0 {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (resp *kvrpcpb.PrewriteResponse, err error) {
	// Your Code Here (4B).
	resp = new(kvrpcpb.PrewriteResponse)
	defer func() {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			err = nil
		}
	}()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	var keyErrors []*kvrpcpb.KeyError
	for _, operation := range req.Mutations {
		write, commitTS, err := txn.MostRecentWrite(operation.Key)
		if err != nil {
			return resp, err
		}

		if write != nil && commitTS >= req.StartVersion {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTS,
					Key:        operation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		lock, err := txn.GetLock(operation.Key)
		if err != nil {
			return resp, err
		}
		if lock != nil {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: req.PrimaryLock,
					LockVersion: lock.Ts,
					Key:         operation.Key,
					LockTtl:     lock.Ttl,
				},
			})
			continue
		}

		kind := mvcc.WriteKindFromProto(operation.Op)
		switch kind {
		case mvcc.WriteKindPut:
			txn.PutValue(operation.Key, operation.Value)
		case mvcc.WriteKindDelete:
			txn.DeleteValue(operation.Key)
		case mvcc.WriteKindRollback:
			return nil, nil
		}

		txn.PutLock(operation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})
	}

	if len(keyErrors) > 0 {
		resp.Errors = keyErrors
		return resp, nil
	}

	err = server.storage.Write(req.Context, txn.Writes())
	return resp, err
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (resp *kvrpcpb.CommitResponse, err error) {
	// Your Code Here (4B).
	resp = new(kvrpcpb.CommitResponse)
	if len(req.Keys) == 0 {
		return resp, nil
	}

	defer func() {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			err = nil
		}
	}()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	resp.Error, err = server.kvCommit(txn, req.Keys, req.StartVersion, req.CommitVersion)
	if resp.Error != nil || err != nil {
		return resp, err
	}

	err = server.storage.Write(req.Context, txn.Writes())
	return resp, err
}

func (server *Server) kvCommit(txn *mvcc.MvccTxn, keys [][]byte, startVersion, commitVersion uint64) (*kvrpcpb.KeyError, error) {
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				return &kvrpcpb.KeyError{Abort: "Rollbacked"}, nil
			} else {
				return nil, nil
			}
		} else {
			if lock.Ts != startVersion {
				return &kvrpcpb.KeyError{Retryable: "true"}, nil
			}
			txn.PutWrite(key, commitVersion, &mvcc.Write{
				StartTS: startVersion,
				Kind:    lock.Kind,
			})
			txn.DeleteLock(key)
		}
	}
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (resp *kvrpcpb.ScanResponse, err error) {
	// Your Code Here (4C).
	resp = new(kvrpcpb.ScanResponse)
	resp.Pairs = make([]*kvrpcpb.KvPair, 0)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()
	for i := uint32(0); i < req.Limit; i++ {
		key, value, err := scanner.Next()
		if key == nil && value == nil && err == nil {
			return resp, nil
		}
		kvPair := &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		}
		if err != nil {
			kvPair.Error = &kvrpcpb.KeyError{
				Retryable: err.Error(),
			}
		}
		resp.Pairs = append(resp.Pairs, kvPair)
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (resp *kvrpcpb.CheckTxnStatusResponse, err error) {
	// Your Code Here (4C).
	resp = new(kvrpcpb.CheckTxnStatusResponse)
	defer func() {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			err = nil
		}
	}()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	write, commitTS, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return resp, err
	}

	if commitTS != 0 {
		if write.Kind == mvcc.WriteKindRollback {
			return resp, nil
		}
		resp.CommitVersion = commitTS
		return resp, nil
	}

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	if lock != nil && lock.Ts == req.LockTs {
		if mvcc.PhysicalTime(req.CurrentTs)-mvcc.PhysicalTime(req.LockTs) < lock.Ttl {
			resp.LockTtl = lock.Ttl
			return resp, nil
		} else {
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
		}
	} else {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	}

	txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
		StartTS: req.LockTs,
		Kind:    mvcc.WriteKindRollback,
	})
	err = server.storage.Write(req.Context, txn.Writes())
	return resp, err
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (resp *kvrpcpb.BatchRollbackResponse, err error) {
	// Your Code Here (4C).
	resp = new(kvrpcpb.BatchRollbackResponse)
	if len(req.Keys) == 0 {
		return resp, nil
	}

	defer func() {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			err = nil
		}
	}()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	resp.Error, err = server.kvBatchRollback(txn, req.Keys, req.StartVersion)
	if resp.Error != nil || err != nil {
		return resp, err
	}

	err = server.storage.Write(req.Context, txn.Writes())
	return resp, err
}

func (server *Server) kvBatchRollback(txn *mvcc.MvccTxn, keys [][]byte, startVersion uint64) (*kvrpcpb.KeyError, error) {
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	for _, key := range keys {
		write, commitTS, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if commitTS != 0 {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			}
			return &kvrpcpb.KeyError{
				Abort: "already commited",
			}, nil
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == startVersion {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}

		txn.PutWrite(key, startVersion, &mvcc.Write{
			StartTS: startVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (resp *kvrpcpb.ResolveLockResponse, err error) {
	// Your Code Here (4C).
	resp = new(kvrpcpb.ResolveLockResponse)

	defer func() {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			err = nil
		}
	}()
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	keys := make([][]byte, 0)
	iter := txn.Reader.IterCF(engine_util.CfLock)
	for ; iter.Valid(); iter.Next() {
		dataKey := iter.Item().KeyCopy(nil)
		lockData, err := iter.Item().ValueCopy(nil)
		if err != nil {
			break
		}
		lock, err := mvcc.ParseLock(lockData)
		if err != nil {
			break
		}
		if lock.Ts == txn.StartTS {
			keys = append(keys, dataKey)
		}
	}
	iter.Close()

	if req.CommitVersion == 0 {
		resp.Error, err = server.kvBatchRollback(txn, keys, req.StartVersion)
	} else {
		resp.Error, err = server.kvCommit(txn, keys, req.StartVersion, req.CommitVersion)
	}
	if resp.Error != nil || err != nil {
		return resp, err
	}

	err = server.storage.Write(req.Context, txn.Writes())
	return resp, err
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
