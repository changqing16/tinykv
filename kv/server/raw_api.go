package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	data, err := reader.GetCF(req.Cf, req.Key)
	resp.Value = data
	if data == nil {
		resp.NotFound = true
	}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := &kvrpcpb.RawPutResponse{}
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	err := server.storage.Write(req.Context, []storage.Modify{{Data: put}})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := &kvrpcpb.RawDeleteResponse{}
	delete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	err := server.storage.Write(req.Context, []storage.Modify{{Data: delete}})
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	results := make([]*kvrpcpb.KvPair, 0, req.Limit)
	for i := 0; i < int(req.Limit) && iter.Valid(); i++ {
		item := iter.Item()
		data := &kvrpcpb.KvPair{
			Key: item.KeyCopy(nil),
		}

		value, err := item.ValueCopy(nil)
		if err != nil {
			data.Error = &kvrpcpb.KeyError{
				Abort: err.Error(),
			}
		} else {
			data.Value = value
		}

		results = append(results, data)
		iter.Next()
	}
	resp.Kvs = results
	return resp, nil
}
