package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		log.Debug("create reader errors")
		return nil,err
	}
	b, err2 := reader.GetCF(req.Cf, req.Key)
	
	if err2 != nil {
		//log.Fatal("get value errors")
		return &kvrpcpb.RawGetResponse{NotFound: true},nil
	}
	resp := &kvrpcpb.RawGetResponse{}
	if b == nil{
		log.Debug("not fount key%s %s",req.Cf,req.Key)
		resp = &kvrpcpb.RawGetResponse{NotFound: true}
	}else{
		resp.Value = b
	}
	

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var batchs []storage.Modify
	put := storage.Put{
		Key : req.Key, 
		Value : req.Value,
		Cf : req.Cf,
	}
	batchs = append(batchs, storage.Modify{Data: put})
	err := server.storage.Write(nil, batchs)
	

	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var batchs []storage.Modify
	del := storage.Delete{
		Key : req.Key, 
		Cf : req.Cf,
	}
	batchs = append(batchs, storage.Modify{Data: del})
	err := server.storage.Write(nil, batchs)
	return &kvrpcpb.RawDeleteResponse{}, err
	
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil{
		return nil,err
	}
	d := reader.IterCF(req.Cf)
	d.Seek(req.StartKey)
	
	resp := &kvrpcpb.RawScanResponse{}
	
	for index := 0;d.Valid() && index < int(req.Limit);index++ {
		item := d.Item()
		val, _ := item.Value()
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: item.Key(),Value: val})
		d.Next()
	}
	d.Close()
	return resp, nil
}
