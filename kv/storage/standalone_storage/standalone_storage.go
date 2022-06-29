package standalone_storage

import (
	"fmt"

	"github.com/pingcap-incubator/tinykv/log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf *config.Config
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil{
		log.Fatal("create db fails!");
	}
	return &StandAloneStorage{conf : conf,db: db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	sr := &StandAloneStorageReader{db: s.db}
	return sr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wbatch := new(engine_util.WriteBatch)
	for _,modify := range batch{
		switch modify.Data.(type){
		case storage.Put :
			put := modify.Data.(storage.Put)
			wbatch.SetCF(put.Cf,put.Key,put.Value)
			//log.Debug("xiaoming",modify.Cf(),modify.Data.(*storage.Put).Cf)
		case storage.Delete :
			del := modify.Data.(storage.Delete)
			wbatch.DeleteCF(del.Cf,del.Key)
		default :
			log.Debug(fmt.Sprintf("type %T",modify.Data))
		}
	}
	return wbatch.WriteToDB(s.db)

}

type StandAloneStorageReader struct{
	
	db *badger.DB
}

// func NewStandAloneStorageReader(conf *config.Config) *StandAloneStorageReader {
// 	// Your Code Here (1).
	
// 	opts := badger.DefaultOptions
// 	opts.Dir = conf.DBPath
// 	opts.ValueDir = conf.DBPath
// 	db, err := badger.Open(opts)
// 	if err != nil{
// 		log.Fatal("create db fails!");
// 	}
// 	return &StandAloneStorageReader{conf : conf,db: db}
// }

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error){
	val, err := engine_util.GetCF(s.db, cf, key)
	if val == nil{
		return nil,nil
	}else{
		return val,err
	}
}
func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator{
	txn := s.db.NewTransaction(false)
	//defer txn.Discard()
	defaultIter := engine_util.NewCFIterator(cf, txn)
	return defaultIter

}
func (s *StandAloneStorageReader) Close(){
	s.db.Close()
}