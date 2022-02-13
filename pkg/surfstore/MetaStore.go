package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	metastore_lock sync.Mutex
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.metastore_lock.Lock()
	fmd, ok := m.FileMetaMap[fileMetaData.Filename]
	if ok {
		current_version := fmd.Version
		new_version := fileMetaData.Version
		if (new_version - current_version) != 1 {
			m.metastore_lock.Unlock()
			return &Version{Version: -1}, nil
		} else {
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
			m.metastore_lock.Unlock()
			return &Version{Version: fileMetaData.Version}, nil
		}
	} else {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		m.metastore_lock.Unlock()
		return &Version{Version: fileMetaData.Version}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
