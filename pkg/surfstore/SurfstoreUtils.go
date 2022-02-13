package surfstore

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
)

func Equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func StringInArray(s string, arr []string) (flag bool, idx int) {
	for i, b := range arr {
		if b == s {
			return true, i
		}
	}
	return false, -1
}

func remove(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func DownloadFile(client RPCClient, filename string, remote_meta *FileMetaData, base_dir string, block_store_addr string) (FileMetaData, error) {
	hash_list := remote_meta.BlockHashList
	local_meta := FileMetaData{}
	if Equal(hash_list, []string{"0"}) {
		local_meta = *remote_meta
		err := os.Remove(ConcatPath(base_dir, filename))
		if err != nil {
			return local_meta, err
		}
	}
	bytes_to_write := []byte{}
	// Get blocks via rpc call
	for _, hash_string := range hash_list {
		block := &Block{}
		err := client.GetBlock(hash_string, block_store_addr, block)
		if err != nil {
			log.Printf("Received error getting block %v", err)
		}
		bytes_to_write = append(bytes_to_write, block.BlockData...)
	}
	// Write out file in base_dir
	local_meta = *remote_meta
	path := ConcatPath(base_dir, filename)
	err := ioutil.WriteFile(path, bytes_to_write, 0644)
	if err != nil {
		log.Printf("Received error writing file locally %v", err)
	}
	return local_meta, err
}

func UploadFile(client RPCClient, filename string, local_meta *FileMetaData,
	base_dir string, block_store_addr string, block_size int) (FileMetaData, error) {
	full_path := ConcatPath(base_dir, filename)
	data, err := ioutil.ReadFile(full_path)
	if err != nil {
		log.Printf("Received error while reading file %v", err)
	}
	num_of_blocks := int(math.Ceil(float64(len(data)) / float64(block_size)))
	//Get blocks and map them to hash strings from file's local meta
	hash_to_block := make(map[string]*Block)
	for i := 0; i < num_of_blocks; i++ {
		hash_string := local_meta.BlockHashList[i]
		end := int(math.Min(float64((i+1)*block_size), float64(len(data))))
		block := data[i*block_size : end]
		hash_to_block[hash_string] = &Block{BlockData: block, BlockSize: int32(len(block))}
	}
	//Get hashes for file that are in blockstore
	block_hashes_in_bs := []string{}
	err = client.HasBlocks(local_meta.BlockHashList, block_store_addr, &block_hashes_in_bs)
	if err != nil {
		log.Printf("Received error while getting hashstrings in blockstore %v ", err)
	}
	// Put blocks into blockstore
	overall_success := true
	for hash_string, block_data := range hash_to_block {
		flag, _ := StringInArray(hash_string, block_hashes_in_bs)
		if !flag {
			success := &Success{Flag: false}
			err = client.PutBlock(block_data, block_store_addr, &success.Flag)
			if err != nil {
				log.Printf("Received error putting block %v ", err)
			}
			overall_success = overall_success && success.Flag
		}
	}
	// Update remote index
	if overall_success {
		var server_version int32
		local_meta.Version = local_meta.Version + 1
		err := client.UpdateFile(local_meta, &server_version)
		if err != nil {
			log.Printf("Received err updating remote index %v", err)
		}
		// Conflict, need to download server version
		if server_version == -1 {
			remote_index := make(map[string]*FileMetaData)
			err = client.GetFileInfoMap(&remote_index)
			if err != nil {
				log.Printf("Received error in GetFileInfoMap %v", err)
			}
			remote_meta := remote_index[filename]
			hash_list := remote_meta.BlockHashList
			bytes_to_write := []byte{}
			// Get blocks via rpc call
			for _, hash_string := range hash_list {
				block := &Block{}
				err := client.GetBlock(hash_string, block_store_addr, block)
				if err != nil {
					log.Printf("Received error getting block %v", err)
				}
				bytes_to_write = append(bytes_to_write, block.BlockData...)
			}
			// Write out file in base_dir
			path := ConcatPath(base_dir, filename)
			err := ioutil.WriteFile(path, bytes_to_write, 0644)
			if err != nil {
				log.Printf("Received error writing file locally %v", err)
			}
			return *remote_meta, nil
		} else {
			return *local_meta, nil
		}
	} else {
		return FileMetaData{}, nil
	}
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	base_dir := client.BaseDir
	block_size := client.BlockSize
	block_store_addr := &BlockStoreAddr{}
	err := client.GetBlockStoreAddr(&block_store_addr.Addr)
	if err != nil {
		log.Printf("Received error while gettting block store address %v", err)
	}
	// Create filemetas to be written locally in the end
	local_filemetas_towrite := make(map[string]*FileMetaData)

	// Read files from base directory
	files, err := ioutil.ReadDir(base_dir)
	if err != nil {
		log.Printf("Received error while reading files in base directory %v", err)
	}

	// Check if there is a local index.txt
	is_index_present := false
	for _, file := range files {
		if file.Name() == DEFAULT_META_FILENAME {
			is_index_present = true
		}
	}
	// Write local index.txt if not exists
	if !is_index_present {
		err := WriteMetaFile(local_filemetas_towrite, base_dir)
		if err != nil {
			log.Printf("Received error while writing local meta file %v ", err)
		}
	}

	// Get current local_index
	local_index, err := LoadMetaFromMetaFile(base_dir)
	local_filemetas_towrite = local_index

	if err != nil {
		log.Printf("Received error while loading local meta file %v ", err)
	}
	files_to_upload := []string{}
	files_to_download := []string{}
	for _, file := range files {
		filename := file.Name()
		if (filename != DEFAULT_META_FILENAME) && (!file.IsDir()) {
			// Get file local_meta_data
			local_meta, ok_local := local_index[filename]
			// Open file and get hash_list
			full_path := ConcatPath(base_dir, filename)
			data, err := ioutil.ReadFile(full_path)
			if err != nil {
				log.Printf("Received error while reading file %v", err)
			}
			num_of_blocks := int(math.Ceil(float64(len(data)) / float64(block_size)))
			hash_list := []string{}
			for i := 0; i < num_of_blocks; i++ {
				end := int(math.Min(float64((i+1)*block_size), float64(len(data))))
				block := data[i*block_size : end]
				hash_string := GetBlockHashString(block)
				hash_list = append(hash_list, hash_string)
			}
			//File is in local_index
			if ok_local {
				// File has uncommited changes
				if !Equal(hash_list, local_meta.BlockHashList) {
					files_to_upload = append(files_to_upload, filename)
					local_meta.BlockHashList = hash_list
				}
			} else {
				// File is not in local_index
				files_to_upload = append(files_to_upload, filename)
				local_index[filename] = &FileMetaData{Filename: filename, Version: 0, BlockHashList: hash_list}
			}

		}
	}
	// Mark files that have been deleted
	files_deleted := []string{}
	for filename, _ := range local_index {
		path := ConcatPath(base_dir, filename)
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			files_deleted = append(files_deleted, filename)
		}
	}

	// Fetch remote filemetas from MetaStore
	remote_index := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remote_index)
	if err != nil {
		log.Printf("Received error in GetFileInfoMap %v", err)
	}

	// Look for files to download
	for filename, remote_meta := range remote_index {
		local_meta, ok := local_index[filename]
		if ok {
			if local_meta.Version < remote_meta.Version {
				files_to_download = append(files_to_download, filename)
				in_arr, idx := StringInArray(filename, files_to_upload)
				if in_arr {
					files_to_upload = remove(files_to_upload, idx)
				}
			}
		} else {
			files_to_download = append(files_to_download, filename)
		}
	}

	//Download needed files
	fmt.Println(files_to_download)
	for _, filename := range files_to_download {
		new_meta, err := DownloadFile(client, filename, remote_index[filename], base_dir, block_store_addr.Addr)
		if err != nil {
			log.Printf("Received error downloading file %v", err)
		} else {
			local_filemetas_towrite[filename] = &new_meta
		}
	}

	//Upload needed files
	fmt.Println(files_to_upload)
	for _, filename := range files_to_upload {
		new_meta, err := UploadFile(client, filename, local_index[filename], base_dir, block_store_addr.Addr, block_size)
		if err != nil {
			log.Printf("Received error downloading file %v", err)
		} else {
			local_filemetas_towrite[filename] = &new_meta
		}
	}

	// Look for files to upload
	// for filename, local_meta := range local_filemetas_towrite {
	// 	remote_meta, ok := remote_index[filename]
	// 	// If doesn't exist or if there are changes that need to be synced
	// 	if !ok || local_meta.Version > remote_meta.Version {
	// 		full_path := ConcatPath(base_dir, filename)
	// 		// If file needs to be deleted on metastore
	// 		if Equal(local_meta.BlockHashList, []string{"0"}) {
	// 			//Update File
	// 			var server_version int32
	// 			err := client.UpdateFile(local_meta, &server_version)
	// 			if err != nil {
	// 				log.Printf("Received err updating remote index %v", err)
	// 			}
	// 			// Conflict, need to download server version
	// 			if server_version == -1 {
	// 				err = client.GetFileInfoMap(&remote_index)
	// 				if err != nil {
	// 					log.Printf("Received error in GetFileInfoMap %v", err)
	// 				}
	// 				remote_meta = remote_index[filename]
	// 				hash_list := remote_meta.BlockHashList
	// 				bytes_to_write := []byte{}
	// 				// Get blocks via rpc call
	// 				for _, hash_string := range hash_list {
	// 					block := &Block{}
	// 					err := client.GetBlock(hash_string, block_store_addr.Addr, block)
	// 					if err != nil {
	// 						log.Printf("Received error getting block %v", err)
	// 					}
	// 					bytes_to_write = append(bytes_to_write, block.BlockData...)
	// 				}
	// 				// Write out file in base_dir
	// 				local_filemetas_towrite[filename] = remote_meta
	// 				path := ConcatPath(base_dir, filename)
	// 				err := ioutil.WriteFile(path, bytes_to_write, 0644)
	// 				if err != nil {
	// 					log.Printf("Received error writing file locally %v", err)
	// 				}
	// 			} else {
	// 				os.Remove(full_path)
	// 			}
	// 			continue
	// 		}

	// 	}
	// }
	err = WriteMetaFile(local_filemetas_towrite, base_dir)
	if err != nil {
		log.Printf("Received error writing local index %v", err)
	}
}
