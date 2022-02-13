package surfstore

import (
	"io/ioutil"
	"log"
	"math"
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

func StringInArray(s string, arr []string) bool {
	for _, b := range arr {
		if b == s {
			return true
		}
	}
	return false
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
	if err != nil {
		log.Printf("Received error while loading local meta file %v ", err)
	}
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
					local_filemetas_towrite[filename] = &FileMetaData{}
					local_filemetas_towrite[filename].Filename = filename
					local_filemetas_towrite[filename].Version = local_meta.Version + 1
					local_filemetas_towrite[filename].BlockHashList = hash_list
				} else {
					// File is unchanged
					local_filemetas_towrite[filename] = &FileMetaData{}
					local_filemetas_towrite[filename].Filename = filename
					local_filemetas_towrite[filename].Version = local_meta.Version
					local_filemetas_towrite[filename].BlockHashList = local_meta.BlockHashList
				}
			} else {
				// File is not in local_index
				local_filemetas_towrite[filename] = &FileMetaData{}
				local_filemetas_towrite[filename].Filename = filename
				local_filemetas_towrite[filename].Version = 1
				local_filemetas_towrite[filename].BlockHashList = hash_list
			}

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
		local_meta, ok := local_filemetas_towrite[filename]
		if !ok || local_meta.Version < remote_meta.Version {
			// Download files
			hash_list := remote_meta.BlockHashList
			bytes_to_write := []byte{}
			// Get blocks via rpc call
			for _, hash_string := range hash_list {
				block := &Block{}
				err := client.GetBlock(hash_string, block_store_addr.Addr, block)
				if err != nil {
					log.Printf("Received error getting block %v", err)
				}
				bytes_to_write = append(bytes_to_write, block.BlockData...)
			}
			// Write out file in base_dir
			local_filemetas_towrite[filename] = remote_meta
			path := ConcatPath(base_dir, filename)
			err := ioutil.WriteFile(path, bytes_to_write, 0644)
			if err != nil {
				log.Printf("Received error writing file locally %v", err)
			}

		}
	}

	// Look for files to upload
	for filename, local_meta := range local_filemetas_towrite {
		remote_meta, ok := remote_index[filename]
		// If doesn't exist or if there are changes that need to be synced
		if !ok || local_meta.Version > remote_meta.Version {
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
			err = client.HasBlocks(local_meta.BlockHashList, block_store_addr.Addr, &block_hashes_in_bs)
			if err != nil {
				log.Printf("Received error while getting hashstrings in blockstore %v ", err)
			}

			// Put blocks into blockstore
			overall_success := true
			for hash_string, block_data := range hash_to_block {
				if !StringInArray(hash_string, block_hashes_in_bs) {
					success := &Success{Flag: false}
					err = client.PutBlock(block_data, block_store_addr.Addr, &success.Flag)
					if err != nil {
						log.Printf("Received error putting block %v ", err)
					}
					overall_success = overall_success && success.Flag
				}
			}
			// Update remote index
			if overall_success {
				var server_version int32
				err := client.UpdateFile(local_filemetas_towrite[filename], &server_version)
				if err != nil {
					log.Printf("Received err updating remote index %v", err)
				}
				// Conflict, need to download server version
				if server_version == -1 {
					err = client.GetFileInfoMap(&remote_index)
					if err != nil {
						log.Printf("Received error in GetFileInfoMap %v", err)
					}
					remote_meta = remote_index[filename]
					hash_list := remote_meta.BlockHashList
					bytes_to_write := []byte{}
					// Get blocks via rpc call
					for _, hash_string := range hash_list {
						block := &Block{}
						err := client.GetBlock(hash_string, block_store_addr.Addr, block)
						if err != nil {
							log.Printf("Received error getting block %v", err)
						}
						bytes_to_write = append(bytes_to_write, block.BlockData...)
					}
					// Write out file in base_dir
					local_filemetas_towrite[filename] = remote_meta
					path := ConcatPath(base_dir, filename)
					err := ioutil.WriteFile(path, bytes_to_write, 0644)
					if err != nil {
						log.Printf("Received error writing file locally %v", err)
					}
				}
			}

		}
	}
	err = WriteMetaFile(local_filemetas_towrite, base_dir)
	if err != nil {
		log.Printf("Received error writing local index %v", err)
	}
}
