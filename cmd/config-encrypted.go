/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"bytes"
	"fmt"
	"path"
	"unicode/utf8"

	"github.com/zhaohuxing/minio/cmd/config"
	"github.com/zhaohuxing/minio/cmd/logger"
	"github.com/zhaohuxing/minio/pkg/auth"
	"github.com/zhaohuxing/minio/pkg/kms"
	"github.com/zhaohuxing/minio/pkg/madmin"
)

func handleEncryptedConfigBackend(objAPI ObjectLayer) error {
	encrypted, err := checkBackendEncrypted(objAPI)
	if err != nil {
		return fmt.Errorf("Unable to encrypt config %w", err)
	}
	if err = migrateConfigPrefixToEncrypted(objAPI, encrypted); err != nil {
		return fmt.Errorf("Unable to migrate all config at .minio.sys/config/: %w", err)
	}
	return nil
}

const backendEncryptedFile = "backend-encrypted"

var backendEncryptedMigrationComplete = []byte("encrypted")

func checkBackendEncrypted(objAPI ObjectLayer) (bool, error) {
	data, err := readConfig(GlobalContext, objAPI, backendEncryptedFile)
	if err != nil && err != errConfigNotFound {
		return false, err
	}
	return err == nil && bytes.Equal(data, backendEncryptedMigrationComplete), nil
}

// decryptData - decrypts input data with more that one credentials,
func decryptData(edata []byte, creds ...auth.Credentials) ([]byte, error) {
	var err error
	var data []byte
	for _, cred := range creds {
		data, err = madmin.DecryptData(cred.String(), bytes.NewReader(edata))
		if err != nil {
			if err == madmin.ErrMaliciousData {
				continue
			}
			return nil, err
		}
		break
	}
	return data, err
}

func migrateConfigPrefixToEncrypted(objAPI ObjectLayer, encrypted bool) error {
	if !encrypted {
		return nil
	}
	if encrypted {
		if GlobalKMS != nil {
			stat, err := GlobalKMS.Stat()
			if err != nil {
				return err
			}
			logger.Info("Attempting to re-encrypt config, IAM users and policies on MinIO with %q (%s)", stat.DefaultKey, stat.Name)
		} else {
			logger.Info("Attempting to migrate encrypted config, IAM users and policies on MinIO to a plaintext format. To encrypt all MinIO config data a KMS is needed")
		}
	}

	var marker string
	for {
		res, err := objAPI.ListObjects(GlobalContext, MinioMetaBucket, minioConfigPrefix, marker, "", maxObjectList)
		if err != nil {
			return err
		}
		for _, obj := range res.Objects {
			data, err := readConfig(GlobalContext, objAPI, obj.Name)
			if err != nil {
				return err
			}

			if !utf8.Valid(data) {
				data, err = decryptData(data, globalActiveCred)
				if err != nil {
					return fmt.Errorf("Decrypting config failed %w, possibly credentials are incorrect", err)
				}
			}
			if GlobalKMS != nil {
				data, err = config.EncryptBytes(GlobalKMS, data, kms.Context{
					obj.Bucket: path.Join(obj.Bucket, obj.Name),
				})
				if err != nil {
					return err
				}
			}
			if err = saveConfig(GlobalContext, objAPI, obj.Name, data); err != nil {
				return err
			}
		}

		if !res.IsTruncated {
			break
		}
		marker = res.NextMarker
	}
	if encrypted {
		if GlobalKMS != nil {
			logger.Info("Migration of encrypted config data completed. All config data is now encrypted with the KMS")
		} else {
			logger.Info("Migration of encrypted config data completed. All config data is now stored in plaintext")
		}
	}
	return deleteConfig(GlobalContext, globalObjectAPI, backendEncryptedFile)
}
