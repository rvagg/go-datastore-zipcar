/*
Package zipcar provides an implementation of a Datastore (https://github.com/ipfs/go-datastore) that operates
on ZIP files, with the addition of some utility methods to interact via CID rather than the native
Datastore Key type.

ZipDatastore is similar in concept to go-car (https://github.com/ipfs/go-car), the content addressable archive
format but uses native ZIP format for storage and indexing for easier cross-language compatibility and
interoperability with the rich native system tooling that exists for ZIP files.

It is assumed that all `key`s provided are stringified CIDs (https://github.com/ipfs/go-cid).
A `key` that does not convert to a CID will raise an error. Support for non-CIDs is not precluded
in the future, it will just require a lot more testing (i.e. if you bring a PR for this, make sure
it has lots of tests!)

Entries are stored with their stringified key/CID as the filename and the binary data as the file contents.
Version 0 CIDs are converted to base58btc strings while version 1 CIDs are converted to base32 strings.

Calling any mutation operation, Put() or Delete(), will cause the ZIP archive to be written or rewritten when
Close() is called. This may become expensive for large archives as the contents are stored in memory until the
new file is written, so care should be taken.
*/
package zipcar

import (
	"archive/zip"
	"errors"
	"io/ioutil"
	"os"
	"time"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	mbase "github.com/multiformats/go-multibase"
)

var (
	// ErrUnimplemented indicates that the method being called has not yet been implemented (but could, send a PR!)
	ErrUnimplemented = errors.New("zipcar: unimplemented operation")
)

// ZipDatastore is an implementation of a Datastore (https://github.com/ipfs/go-datastore) that operates
// on ZIP files.
type ZipDatastore struct {
	index    map[string]*zip.File
	cache    map[string][]byte
	file     *os.File
	modified bool
}

var _ ds.Datastore = (*ZipDatastore)(nil)

// PutCid is a utility method that calls Put() with the provided CID converted to a ds.Key.
func (zipDs *ZipDatastore) PutCid(cid cid.Cid, value []byte) (err error) {
	return zipDs.Put(dshelp.CidToDsKey(cid), value)
}

// Put stores the given key/value pair as a file in the underlying ZIP archive. `key` must be a string formatted CID.
// As a mutation operation, calling this method one or more times will trigger a full rewrite of the ZIP archive upon
// Close().
func (zipDs *ZipDatastore) Put(key ds.Key, value []byte) (err error) {
	cidStr, err := dsKeyToCidString(key)
	if err != nil {
		return err
	}

	if has, _ := zipDs.has(cidStr); has { // dupe, assume CID is correct and ignore
		return nil
	}

	zipDs.modified = true
	zipDs.cache[*cidStr] = value

	return nil
}

// GetCid is a utility method that calls Get() with the provided CID converted to a ds.Key.
func (zipDs *ZipDatastore) GetCid(cid cid.Cid) (value []byte, err error) {
	return zipDs.Get(dshelp.CidToDsKey(cid))
}

// Get retrieves the given `key` if it exists in the underlying ZIP archive. A ds.ErrNotFound error is
// returned if it is not found, otherwise the binary data is returned. `key` must be a string formatted CID.
func (zipDs *ZipDatastore) Get(key ds.Key) (value []byte, err error) {
	cidStr, err := dsKeyToCidString(key)
	if err != nil {
		return nil, err
	}

	if zipDs.cache[*cidStr] != nil {
		return zipDs.cache[*cidStr], nil
	}

	f := zipDs.index[*cidStr]
	if f == nil {
		return nil, ds.ErrNotFound
	}

	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	zipDs.cache[*cidStr], err = ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	return zipDs.cache[*cidStr], nil
}

// Has returns a bool indicating whether the given key exists in the underlying ZIP archive.
// `key` must be a string formatted CID.
func (zipDs *ZipDatastore) Has(key ds.Key) (bool, error) {
	cidStr, err := dsKeyToCidString(key)
	if err != nil {
		return false, err
	}

	return zipDs.has(cidStr)
}

func (zipDs *ZipDatastore) has(cidStr *string) (bool, error) {
	return zipDs.cache[*cidStr] != nil || zipDs.index[*cidStr] != nil, nil
}

// HasCid is a utility method that calls Has() with the provided CID converted to a ds.Key.
func (zipDs *ZipDatastore) HasCid(cid cid.Cid) (bool, error) {
	return zipDs.Has(dshelp.CidToDsKey(cid))
}

// DeleteCid is a utility method that calls Delete() with the provided CID converted to a ds.Key.
func (zipDs *ZipDatastore) DeleteCid(cid cid.Cid) error {
	return zipDs.Delete(dshelp.CidToDsKey(cid))
}

// Delete removes the given key's record from the ZIP archive. As a mutation operation, calling this method
// one or more times will trigger a full rewrite of the ZIP archive upon Close().
func (zipDs *ZipDatastore) Delete(key ds.Key) error {
	cidStr, err := dsKeyToCidString(key)
	if err != nil {
		return err
	}
	zipDs.cache[*cidStr] = nil
	zipDs.index[*cidStr] = nil
	return nil
}

// GetSizeCid is a utility method that calls GetSize() with the provided CID converted to a ds.Key.
func (zipDs *ZipDatastore) GetSizeCid(cid cid.Cid) (int, error) {
	return zipDs.GetSize(dshelp.CidToDsKey(cid))
}

// GetSize returns the size of the binary data for the given key, where the size is the number of bytes.
// A ds.ErrNotFound error is returned if it is not found. `key` must be a string formatted CID.
func (zipDs *ZipDatastore) GetSize(key ds.Key) (int, error) {
	cidStr, err := dsKeyToCidString(key)
	if err != nil {
		return 0, err
	}

	if zipDs.cache[*cidStr] != nil {
		return len(zipDs.cache[*cidStr]), nil
	}

	f := zipDs.index[*cidStr]
	if f == nil {
		return 0, ds.ErrNotFound
	}

	return int(f.FileInfo().Size()), nil
}

// Query is not implemented, it will always return an error when called
func (zipDs *ZipDatastore) Query(q dsq.Query) (dsq.Results, error) {
	return nil, ErrUnimplemented
}

// Close should be called after ZipDatastore is no longer needed in order to ensure a
// properly formatted ZIP archive.
func (zipDs *ZipDatastore) Close() (err error) {
	if zipDs.modified {
		// load everything into cache that's not already so we can write it out again
		for cidStr, f := range zipDs.index {
			if f == nil { // deleted
				continue
			}
			if zipDs.cache[cidStr] == nil {
				rc, err := f.Open()
				if err != nil {
					return err
				}
				zipDs.cache[cidStr], err = ioutil.ReadAll(rc)
				rc.Close()
			}
		}
	}

	err = zipDs.file.Close()

	if err != nil || !zipDs.modified {
		// if it wasn't modified, no need for the rewrite below
		return err
	}

	// write the file from scratch, truncate if it exists
	zipDs.file, err = os.OpenFile(zipDs.file.Name(), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		ierr := zipDs.file.Close()
		if err == nil {
			err = ierr
		}
	}()

	writer := zip.NewWriter(zipDs.file)
	defer func() {
		ierr := writer.Close()
		if err == nil {
			err = ierr
		}
	}()

	for cidStr, bytes := range zipDs.cache {
		if bytes == nil { // deleted
			continue
		}
		fh := zip.FileHeader{Name: cidStr, Method: zip.Deflate, Modified: time.Now()}
		f, err := writer.CreateHeader(&fh)
		if err != nil {
			return err
		}
		_, err = f.Write(bytes)
		if err != nil {
			return err
		}
	}

	return err
}

func dsKeyToCidString(key ds.Key) (*string, error) {
	cid, err := dshelp.DsKeyToCid(key)
	if err != nil {
		return nil, err
	}
	var cidStr string
	if cid.Version() == 0 {
		cidStr, err = cid.StringOfBase(mbase.Base58BTC)
	} else {
		cidStr, err = cid.StringOfBase(mbase.Base32)
	}
	if err != nil {
		return nil, err
	}
	return &cidStr, nil
}

// NewDatastore instantiates a ZipDatastore for a given path on the filesystem. If the file exists and is
// a ZIP archive, its contents will be made available, otherwise a new, empty ZIP archive will be created.
//
// Always call Close() on a ZipDatastore when it is no longer required
func NewDatastore(path string) (*ZipDatastore, error) {
	var zipDs = ZipDatastore{modified: false}
	var err error
	var exists = true

	zipDs.index = make(map[string]*zip.File)
	zipDs.cache = make(map[string][]byte)

	fileinfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			exists = false
		} else {
			return nil, err
		}
	}

	zipDs.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	if exists {
		// read in existing keys
		reader, err := zip.NewReader(zipDs.file, fileinfo.Size())
		if err != nil {
			return nil, err
		}

		for _, f := range reader.File {
			zipDs.index[f.Name] = f
		}
	}

	return &zipDs, nil
}
