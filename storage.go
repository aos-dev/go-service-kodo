package kodo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	qs "github.com/qiniu/go-sdk/v7/storage"

	"github.com/aos-dev/go-storage/v3/pkg/iowrap"
	. "github.com/aos-dev/go-storage/v3/types"
)

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	o = s.newObject(false)
	o.Mode = ModeRead
	o.ID = s.getAbsPath(path)
	o.Path = path
	return o
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)

	err = s.bucket.Delete(s.name, rp)
	if err != nil && checkError(err, responseCodeResourceNotExist) {
		// Omit `612`(resource to be deleted dose not exist) error code here
		//
		// References
		// - [AOS-46](https://github.com/aos-dev/specs/blob/master/rfcs/46-idempotent-delete.md)
		// - https://developer.qiniu.com/kodo/1257/delete
		err = nil
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	input := &objectPageStatus{
		limit:  1000,
		prefix: s.getAbsPath(path),
	}

	var nextFn NextObjectFunc

	switch {
	case opt.ListMode.IsDir():
		input.delimiter = "/"
		nextFn = s.nextObjectPageByDir
	case opt.ListMode.IsPrefix():
		nextFn = s.nextObjectPageByPrefix
	default:
		return nil, fmt.Errorf("invalid list mode")
	}

	return NewObjectIterator(ctx, nextFn, input), nil
}

func (s *Storage) metadata(ctx context.Context, opt pairStorageMetadata) (meta *StorageMeta, err error) {
	meta = NewStorageMeta()
	meta.Name = s.name
	meta.WorkDir = s.workDir
	return meta, nil
}

func (s *Storage) nextObjectPageByDir(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	entries, commonPrefix, nextMarker, _, err := s.bucket.ListFiles(
		s.name,
		input.prefix,
		input.delimiter,
		input.marker,
		input.limit,
	)
	if err != nil {
		return err
	}

	for _, v := range commonPrefix {
		o := s.newObject(true)
		o.ID = v
		o.Path = s.getRelPath(v)
		o.Mode |= ModeDir

		page.Data = append(page.Data, o)
	}

	for _, v := range entries {
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if nextMarker == "" {
		return IterateDone
	}

	input.marker = nextMarker
	return nil
}

func (s *Storage) nextObjectPageByPrefix(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	entries, _, nextMarker, _, err := s.bucket.ListFiles(
		s.name,
		input.prefix,
		input.delimiter,
		input.marker,
		input.limit,
	)
	if err != nil {
		return err
	}

	for _, v := range entries {
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if nextMarker == "" {
		return IterateDone
	}

	input.marker = nextMarker
	return nil
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	rp := s.getAbsPath(path)

	deadline := time.Now().Add(time.Hour).Unix()
	url := qs.MakePrivateURL(s.bucket.Mac, s.domain, rp, deadline)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}

	resp, err := s.bucket.Client.Do(ctx, req)
	if err != nil {
		return 0, err
	}

	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil {
			err = cerr
		}
	}()

	if resp.StatusCode != http.StatusOK {
		err = qs.ResponseError(resp)
		return 0, err
	}

	rc := resp.Body

	if opt.HasIoCallback {
		rc = iowrap.CallbackReadCloser(resp.Body, opt.IoCallback)
	}

	return io.Copy(w, rc)
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	rp := s.getAbsPath(path)

	fi, err := s.bucket.Stat(s.name, rp)
	if err != nil {
		return nil, err
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path
	o.Mode |= ModeRead

	o.SetLastModified(convertUnixTimestampToTime(fi.PutTime))
	o.SetContentLength(fi.Fsize)

	if fi.Hash != "" {
		o.SetEtag(fi.Hash)
	}
	if fi.MimeType != "" {
		o.SetContentType(fi.MimeType)
	}

	var sm ObjectMetadata
	sm.StorageClass = fi.Type
	o.SetServiceMetadata(sm)

	return o, nil
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}

	rp := s.getAbsPath(path)

	uploader := qs.NewFormUploader(s.bucket.Cfg)
	ret := qs.PutRet{}
	err = uploader.Put(ctx,
		&ret, s.putPolicy.UploadToken(s.bucket.Mac), rp, r, size, nil)
	if err != nil {
		return
	}
	return size, nil
}
