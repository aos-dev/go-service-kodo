package kodo

import (
	"errors"
	"fmt"

	"strconv"
	"strings"
	"time"

	"github.com/qiniu/go-sdk/v7/auth/qbox"
	qc "github.com/qiniu/go-sdk/v7/client"
	qs "github.com/qiniu/go-sdk/v7/storage"

	ps "github.com/aos-dev/go-storage/v3/pairs"
	"github.com/aos-dev/go-storage/v3/pkg/credential"
	"github.com/aos-dev/go-storage/v3/pkg/endpoint"
	"github.com/aos-dev/go-storage/v3/pkg/httpclient"
	"github.com/aos-dev/go-storage/v3/services"
	typ "github.com/aos-dev/go-storage/v3/types"
)

// Service is the kodo config.
type Service struct {
	service *qs.BucketManager
}

// String implements Service.String
func (s *Service) String() string {
	return fmt.Sprintf("Servicer kodo")
}

// Storage is the gcs service client.
type Storage struct {
	bucket    *qs.BucketManager
	domain    string
	putPolicy qs.PutPolicy // kodo need PutPolicy to generate upload token.

	name    string
	workDir string

	pairPolicy typ.PairPolicy
}

// String implements Storager.String
func (s *Storage) String() string {
	return fmt.Sprintf(
		"Storager kodo {Name: %s, WorkDir: %s}",
		s.name, s.workDir,
	)
}

// New will create both Servicer and Storager.
func New(pairs ...typ.Pair) (typ.Servicer, typ.Storager, error) {
	return newServicerAndStorager(pairs...)
}

// NewServicer will create Servicer only.
func NewServicer(pairs ...typ.Pair) (typ.Servicer, error) {
	return newServicer(pairs...)
}

// NewStorager will create Storager only.
func NewStorager(pairs ...typ.Pair) (typ.Storager, error) {
	_, store, err := newServicerAndStorager(pairs...)
	return store, err
}

func newServicer(pairs ...typ.Pair) (srv *Service, err error) {
	defer func() {
		if err != nil {
			err = &services.InitError{Op: "new_servicer", Type: Type, Err: err, Pairs: pairs}
		}
	}()

	srv = &Service{}

	opt, err := parsePairServiceNew(pairs)
	if err != nil {
		return nil, err
	}

	cp, err := credential.Parse(opt.Credential)
	if err != nil {
		return nil, err
	}
	if cp.Protocol() != credential.ProtocolHmac {
		return nil, services.NewPairUnsupportedError(ps.WithCredential(opt.Credential))
	}
	ak, sk := cp.Hmac()

	mac := qbox.NewMac(ak, sk)
	cfg := &qs.Config{}
	srv.service = qs.NewBucketManager(mac, cfg)
	srv.service.Client.Client = httpclient.New(opt.HTTPClientOptions)
	return
}

func newServicerAndStorager(pairs ...typ.Pair) (srv *Service, store *Storage, err error) {
	defer func() {
		if err != nil {
			err = &services.InitError{Op: "new_storager", Type: Type, Err: err, Pairs: pairs}
		}
	}()

	srv, err = newServicer(pairs...)
	if err != nil {
		return
	}

	store, err = srv.newStorage(pairs...)
	if err != nil {
		if e := services.NewPairRequiredError(); errors.As(err, &e) {
			return srv, nil, nil
		}
		return nil, nil, err
	}
	return srv, store, nil
}

func convertUnixTimestampToTime(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(v, 0)
}

// All available storage classes are listed here.
const (
	// ref: https://developer.qiniu.com/kodo/api/3710/chtype
	StorageClassStandard   = 0
	StorageClassStandardIA = 1
	StorageClassArchive    = 2
)

// ref: https://developer.qiniu.com/kodo/api/3928/error-responses
func formatError(err error) error {
	e, ok := err.(*qc.ErrorInfo)
	if !ok {
		return err
	}

	// error code returned by kodo looks like http status code, but it's not.
	// kodo could return 6xx or 7xx for their costumed errors, so we use untyped int directly.
	switch e.Code {
	case 612:
		return fmt.Errorf("%w: %v", services.ErrObjectNotExist, err)
	case 403:
		return fmt.Errorf("%w: %v", services.ErrPermissionDenied, err)
	default:
		return err
	}
}

// newStorage will create a new client.
func (s *Service) newStorage(pairs ...typ.Pair) (store *Storage, err error) {
	opt, err := parsePairStorageNew(pairs)
	if err != nil {
		return nil, err
	}

	ep, err := endpoint.Parse(opt.Endpoint)
	if err != nil {
		return nil, err
	}

	store = &Storage{
		bucket: s.service,
		domain: ep.String(),
		putPolicy: qs.PutPolicy{
			Scope: opt.Name,
		},

		name:    opt.Name,
		workDir: "/",
	}

	if opt.HasWorkDir {
		store.workDir = opt.WorkDir
	}
	return store, nil
}

func (s *Service) formatError(op string, err error, name string) error {
	if err == nil {
		return nil
	}

	return &services.ServiceError{
		Op:       op,
		Err:      formatError(err),
		Servicer: s,
		Name:     name,
	}
}

// getAbsPath will calculate object storage's abs path
func (s *Storage) getAbsPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return prefix + path
}

// getRelPath will get object storage's rel path.
func (s *Storage) getRelPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return strings.TrimPrefix(path, prefix)
}

func (s *Storage) formatError(op string, err error, path ...string) error {
	if err == nil {
		return nil
	}

	return &services.StorageError{
		Op:       op,
		Err:      formatError(err),
		Storager: s,
		Path:     path,
	}
}

func (s *Storage) formatFileObject(v qs.ListItem) (o *typ.Object, err error) {
	o = s.newObject(false)
	o.ID = v.Key
	o.Path = s.getRelPath(v.Key)
	o.Mode |= typ.ModeRead

	o.SetContentLength(v.Fsize)
	o.SetLastModified(convertUnixTimestampToTime(v.PutTime))

	if v.MimeType != "" {
		o.SetContentType(v.MimeType)
	}
	if v.Hash != "" {
		o.SetEtag(v.Hash)
	}

	sm := make(map[string]string)
	sm[MetadataStorageClass] = strconv.Itoa(v.Type)
	o.SetServiceMetadata(sm)

	return
}

func (s *Storage) newObject(done bool) *typ.Object {
	return typ.NewObject(s, done)
}
