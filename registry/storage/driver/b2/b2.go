package b2

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "b2"

// 10MiB
const defaultChunkSize int64 = 10 << 20

// DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	KeyID          string
	ApplicationKey string
	Bucket         string
	ChunkSize      int64
	RootDirectory  string
}

func init() {
	factory.Register(driverName, &b2DriverFactory{})
}

type b2DriverFactory struct{}

func (factory *b2DriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	client        *b2Client
	Bucket        string
	ChunkSize     int64
	RootDirectory string
}

type baseEmbed struct {
	base.Base
}

type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - keyid
// - applicationkey
// - bucket
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	keyID, ok := parameters["keyid"]
	if !ok {
		return nil, fmt.Errorf("No keyid parameter provided")
	}

	applicationKey, ok := parameters["applicationkey"]
	if !ok {
		return nil, fmt.Errorf("No applicationkey parameter provided")
	}

	bucket, ok := parameters["bucket"]
	if !ok {
		return nil, fmt.Errorf("No bucket paramter provided")
	}

	chunkSize := defaultChunkSize
	chunkSizeData, ok := parameters["chunksize"]
	if ok {
		var err error
		chunkSize, err = strconv.ParseInt(fmt.Sprint(chunkSizeData), 10, 64)
		if err != nil {
			return nil, err
		}
	}

	rootDirectory, ok := parameters["rootdirectory"]
	if !ok {
		rootDirectory = ""
	}

	params := DriverParameters{
		KeyID:          fmt.Sprint(keyID),
		ApplicationKey: fmt.Sprint(applicationKey),
		Bucket:         fmt.Sprint(bucket),
		RootDirectory:  fmt.Sprint(rootDirectory),
	}

	return New(params)
}

// New constructs a new Driver
func New(params DriverParameters) (*Driver, error) {
	d := &driver{
		client:        newClient(params.KeyID, params.ApplicationKey),
		Bucket:        params.Bucket,
		ChunkSize:     params.ChunkSize,
		RootDirectory: params.RootDirectory,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	reader, err := d.client.Download(ctx, d.Bucket, d.b2Path(path), 0)
	if err != nil {
		return nil, convertError(err, path)
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	return d.client.Upload(ctx, d.Bucket, d.b2Path(path), content)
}

func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	reader, err := d.client.Download(ctx, d.Bucket, d.b2Path(path), offset)
	if err != nil {
		return nil, convertError(err, path)
	}
	return reader, nil
}

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	return d.client.UploadLargeFile(ctx, d.Bucket, d.b2Path(path), d.ChunkSize, append)
}

// TODO: Stat, List, Move, Delete, URLFor, Walk
// https://godoc.org/github.com/docker/distribution/registry/storage/driver#StorageDriver

func (d *driver) b2Path(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.RootDirectory, "/")+path, "/")
}

func convertError(err error, path string) error {
	if isNotFound(err) {
		return storagedriver.PathNotFoundError{
			Path:       path,
			DriverName: driverName,
		}
	}

	return err
}
