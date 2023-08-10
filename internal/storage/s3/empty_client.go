package s3

import (
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"go.uber.org/fx"
)

type (
	emptyS3Client struct {
		s3iface.S3API
	}

	emptyDownloader struct {
		Downloader
	}

	emptyUploader struct {
		Uploader
	}

	emptyClientOption struct{}
)

// WithEmptyClient injects an empty implementation of the s3 interfaces.
// You may use this option when the storage is not referenced in your test case.
func WithEmptyClient() fx.Option {
	return fx.Provide(func() *emptyClientOption { return &emptyClientOption{} })
}
