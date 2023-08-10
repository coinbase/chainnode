package blob

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501
	"encoding/base64"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/storage/internal"
	"github.com/coinbase/chainnode/internal/storage/s3"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/instrument"
)

type (
	BlobStorage interface {
		Upload(ctx context.Context, key string, data []byte) error
		Download(ctx context.Context, objectKey string) ([]byte, error)
	}

	BlobStorageParams struct {
		fx.In
		fxparams.Params
		Downloader s3.Downloader
		Uploader   s3.Uploader
	}

	blobStorageImpl struct {
		logger     *zap.Logger
		config     *config.Config
		bucket     string
		downloader s3.Downloader
		uploader   s3.Uploader

		blobStorageMetrics *blobStorageMetrics
		instrumentUpload   instrument.Call
		instrumentDownload instrument.Call
	}

	blobStorageMetrics struct {
		blobDownloadedSize tally.Gauge
		blobUploadedSize   tally.Gauge
	}
)

const (
	blobUploaderScopeName   = "uploader"
	blobDownloaderScopeName = "downloader"
	blobSizeMetricName      = "blob_size"
)

var _ BlobStorage = (*blobStorageImpl)(nil)

func NewBlobStorage(params BlobStorageParams) (BlobStorage, error) {
	metrics := params.Metrics.SubScope("blob_storage")
	return &blobStorageImpl{
		logger:             params.Logger,
		config:             params.Config,
		bucket:             params.Config.AWS.Bucket,
		downloader:         params.Downloader,
		uploader:           params.Uploader,
		blobStorageMetrics: newBlobStorageMetrics(metrics),
		instrumentUpload:   instrument.NewCall(metrics, "upload"),
		instrumentDownload: instrument.NewCall(metrics, "download"),
	}, nil
}

func newBlobStorageMetrics(scope tally.Scope) *blobStorageMetrics {
	return &blobStorageMetrics{
		blobDownloadedSize: scope.SubScope(blobDownloaderScopeName).Gauge(blobSizeMetricName),
		blobUploadedSize:   scope.SubScope(blobUploaderScopeName).Gauge(blobSizeMetricName),
	}
}

func (s *blobStorageImpl) Upload(ctx context.Context, key string, data []byte) error {
	if err := s.instrumentUpload.Instrument(ctx, func(ctx context.Context) error {
		defer s.logDuration("upload", time.Now())

		// #nosec G401
		h := md5.New()
		size, err := h.Write(data)
		if err != nil {
			return xerrors.Errorf("failed to compute checksum: %w", err)
		}

		checksum := base64.StdEncoding.EncodeToString(h.Sum(nil))

		if _, err := s.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Bucket:     aws.String(s.bucket),
			Key:        aws.String(key),
			Body:       bytes.NewReader(data),
			ContentMD5: aws.String(checksum),
		}); err != nil {
			return xerrors.Errorf("failed to upload to s3: %w", err)
		}

		s.blobStorageMetrics.blobUploadedSize.Update(float64(size))

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (s *blobStorageImpl) Download(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	if err := s.instrumentDownload.Instrument(ctx, func(ctx context.Context) error {
		defer s.logDuration("download", time.Now())

		buf := aws.NewWriteAtBuffer([]byte{})

		size, err := s.downloader.DownloadWithContext(ctx, buf, &awss3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				return internal.ErrRequestCanceled
			}
			return xerrors.Errorf("failed to download from s3 (bucket=%s, key=%s): %w", s.bucket, key, err)
		}

		data = buf.Bytes()

		s.blobStorageMetrics.blobDownloadedSize.Update(float64(size))
		return nil
	}); err != nil {
		return nil, err
	}

	return data, nil
}

func (s *blobStorageImpl) logDuration(method string, start time.Time) {
	s.logger.Debug(
		"blob_storage",
		zap.String("method", method),
		zap.Duration("duration", time.Since(start)),
	)
}
