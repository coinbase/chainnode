package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/awstesting"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/storage/internal"
	"github.com/coinbase/chainnode/internal/storage/s3"
	s3mocks "github.com/coinbase/chainnode/internal/storage/s3/mocks"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type BlobStorageTestSuite struct {
	suite.Suite
	ctrl       *gomock.Controller
	app        testapp.TestApp
	storage    BlobStorage
	downloader *s3mocks.MockDownloader
	uploader   *s3mocks.MockUploader
}

func TestBlobStorageTestSuite(t *testing.T) {
	suite.Run(t, new(BlobStorageTestSuite))
}

func (s *BlobStorageTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.downloader = s3mocks.NewMockDownloader(s.ctrl)
	s.uploader = s3mocks.NewMockUploader(s.ctrl)

	s.app = testapp.New(
		s.T(),
		Module,
		fx.Provide(func() s3.Downloader { return s.downloader }),
		fx.Provide(func() s3.Uploader { return s.uploader }),
		fx.Populate(&s.storage),
	)
}

func (s *BlobStorageTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *BlobStorageTestSuite) TestUpload() {
	require := testutil.Require(s.T())

	key := "object-key"
	data := testutil.MakeFile(10)

	s.uploader.EXPECT().UploadWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(
			func(ctx context.Context, input *s3manager.UploadInput) (*s3manager.UploadOutput, error) {
				require.Equal(aws.String(s.app.Config().AWS.Bucket), input.Bucket)
				require.Equal(aws.String(key), input.Key)
				require.Equal(bytes.NewReader(data), input.Body)
				require.NotEmpty(input.ContentMD5)
				return &s3manager.UploadOutput{}, nil
			})

	err := s.storage.Upload(context.Background(), key, data)
	require.NoError(err)
}

func (s *BlobStorageTestSuite) TestUpload_Failure() {
	require := testutil.Require(s.T())

	key := "object-key"
	data := testutil.MakeFile(10)

	s.uploader.EXPECT().UploadWithContext(gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("failed to upload"))

	err := s.storage.Upload(context.Background(), key, data)
	require.Error(err)
}

func (s *BlobStorageTestSuite) TestDownload() {
	require := testutil.Require(s.T())

	key := "object-key"
	data := testutil.MakeFile(10)

	s.downloader.EXPECT().DownloadWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(
			func(ctx context.Context, writer io.WriterAt, input *awss3.GetObjectInput) (int64, error) {
				require.Equal(aws.String(s.app.Config().AWS.Bucket), input.Bucket)
				require.Equal(aws.String(key), input.Key)
				at, err := writer.WriteAt(data, 0)
				if err != nil {
					return 0, err
				}
				return int64(at), nil
			})

	actual, err := s.storage.Download(context.Background(), key)
	require.NoError(err)
	require.Equal(data, actual)
}

func (s *BlobStorageTestSuite) TestDownload_Failure() {
	require := testutil.Require(s.T())

	key := "object-key"

	s.downloader.EXPECT().DownloadWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(int64(0), xerrors.Errorf("failed to download"))

	actual, err := s.storage.Download(context.Background(), key)
	require.Error(err)
	require.Nil(actual)
}

func TestDownload_DownloadErrRequestCanceled(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	uploader := s3mocks.NewMockUploader(ctrl)
	downloader := s3manager.NewDownloader(unit.Session)

	var blobStorage BlobStorage
	app := testapp.New(
		t,
		Module,
		fx.Provide(func() s3.Downloader { return downloader }),
		fx.Provide(func() s3.Uploader { return uploader }),
		fx.Populate(&blobStorage),
	)
	defer app.Close()
	require.NotNil(blobStorage)

	ctx := &awstesting.FakeContext{DoneCh: make(chan struct{})}
	ctx.Error = fmt.Errorf("context canceled")
	close(ctx.DoneCh)

	actual, err := blobStorage.Download(ctx, "object-key")
	require.Error(err)
	require.Equal(internal.ErrRequestCanceled, err)
	require.Nil(actual)
}
