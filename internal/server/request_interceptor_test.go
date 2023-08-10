package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/golang/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	controllermocks "github.com/coinbase/chainnode/internal/controller/mocks"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type (
	eofReadCloser struct{}
)

func (e eofReadCloser) Read(p []byte) (n int, err error) {
	return 0, io.ErrUnexpectedEOF
}

func (e eofReadCloser) Close() error {
	return nil
}

func TestRequestInterceptor(t *testing.T) {
	const expectedBody = "{}"

	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	handler := controllermocks.NewMockHandler(ctrl)
	handler.EXPECT().
		PrepareContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request json.RawMessage) (context.Context, error) {
			require.Equal(json.RawMessage(expectedBody), request)
			return ctx, nil
		})

	request := &http.Request{
		Body: ioutil.NopCloser(bytes.NewBufferString(expectedBody)),
	}

	interceptor := NewRequestInterceptor(request, handler)
	require.Equal(json.RawMessage(expectedBody), interceptor.Body())

	_, err := interceptor.WithContext(context.Background())
	require.NoError(err)
}

func TestRequestInterceptor_Error(t *testing.T) {
	require := testutil.Require(t)

	ctrl := gomock.NewController(t)
	handler := controllermocks.NewMockHandler(ctrl)

	request := &http.Request{
		Body: eofReadCloser{},
	}

	interceptor := NewRequestInterceptor(request, handler)
	body := interceptor.Body()
	require.Nil(body)

	_, err := interceptor.WithContext(context.Background())
	require.Error(err)

	st, ok := status.FromError(err)
	require.True(ok)
	require.Equal(codes.InvalidArgument, st.Code())
}
