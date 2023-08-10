package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/uber-go/tally/v4"
	"google.golang.org/grpc/metadata"

	"github.com/coinbase/chainnode/internal/config"
)

type (
	HttpTestServer struct {
		rpcServer *Server
	}
)

const (
	unknownClientID = "unknown"
)

func confirmStatusCode(t *testing.T, got, want int) {
	t.Helper()
	if got == want {
		return
	}
	if gotName := http.StatusText(got); len(gotName) > 0 {
		if wantName := http.StatusText(want); len(wantName) > 0 {
			t.Fatalf("response status code: got %d (%s), want %d (%s)", got, gotName, want, wantName)
		}
	}
	t.Fatalf("response status code: got %d, want %d", got, want)
}

func confirmRequestValidationCode(t *testing.T, method, contentType, body string, expectedStatusCode int) {
	t.Helper()
	request := httptest.NewRequest(method, "http://url.com", strings.NewReader(body))
	if len(contentType) > 0 {
		request.Header.Set("Content-Type", contentType)
	}
	code, err := validateRequest(request)
	if code == 0 {
		if err != nil {
			t.Errorf("validation: got error %v, expected nil", err)
		}
	} else if err == nil {
		t.Errorf("validation: code %d: got nil, expected error", code)
	}
	confirmStatusCode(t, code, expectedStatusCode)
}

func TestHTTPErrorResponseWithDelete(t *testing.T) {
	confirmRequestValidationCode(t, http.MethodDelete, contentType, "", http.StatusMethodNotAllowed)
}

func TestHTTPErrorResponseWithPut(t *testing.T) {
	confirmRequestValidationCode(t, http.MethodPut, contentType, "", http.StatusMethodNotAllowed)
}

func TestHTTPErrorResponseWithMaxContentLength(t *testing.T) {
	body := make([]rune, maxRequestContentLength+1)
	confirmRequestValidationCode(t,
		http.MethodPost, contentType, string(body), http.StatusRequestEntityTooLarge)
}

func TestHTTPErrorResponseWithEmptyContentType(t *testing.T) {
	confirmRequestValidationCode(t, http.MethodPost, "", "", http.StatusUnsupportedMediaType)
}

func TestHTTPErrorResponseWithValidRequest(t *testing.T) {
	confirmRequestValidationCode(t, http.MethodPost, contentType, "", 0)
}

func confirmHTTPRequestYieldsStatusCode(t *testing.T, method, contentType, body string, expectedStatusCode int) {
	t.Helper()
	s := Server{}
	ts := httptest.NewServer(&s)
	defer ts.Close()

	request, err := http.NewRequest(method, ts.URL, strings.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create a valid HTTP request: %v", err)
	}
	if len(contentType) > 0 {
		request.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	confirmStatusCode(t, resp.StatusCode, expectedStatusCode)
}

func TestHTTPResponseWithEmptyGet(t *testing.T) {
	confirmHTTPRequestYieldsStatusCode(t, http.MethodGet, "", "", http.StatusOK)
}

// This checks that maxRequestContentLength is not applied to the response of a request.
func TestHTTPRespBodyUnlimited(t *testing.T) {
	const respLength = maxRequestContentLength * 3
	scope := tally.NewTestScope("", nil)
	s := NewServer(ServerParams{BatchLimitConfig: config.BatchLimitConfig{}, Scope: scope})
	defer s.Stop()
	s.RegisterName("test", largeRespService{respLength})
	ts := httptest.NewServer(s)
	defer ts.Close()

	c, err := DialHTTP(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var r string
	if err := c.Call(&r, "test_largeResp"); err != nil {
		t.Fatal(err)
	}
	if len(r) != respLength {
		t.Fatalf("response has wrong length %d, want %d", len(r), respLength)
	}

	if err := verifyMetrics(scope, 0, 0, 1); err != nil {
		t.Fatal(err)
	}
}

// Tests that an HTTP error results in an HTTPError instance
// being returned with the expected attributes.
func TestHTTPErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "error has occurred!", http.StatusTeapot)
	}))
	defer ts.Close()

	c, err := DialHTTP(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	var r string
	err = c.Call(&r, "test_method")
	if err == nil {
		t.Fatal("error was expected")
	}

	httpErr, ok := err.(HTTPError)
	if !ok {
		t.Fatalf("unexpected error type %T", err)
	}

	if httpErr.StatusCode != http.StatusTeapot {
		t.Error("unexpected status code", httpErr.StatusCode)
	}
	if httpErr.Status != "418 I'm a teapot" {
		t.Error("unexpected status text", httpErr.Status)
	}
	if body := string(httpErr.Body); body != "error has occurred!\n" {
		t.Error("unexpected body", body)
	}

	if errMsg := httpErr.Error(); errMsg != "418 I'm a teapot: error has occurred!\n" {
		t.Error("unexpected error message", errMsg)
	}
}

func TestHTTPPeerInfo(t *testing.T) {
	s := newTestServer()
	defer s.Stop()
	ts := httptest.NewServer(s)
	defer ts.Close()

	c, err := Dial(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	c.SetHeader("user-agent", "ua-testing")
	c.SetHeader("origin", "origin.example.com")

	// Request peer information.
	var info PeerInfo
	if err := c.Call(&info, "test_peerInfo"); err != nil {
		t.Fatal(err)
	}

	if info.RemoteAddr == "" {
		t.Error("RemoteAddr not set")
	}
	if info.Transport != "http" {
		t.Errorf("wrong Transport %q", info.Transport)
	}
	if info.HTTP.Version != "HTTP/1.1" {
		t.Errorf("wrong HTTP.Version %q", info.HTTP.Version)
	}
	if info.HTTP.UserAgent != "ua-testing" {
		t.Errorf("wrong HTTP.UserAgent %q", info.HTTP.UserAgent)
	}
	if info.HTTP.Origin != "origin.example.com" {
		t.Errorf("wrong HTTP.Origin %q", info.HTTP.UserAgent)
	}
}

func TestHTTPBatchRequestLimit(t *testing.T) {
	numBatchCalls := 0
	batchLimitConfig := config.BatchLimitConfig{
		DefaultLimit: 3,
	}
	scope := tally.NewTestScope("", nil)
	s := newHttpTestServer(ServerParams{BatchLimitConfig: batchLimitConfig, Scope: scope})
	defer s.Stop()
	s.RegisterName("test", new(testService))

	ts := httptest.NewServer(s)
	defer ts.Close()

	c, err := DialHTTP(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	testCases := map[string]struct {
		header           string
		numMsgs          int
		expectedNumResp  int
		expectedErrorMsg string
	}{
		"no header provided should fail default case": {
			header:           "",
			numMsgs:          4,
			expectedNumResp:  0,
			expectedErrorMsg: "maximum allowed batch size 3",
		},

		"no header provided succeeds": {
			header:          "",
			numMsgs:         3,
			expectedNumResp: 3,
		},
	}

	for testName, tc := range testCases {
		tc := tc
		t.Run(testName, func(t *testing.T) {
			msgs := createBatchMsgs(tc.numMsgs)

			if tc.expectedNumResp > 0 {
				s := "hello"
				num := 2
				arg := "world"
				batch := createBatchElems(tc.numMsgs, s, num, arg)

				c.BatchCall(batch)

				for _, item := range batch {
					result := item.Result.(*echoResult)
					if result.String != s || result.Int != num || result.Args.S != arg {
						t.Fatal("unexpected echo result")
					}
				}
			} else {
				hc := c.writeConn.(*httpConn)
				ctx := context.Background()
				respBody, err := hc.doRequest(ctx, msgs)
				if err != nil {
					t.Fatal(err)
				}
				defer respBody.Close()

				var respmsg jsonrpcMessage
				if err := json.NewDecoder(respBody).Decode(&respmsg); err != nil {
					t.Fatal(err)
				}

				if respmsg.Error.Message != tc.expectedErrorMsg {
					t.Fatal("unexpected error")
				}
			}

			// Verify metrics if batch call succeeds
			if tc.expectedErrorMsg == "" {
				numBatchCalls++
				if err := verifyMetrics(scope, int64(numBatchCalls), float64(tc.expectedNumResp), 0); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func newHttpTestServer(params ServerParams) *HttpTestServer {
	return &HttpTestServer{
		rpcServer: NewServer(params),
	}
}

func (s *HttpTestServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := contextWithClientId(request)
	s.rpcServer.ServeHTTP(writer, request.WithContext(ctx))
	return
}

func (s *HttpTestServer) RegisterName(name string, receiver interface{}) error {
	return s.rpcServer.RegisterName(name, receiver)
}

func (s *HttpTestServer) Stop() {
	s.rpcServer.Stop()
}

func contextWithClientId(request *http.Request) context.Context {
	md := make(metadata.MD)
	ctx := metadata.NewIncomingContext(request.Context(), md)

	for k, v := range request.Header {
		k = strings.ToLower(k)
		md[k] = v
	}

	return ctx
}

func createBatchMsgs(numOfMsgs int) []jsonrpcMessage {
	msgs := make([]jsonrpcMessage, numOfMsgs)

	for i := 0; i < numOfMsgs; i++ {
		msgs[i] = jsonrpcMessage{
			Method: "test_echo",
		}
	}

	return msgs
}

func createBatchElems(numElems int, s string, num int, arg string) []BatchElem {
	elems := make([]BatchElem, numElems)

	for i := 0; i < numElems; i++ {
		elems[i] = BatchElem{
			Method: "test_echo",
			Args:   []interface{}{s, num, &echoArgs{arg}},
			Result: new(echoResult),
		}
	}

	return elems
}

func verifyMetrics(scope tally.TestScope, expectedBatchRequestCount int64, expectedBatchSize float64, expectedSingleRequestCount int64) error {
	snapshot := scope.Snapshot()
	batchRequestCountKey := rpcServerScope + ".request+mode=batch"
	batchRequestSizeKey := rpcServerScope + ".request.size+mode=batch"
	singleRequestCountKey := rpcServerScope + ".request+mode=single"

	if snapshot.Counters() == nil || snapshot.Gauges() == nil ||
		snapshot.Counters()[batchRequestCountKey].Value() != expectedBatchRequestCount ||
		snapshot.Gauges()[batchRequestSizeKey].Value() != expectedBatchSize ||
		snapshot.Counters()[singleRequestCountKey].Value() != expectedSingleRequestCount {
		return errors.New("metric validation failed")
	}

	return nil
}
