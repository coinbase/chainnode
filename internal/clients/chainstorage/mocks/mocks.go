// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/coinbase/chainnode/internal/clients/chainstorage (interfaces: Client,Parser)

// Package chainstoragemocks is a generated GoMock package.
package chainstoragemocks

import (
	context "context"
	reflect "reflect"

	chainstorage "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
	sdk "github.com/coinbase/chainstorage/sdk"
	gomock "github.com/golang/mock/gomock"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// GetBlock mocks base method.
func (m *MockClient) GetBlock(arg0 context.Context, arg1 uint64, arg2 string) (*chainstorage.Block, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBlock", arg0, arg1, arg2)
	ret0, _ := ret[0].(*chainstorage.Block)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBlock indicates an expected call of GetBlock.
func (mr *MockClientMockRecorder) GetBlock(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBlock", reflect.TypeOf((*MockClient)(nil).GetBlock), arg0, arg1, arg2)
}

// GetBlockWithTag mocks base method.
func (m *MockClient) GetBlockWithTag(arg0 context.Context, arg1 uint32, arg2 uint64, arg3 string) (*chainstorage.Block, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBlockWithTag", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(*chainstorage.Block)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBlockWithTag indicates an expected call of GetBlockWithTag.
func (mr *MockClientMockRecorder) GetBlockWithTag(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBlockWithTag", reflect.TypeOf((*MockClient)(nil).GetBlockWithTag), arg0, arg1, arg2, arg3)
}

// GetBlocksByRange mocks base method.
func (m *MockClient) GetBlocksByRange(arg0 context.Context, arg1, arg2 uint64) ([]*chainstorage.Block, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBlocksByRange", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*chainstorage.Block)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBlocksByRange indicates an expected call of GetBlocksByRange.
func (mr *MockClientMockRecorder) GetBlocksByRange(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBlocksByRange", reflect.TypeOf((*MockClient)(nil).GetBlocksByRange), arg0, arg1, arg2)
}

// GetBlocksByRangeWithTag mocks base method.
func (m *MockClient) GetBlocksByRangeWithTag(arg0 context.Context, arg1 uint32, arg2, arg3 uint64) ([]*chainstorage.Block, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBlocksByRangeWithTag", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]*chainstorage.Block)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBlocksByRangeWithTag indicates an expected call of GetBlocksByRangeWithTag.
func (mr *MockClientMockRecorder) GetBlocksByRangeWithTag(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBlocksByRangeWithTag", reflect.TypeOf((*MockClient)(nil).GetBlocksByRangeWithTag), arg0, arg1, arg2, arg3)
}

// GetChainEvents mocks base method.
func (m *MockClient) GetChainEvents(arg0 context.Context, arg1 *chainstorage.GetChainEventsRequest) ([]*chainstorage.BlockchainEvent, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetChainEvents", arg0, arg1)
	ret0, _ := ret[0].([]*chainstorage.BlockchainEvent)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetChainEvents indicates an expected call of GetChainEvents.
func (mr *MockClientMockRecorder) GetChainEvents(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetChainEvents", reflect.TypeOf((*MockClient)(nil).GetChainEvents), arg0, arg1)
}

// GetChainMetadata mocks base method.
func (m *MockClient) GetChainMetadata(arg0 context.Context, arg1 *chainstorage.GetChainMetadataRequest) (*chainstorage.GetChainMetadataResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetChainMetadata", arg0, arg1)
	ret0, _ := ret[0].(*chainstorage.GetChainMetadataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetChainMetadata indicates an expected call of GetChainMetadata.
func (mr *MockClientMockRecorder) GetChainMetadata(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetChainMetadata", reflect.TypeOf((*MockClient)(nil).GetChainMetadata), arg0, arg1)
}

// GetClientID mocks base method.
func (m *MockClient) GetClientID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClientID")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetClientID indicates an expected call of GetClientID.
func (mr *MockClientMockRecorder) GetClientID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClientID", reflect.TypeOf((*MockClient)(nil).GetClientID))
}

// GetLatestBlock mocks base method.
func (m *MockClient) GetLatestBlock(arg0 context.Context) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLatestBlock", arg0)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetLatestBlock indicates an expected call of GetLatestBlock.
func (mr *MockClientMockRecorder) GetLatestBlock(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLatestBlock", reflect.TypeOf((*MockClient)(nil).GetLatestBlock), arg0)
}

// GetStaticChainMetadata mocks base method.
func (m *MockClient) GetStaticChainMetadata(arg0 context.Context, arg1 *chainstorage.GetChainMetadataRequest) (*chainstorage.GetChainMetadataResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStaticChainMetadata", arg0, arg1)
	ret0, _ := ret[0].(*chainstorage.GetChainMetadataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStaticChainMetadata indicates an expected call of GetStaticChainMetadata.
func (mr *MockClientMockRecorder) GetStaticChainMetadata(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStaticChainMetadata", reflect.TypeOf((*MockClient)(nil).GetStaticChainMetadata), arg0, arg1)
}

// GetTag mocks base method.
func (m *MockClient) GetTag() uint32 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTag")
	ret0, _ := ret[0].(uint32)
	return ret0
}

// GetTag indicates an expected call of GetTag.
func (mr *MockClientMockRecorder) GetTag() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTag", reflect.TypeOf((*MockClient)(nil).GetTag))
}

// SetClientID mocks base method.
func (m *MockClient) SetClientID(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetClientID", arg0)
}

// SetClientID indicates an expected call of SetClientID.
func (mr *MockClientMockRecorder) SetClientID(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetClientID", reflect.TypeOf((*MockClient)(nil).SetClientID), arg0)
}

// SetTag mocks base method.
func (m *MockClient) SetTag(arg0 uint32) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetTag", arg0)
}

// SetTag indicates an expected call of SetTag.
func (mr *MockClientMockRecorder) SetTag(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTag", reflect.TypeOf((*MockClient)(nil).SetTag), arg0)
}

// StreamChainEvents mocks base method.
func (m *MockClient) StreamChainEvents(arg0 context.Context, arg1 sdk.StreamingConfiguration) (<-chan *sdk.ChainEventResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StreamChainEvents", arg0, arg1)
	ret0, _ := ret[0].(<-chan *sdk.ChainEventResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StreamChainEvents indicates an expected call of StreamChainEvents.
func (mr *MockClientMockRecorder) StreamChainEvents(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StreamChainEvents", reflect.TypeOf((*MockClient)(nil).StreamChainEvents), arg0, arg1)
}

// MockParser is a mock of Parser interface.
type MockParser struct {
	ctrl     *gomock.Controller
	recorder *MockParserMockRecorder
}

// MockParserMockRecorder is the mock recorder for MockParser.
type MockParserMockRecorder struct {
	mock *MockParser
}

// NewMockParser creates a new mock instance.
func NewMockParser(ctrl *gomock.Controller) *MockParser {
	mock := &MockParser{ctrl: ctrl}
	mock.recorder = &MockParserMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockParser) EXPECT() *MockParserMockRecorder {
	return m.recorder
}

// ParseNativeBlock mocks base method.
func (m *MockParser) ParseNativeBlock(arg0 context.Context, arg1 *chainstorage.Block) (*chainstorage.NativeBlock, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ParseNativeBlock", arg0, arg1)
	ret0, _ := ret[0].(*chainstorage.NativeBlock)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ParseNativeBlock indicates an expected call of ParseNativeBlock.
func (mr *MockParserMockRecorder) ParseNativeBlock(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ParseNativeBlock", reflect.TypeOf((*MockParser)(nil).ParseNativeBlock), arg0, arg1)
}

// ParseRosettaBlock mocks base method.
func (m *MockParser) ParseRosettaBlock(arg0 context.Context, arg1 *chainstorage.Block) (*chainstorage.RosettaBlock, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ParseRosettaBlock", arg0, arg1)
	ret0, _ := ret[0].(*chainstorage.RosettaBlock)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ParseRosettaBlock indicates an expected call of ParseRosettaBlock.
func (mr *MockParserMockRecorder) ParseRosettaBlock(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ParseRosettaBlock", reflect.TypeOf((*MockParser)(nil).ParseRosettaBlock), arg0, arg1)
}
