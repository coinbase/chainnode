package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/tag"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
)

const (
	// generator tests
	testTemplateFile              = "unit_test_templates/unit_test_template.txt"
	errorInterceptorExpected      = "tag/error_interceptor.txt"
	handlerExpected               = "tag/handler.txt"
	instrumentInterceptorExpected = "tag/instrument_interceptor.txt"
	namespaceExpected             = "tag/namespace.txt"
	validatorExpected             = "tag/validator.txt"
	formatterFile                 = "unit_test.go"

	// template ingestor tests
	ingestorTestcase1    = "unit_test_templates/ingestor_testcase1.txt"
	ingestorTestcase2    = "unit_test_templates/ingestor_testcase2.txt"
	namespaceError       = "TAG Template: template ingestor could not find namespace associated with proxy signature"
	ingestorTestcase3    = "unit_test_templates/ingestor_testcase3.txt"
	methodSignatureError = "TAG Template: method signature is not correctly defined. Closing parenthesis not detected in method signature."

	param1 = "param1"
	param2 = "param2"
	param3 = "param3"
	type1  = "type1{}"
	type2  = "type2{}"
	type3  = "type3{}"

	tc1ProxyApiName = "EthBoo"
	tc1ApiName      = "Boo"
	tc1JSONRPC      = "eth_boo"
	tc1Timeout      = "30 * time.Second"
	tc1Namespace    = "EthNamespace"

	tc2ProxyApiName = "NetLoo"
	tc2ApiName      = "Loo"
	tc2JSONRPC      = "net_loo"
	tc2Timeout      = "2 * time.Second"
	tc2Namespace    = "NetNamespace"

	tc3ProxyApiName = "Web3Too"
	tc3ApiName      = "Too"
	tc3JSONRPC      = "web3_too"
	tc3Timeout      = "2 * time.Second"
	tc3Namespace    = "Web3Namespace"
)

func formatter(input bytes.Buffer) string {
	tag.CreateAndWrite(input.String(), formatterFile)

	e := exec.Command("go", "fmt", formatterFile)
	err := e.Run()
	if err != nil {
		panic(err)
	}
	content, err := os.ReadFile(formatterFile)

	if err != nil {
		panic(err)
	}
	deleteErr := os.RemoveAll(formatterFile)
	if deleteErr != nil {
		panic(deleteErr)
	}

	return string(content)
}

func testErrorInterceptor(data []*inputParameters, expectedOutput string, t *testing.T) error {
	errorInterceptor := generateErrorInterceptor(data)
	err := errorCase(t, expectedOutput, formatter(errorInterceptor))
	return err
}

func testHandler(data []*inputParameters, expectedOutput string, t *testing.T) error {
	handler := generateHandler(data)
	err := errorCase(t, expectedOutput, formatter(handler))
	return err
}

func testInstrumentInterceptor(data []*inputParameters, expectedOutput string, t *testing.T) error {
	instrumentInterceptor := generateInstrumentInterceptor(data)
	err := errorCase(t, expectedOutput, formatter(instrumentInterceptor))
	return err
}

func testNamespace(data []*inputParameters, expectedOutput string, t *testing.T) error {
	namespace := generateNamespace(data)
	err := errorCase(t, expectedOutput, formatter(namespace))
	return err
}

func testValidator(data []*inputParameters, expectedOutput string, t *testing.T) error {
	validator := generateValidator(data)
	err := errorCase(t, expectedOutput, formatter(validator))
	return err
}

func errorCase(t *testing.T, expectedOutput string, actualOutput string) error {
	if actualOutput != expectedOutput {
		expectedFile, err := writeToDisk("expected", expectedOutput)
		require.NoError(t, err)
		actualFile, err := writeToDisk("actual", actualOutput)
		require.NoError(t, err)

		logger := zaptest.NewLogger(t)
		logger.Error(
			"responses are different",
			zap.String("expected", expectedFile),
			zap.String("actual", actualFile),
			zap.String("diff", fmt.Sprintf("vimdiff %v %v", expectedFile, actualFile)),
		)
		err = xerrors.Errorf("TAG unit test failed, expected does not equal to actual")
		return err
	}
	return nil
}

func writeToDisk(prefix string, response string) (string, error) {
	pattern := fmt.Sprintf("chainnode-%v-*.txt", prefix)
	file, err := ioutil.TempFile("", pattern)
	if err != nil {
		return "", xerrors.Errorf("failed to create temp file: %w", err)
	}

	_, err = file.WriteString(response)
	if err != nil {
		return "", xerrors.Errorf("failed to write to temp file: %w", err)
	}

	return file.Name(), nil
}

func ingestorErrorCase(readFile *os.File) ([]*inputParameters, error) {
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	data, err := templateIngestor(fileScanner, readFile)
	return data, err
}

func TestGenerator(t *testing.T) {
	data := readTemplate(testTemplateFile)
	expectedOutput := fixtures.MustReadTextFile(errorInterceptorExpected)

	err := testErrorInterceptor(data, expectedOutput, t)
	require.NoError(t, err)

	expectedOutput = fixtures.MustReadTextFile(handlerExpected)
	err = testHandler(data, expectedOutput, t)
	require.NoError(t, err)

	expectedOutput = fixtures.MustReadTextFile(instrumentInterceptorExpected)
	err = testInstrumentInterceptor(data, expectedOutput, t)
	require.NoError(t, err)

	expectedOutput = fixtures.MustReadTextFile(namespaceExpected)
	err = testNamespace(data, expectedOutput, t)
	require.NoError(t, err)

	expectedOutput = fixtures.MustReadTextFile(validatorExpected)
	err = testValidator(data, expectedOutput, t)
	require.NoError(t, err)
}

func TestTemplateIngestor(t *testing.T) {
	readFile, err := os.Open(ingestorTestcase1) // #nosec G304 - False positive: file paths are not based on user or external input
	require.NoError(t, err)
	data, err := ingestorErrorCase(readFile)
	require.NoError(t, err)

	var expected []*inputParameters
	proxyOne := inputParameters{
		ProxyApiName:  tc1ProxyApiName,
		ApiName:       tc1ApiName,
		JSONRPC:       tc1JSONRPC,
		Parameter:     []string{param1, param2},
		ParameterType: []string{type1, type2},
		Timeout:       tc1Timeout,
		Namespace:     tc1Namespace}
	expected = append(expected, &proxyOne)

	proxyTwo := inputParameters{
		ProxyApiName:  tc2ProxyApiName,
		ApiName:       tc2ApiName,
		JSONRPC:       tc2JSONRPC,
		Parameter:     []string{param1},
		ParameterType: []string{type1},
		Timeout:       tc2Timeout,
		Namespace:     tc2Namespace}
	expected = append(expected, &proxyTwo)

	proxyThree := inputParameters{
		ProxyApiName:  tc3ProxyApiName,
		ApiName:       tc3ApiName,
		JSONRPC:       tc3JSONRPC,
		Parameter:     []string{param1, param2, param3},
		ParameterType: []string{type1, type2, type3},
		Timeout:       tc3Timeout,
		Namespace:     tc3Namespace}
	expected = append(expected, &proxyThree)

	for x := range data {
		assert.Equal(t, expected[x].ProxyApiName, data[x].ProxyApiName)
		assert.Equal(t, expected[x].ApiName, data[x].ApiName)
		assert.Equal(t, expected[x].JSONRPC, data[x].JSONRPC)
		assert.Equal(t, expected[x].Parameter, data[x].Parameter)
		assert.Equal(t, expected[x].ParameterType, data[x].ParameterType)
		assert.Equal(t, expected[x].Timeout, data[x].Timeout)
		assert.Equal(t, expected[x].Namespace, data[x].Namespace)
	}

	readFile, err = os.Open(ingestorTestcase2) // #nosec G304 - False positive: file paths are not based on user or external input
	require.NoError(t, err)
	_, err = ingestorErrorCase(readFile)
	assert.EqualError(t, err, namespaceError)

	readFile, err = os.Open(ingestorTestcase3) // #nosec G304 - False positive: file paths are not based on user or external input
	require.NoError(t, err)
	_, err = ingestorErrorCase(readFile)
	assert.EqualError(t, err, methodSignatureError)
}
