package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/coinbase/chainnode/internal/utils/syncgroup"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/tag"
)

type inputParameters struct {
	ProxyApiName  string
	ApiName       string
	Parameter     []string
	ParameterType []string
	JSONRPC       string
	Namespace     string
	Timeout       string
}

const (
	contextTemplate           = "ctx context.Context"
	contextConst              = "context.Context"
	defaultNamespace          = "EthNamespace"
	handlerPackageName        = "handler"
	defaultInterfaceName      = "interface{}"
	nameSpaceConstant         = "Namespace"
	defaultTimeoutValue       = "5"
	ethereumDocumentationLink = "https://ethereum.org/en/developers/docs/apis/json-rpc/#"

	// constant filepath for text/template files
	proxyReceiverCodeTmpl           = "code_templates/handler/proxyreceiver/proxyreceiver_code.tmpl"
	jsonrpcMethodVarTmpl            = "code_templates/handler/jsonrpc_request/jsonrpc_method_var.tmpl"
	handlerMethodTmpl               = "code_templates/handler/handler_structure/handler_method.tmpl"
	handlerGlobalTmpl               = "code_templates/handler/handler_global.tmpl"
	errorInterceptorMethodTmpl      = "code_templates/error_interceptor/error_interceptor_structure/error_interceptor_method.tmpl"
	errorInterceptorGlobalTmpl      = "code_templates/error_interceptor/error_interceptor_global.tmpl"
	namespaceTmpl                   = "code_templates/namespace/namespace_structure/namespace.tmpl"
	namespaceGlobalTmpl             = "code_templates/namespace/namespace_global.tmpl"
	validatorTmpl                   = "code_templates/validator/validator_structure/validator.tmpl"
	validatorGlobalTmpl             = "code_templates/validator/validator_global.tmpl"
	proxyMetricsCodeTmpl            = "code_templates/instrument_interceptor/proxymetrics/proxymetrics_code.tmpl"
	instrumentInterceptorTmpl       = "code_templates/instrument_interceptor/instrument_interceptor_structure/instrument_interceptor.tmpl"
	instrumentInterceptorGlobalTmpl = "code_templates/instrument_interceptor/instrument_interceptor_global.tmpl"

	// constants for TAG filepath
	tagTemplate                  = "internal/tag_generator/tag_template.txt"
	tagFilepath                  = "internal/controller/ethereum/handler/"
	errorInterceptorTagFile      = "error_interceptor_tag_generated.go"
	handlerTagFile               = "handler_tag_generated.go"
	instrumentInterceptorTagFile = "instrument_interceptor_tag_generated.go"
	namespaceTagFile             = "namespace_tag_generated.go"
	validatorTagFile             = "validator_tag_generated.go"
)

func readTemplate(fileName string) []*inputParameters {
	readFile, err := os.Open(fileName) // #nosec G304 - False positive: file paths are not based on user or external input
	if err != nil {
		panic(err)
	}

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	data, err := templateIngestor(fileScanner, readFile)
	if err != nil {
		panic(err)
	}
	return data
}

func templateIngestor(fileScanner *bufio.Scanner, readFile *os.File) ([]*inputParameters, error) {
	deleteEmpty := func(s []string) []string {
		var res []string
		for _, str := range s {
			if len(str) > 0 {
				res = append(res, strings.TrimSpace(str))
			}
		}
		return res
	}

	numInstance := func(s string, targetChar string) int {
		count := 0
		for _, x := range s {
			if string(x) == targetChar {
				count = count + 1
			}
		}
		return count
	}

	var namespace string
	var data []*inputParameters

	for fileScanner.Scan() {
		var Parameter []string
		var ParameterType []string
		newline := strings.TrimSpace(fileScanner.Text())

		if len(newline) != 0 && string(newline[0]) == "#" { //catch comments
			continue
		} else if len(newline) != 0 && string(newline[0]) == "@" { //catch namespace
			namespace = newline[1:]
		} else if len(newline) != 0 { //catch proxy lines
			if len(namespace) == 0 {
				err := errors.New("TAG Template: template ingestor could not find namespace associated with proxy signature")
				return nil, err
			}
			timeoutParse := deleteEmpty(strings.Split(newline, "//"))
			var timeout string
			if len(timeoutParse) == 2 {
				timeout = timeoutParse[1]
			} else {
				timeout = defaultTimeoutValue
			}
			if numInstance(newline, ")") < 2 {
				err := errors.New("TAG Template: method signature is not correctly defined. Closing parenthesis not detected in method signature.")
				return nil, err
			}
			methodInformation := strings.Split(strings.ReplaceAll(newline, ")", ""), "(")
			apiName := methodInformation[0]
			if len(methodInformation) > 0 {
				apiParameter := strings.Split(methodInformation[1], ",")
				for x := range apiParameter {
					slice := strings.Split(strings.TrimSpace(apiParameter[x]), " ")
					Parameter = append(Parameter, slice[0])
					ParameterType = append(ParameterType, slice[1])
				}
			}
			var filteredParameter []string
			var filteredParameterType []string
			for x := range Parameter {
				if Parameter[x] != "ctx" {
					filteredParameter = append(filteredParameter, Parameter[x])
				}
			}
			containsNonDefault := false
			for x := range ParameterType {
				if ParameterType[x] != defaultInterfaceName && ParameterType[x] != contextConst {
					containsNonDefault = true
					break
				}
			}
			if containsNonDefault {
				for x := range ParameterType {
					if ParameterType[x] == contextConst && x == 0 {
						continue
					} else if ParameterType[x] != defaultInterfaceName && ParameterType[x] != contextConst {
						filteredParameterType = append(filteredParameterType, ParameterType[x])
					} else {
						filteredParameterType = append(filteredParameterType, "")
					}
				}
			}
			newProxy := inputParameters{ProxyApiName: fmt.Sprintf("%s%s", namespace, apiName),
				ApiName:       apiName,
				JSONRPC:       fmt.Sprintf("%s_%s", strings.ToLower(namespace), strings.ToLower(apiName[0:1])+apiName[1:]),
				Parameter:     filteredParameter,
				ParameterType: filteredParameterType,
				Timeout:       fmt.Sprintf("%s * time.Second", timeout),
				Namespace:     fmt.Sprintf("%s%s", namespace, nameSpaceConstant)}
			data = append(data, &newProxy)
		}
	}
	err := readFile.Close()
	if err != nil {
		panic(err)
	}
	return data, nil
}

func parseParametersAndTypes(parameters []string, parameterTypes []string) string {
	// returns string consisting of Parameters + Parameter Type; e.g address interface{} ...
	if len(parameters) == len(parameterTypes) || len(parameterTypes) == 0 {
		res := make([]string, len(parameters)+1)
		res[0] = contextTemplate
		for i := range parameters {
			parameterType := defaultInterfaceName
			if i < len(parameterTypes) {
				if parameterTypes[i] != "" {
					parameterType = parameterTypes[i]
				}
			}
			res[i+1] = fmt.Sprintf("%s %s", parameters[i], parameterType)
		}
		return strings.Join(res, ",")
	}
	err := xerrors.Errorf("encountered error with method parseParametersAndTypes, length of parameter/type of different length")
	panic(err)
}

func parseParameters(parameters []string) string {
	// returns string consisting of just Parameters; e.g address, key, blockNrOrHash ...
	res := strings.Join(parameters, ",")
	if len(res) != 0 {
		res = "ctx, " + res
	} else {
		res = "ctx"
	}
	return res
}

func groupByNamespace(data []*inputParameters) (map[string][]*inputParameters, []string) {
	res := map[string][]*inputParameters{}
	for i := 0; i < len(data); i++ {
		if data[i].Namespace == "" {
			data[i].Namespace = defaultNamespace
		}
		if _, ok := res[data[i].Namespace]; ok {
			res[data[i].Namespace] = append(res[data[i].Namespace], data[i])
		} else {
			res[data[i].Namespace] = []*inputParameters{data[i]}
		}
	}
	keys := make([]string, 0, len(res))
	for k := range res {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return res, keys
}

func generateHandler(data []*inputParameters) bytes.Buffer {
	keyValue := func() []string {
		res := make([]string, len(data))
		for i := range data {
			res[i] = data[i].ProxyApiName + ".Name"
		}
		return res
	}

	repeatingArray := func(times int, value string) []string {
		s := make([]string, times)
		for i := range s {
			s[i] = value
		}
		return s
	}

	// handler code generation
	b, w := tag.Init()
	globalTemplate := make(map[string]interface{})
	w.GenerateAutomatedComment()
	w.CreatePackage(handlerPackageName)

	// section: ProxyReceiver
	groupedData, sortedKeys := groupByNamespace(data)
	proxyReceiverCode := ""
	for _, key := range sortedKeys {
		element := groupedData[key]
		proxyReceiverCode = proxyReceiverCode + fmt.Sprintf("\n\t\t// methods from %s \n", key)
		for i := range element {
			vars := make(map[string]interface{})
			vars["MethodName"] = element[i].ApiName
			vars["ParametersAndTypes"] = parseParametersAndTypes(element[i].Parameter, element[i].ParameterType)
			proxyReceiverCode = proxyReceiverCode + tag.ProcessTemplate(proxyReceiverCodeTmpl, vars)
		}
	}
	globalTemplate["ProxyReceivers"] = proxyReceiverCode

	// section: jsonrpc request method
	jsonRPCCode := ""
	for i := range data {
		vars := make(map[string]interface{})
		vars["ApiName"] = data[i].ProxyApiName
		vars["JSONRPC"] = data[i].JSONRPC
		vars["Timeout"] = data[i].Timeout
		jsonRPCCode = jsonRPCCode + tag.ProcessTemplate(jsonrpcMethodVarTmpl, vars)
	}
	globalTemplate["JSONRPCRequest"] = jsonRPCCode

	//section: ProxyMethodMap
	proxyMethodMapBuffer := (tag.CreateMap("proxyMethods", "string", "bool", keyValue(), repeatingArray(len(data), "true")))
	globalTemplate["ProxyMethodMap"] = proxyMethodMapBuffer.String()

	// section: handler
	handlerCode := ""
	for i := range data {
		handlerCode = handlerCode + fmt.Sprintf("//%s implements %s%s \n", data[i].ApiName, ethereumDocumentationLink, strings.ToLower(data[i].JSONRPC))
		vars := make(map[string]interface{})
		vars["MethodName"] = data[i].ApiName
		vars["ParametersAndTypes"] = parseParametersAndTypes(data[i].Parameter, data[i].ParameterType)
		if len(data[i].Parameter) == 0 {
			vars["ReturnStatement"] = fmt.Sprintf("r.proxy(ctx, %s, nil)", data[i].ProxyApiName)
		} else {
			vars["ReturnStatement"] = fmt.Sprintf("r.proxy(ctx, %s, jsonrpc.Params{%s})", data[i].ProxyApiName, strings.Join(data[i].Parameter, ","))
		}
		handlerCode = handlerCode + tag.ProcessTemplate(handlerMethodTmpl, vars)
	}
	globalTemplate["Handlers"] = handlerCode

	// write code to handler file
	fileCode := tag.ProcessTemplate(handlerGlobalTmpl, globalTemplate)
	w.Wf(fileCode)
	return *b
}

func generateErrorInterceptor(data []*inputParameters) bytes.Buffer {
	parseReturn := func(parameter []string) string {
		parameter = append([]string{"ctx"}, parameter...)
		return strings.Join(parameter, ",")
	}

	// error interceptor code generation
	b, w := tag.Init()
	globalTemplate := make(map[string]interface{})
	w.GenerateAutomatedComment()
	w.CreatePackage(handlerPackageName)

	// section: error interceptor
	errorInterceptor := ""
	for i := range data {
		vars := make(map[string]interface{})
		vars["MethodName"] = data[i].ApiName
		vars["ParametersAndTypes"] = parseParametersAndTypes(data[i].Parameter, data[i].ParameterType)
		vars["Parameters"] = parseReturn(data[i].Parameter)
		errorInterceptor = errorInterceptor + tag.ProcessTemplate(errorInterceptorMethodTmpl, vars)
	}
	globalTemplate["ErrorInterceptors"] = errorInterceptor

	// write code to error interceptor file
	fileCode := tag.ProcessTemplate(errorInterceptorGlobalTmpl, globalTemplate)
	w.Wf(fileCode)

	return *b
}

func generateInstrumentInterceptor(data []*inputParameters) bytes.Buffer {
	// instrument interceptor code generation
	b, w := tag.Init()
	globalTemplate := make(map[string]interface{})
	w.GenerateAutomatedComment()
	w.CreatePackage(handlerPackageName)

	// section: proxyMetric struct
	proxyMetricCode := ""
	for i := range data {
		proxyMetricCode = proxyMetricCode + fmt.Sprintf("\ninstrument%s instrument.Call", data[i].ApiName)
	}
	globalTemplate["ProxyMetrics"] = proxyMetricCode

	// section: newProxyMetrics
	newProxyMetricCode := ""
	for i := range data {
		vars := make(map[string]interface{})
		vars["MethodName"] = data[i].ApiName
		vars["ProxyAPIName"] = data[i].ProxyApiName
		newProxyMetricCode = newProxyMetricCode + tag.ProcessTemplate(proxyMetricsCodeTmpl, vars)
	}
	globalTemplate["NewProxyMetrics"] = newProxyMetricCode

	// section: instrument interceptor
	instrumentInterceptorCode := ""
	for i := range data {
		vars := make(map[string]interface{})
		vars["MethodName"] = data[i].ApiName
		vars["ParametersAndTypes"] = parseParametersAndTypes(data[i].Parameter, data[i].ParameterType)
		vars["AllParameters"] = parseParameters(data[i].Parameter)
		if len(data[i].Parameter) == 0 {
			vars["Parameters"] = "nil"
		} else {
			vars["Parameters"] = fmt.Sprintf("jsonrpc.Params{%s}", strings.Join(data[i].Parameter, ","))
		}
		instrumentInterceptorCode = instrumentInterceptorCode + tag.ProcessTemplate(instrumentInterceptorTmpl, vars)
	}
	globalTemplate["InstrumentInterceptors"] = instrumentInterceptorCode

	// write code to error interceptor file
	fileCode := tag.ProcessTemplate(instrumentInterceptorGlobalTmpl, globalTemplate)
	w.Wf(fileCode)

	return *b
}

func generateNamespace(data []*inputParameters) bytes.Buffer {
	// namespace code generation
	b, w := tag.Init()
	globalTemplate := make(map[string]interface{})
	w.GenerateAutomatedComment()
	w.CreatePackage(handlerPackageName)
	groupedData, sortedKeys := groupByNamespace(data)

	// section: namespace
	namespace := ""
	for _, key := range sortedKeys {
		element := groupedData[key]
		namespace = namespace + fmt.Sprintf("// %s \n", key)
		for i := range element {
			vars := make(map[string]interface{})
			vars["Namespace"] = element[i].Namespace
			vars["MethodName"] = element[i].ApiName
			vars["ParametersAndTypes"] = parseParametersAndTypes(element[i].Parameter, element[i].ParameterType)
			vars["Parameters"] = parseParameters(element[i].Parameter)
			namespace = namespace + tag.ProcessTemplate(namespaceTmpl, vars)
		}
	}
	globalTemplate["Namespaces"] = namespace

	// write code to namespace file
	fileCode := tag.ProcessTemplate(namespaceGlobalTmpl, globalTemplate)
	w.Wf(fileCode)

	return *b
}

func generateValidator(data []*inputParameters) bytes.Buffer {
	// validator code generation
	b, w := tag.Init()
	globalTemplate := make(map[string]interface{})
	w.GenerateAutomatedComment()
	w.CreatePackage(handlerPackageName)

	// section: validator
	validator := ""
	for i := range data {
		vars := make(map[string]interface{})
		vars["MethodName"] = data[i].ApiName
		vars["ParametersAndTypes"] = parseParametersAndTypes(data[i].Parameter, data[i].ParameterType)
		vars["Parameters"] = parseParameters(data[i].Parameter)
		validator = validator + tag.ProcessTemplate(validatorTmpl, vars)
	}
	globalTemplate["Validators"] = validator

	// write code to namespace file
	fileCode := tag.ProcessTemplate(validatorGlobalTmpl, globalTemplate)
	w.Wf(fileCode)

	return *b
}

func runErrorInterceptor(data []*inputParameters, filename string) {
	errorInterceptor := generateErrorInterceptor(data)
	tag.CreateAndWrite(errorInterceptor.String(), filename)
}

func runHandler(data []*inputParameters, filename string) {
	handler := generateHandler(data)
	tag.CreateAndWrite(handler.String(), filename)
}

func runInstrumentInterceptor(data []*inputParameters, filename string) {
	instrumentInterceptor := generateInstrumentInterceptor(data)
	tag.CreateAndWrite(instrumentInterceptor.String(), filename)
}

func runNamespace(data []*inputParameters, filename string) {
	namespace := generateNamespace(data)
	tag.CreateAndWrite(namespace.String(), filename)
}

func runValidator(data []*inputParameters, filename string) {
	validator := generateValidator(data)
	tag.CreateAndWrite(validator.String(), filename)
}

func executeGenerator(template string, filepath string, errorInterceptor string, handler string, instrumentInterceptor string, namespace string, validator string) {
	//read template
	data := readTemplate(template)

	ctx := context.Background()
	group, _ := syncgroup.New(ctx)

	// generate proxy code
	group.Go(func() error {
		runErrorInterceptor(data, fmt.Sprintf("%s%s", filepath, errorInterceptor))
		return nil
	})
	group.Go(func() error {
		runHandler(data, fmt.Sprintf("%s%s", filepath, handler))
		return nil
	})
	group.Go(func() error {
		runInstrumentInterceptor(data, fmt.Sprintf("%s%s", filepath, instrumentInterceptor))
		return nil
	})
	group.Go(func() error {
		runNamespace(data, fmt.Sprintf("%s%s", filepath, namespace))
		return nil
	})
	group.Go(func() error {
		runValidator(data, fmt.Sprintf("%s%s", filepath, validator))
		return nil
	})

	err := group.Wait()
	if err != nil {
		panic("failed to complete generation task")
	}
}

func main() {
	executeGenerator(tagTemplate, tagFilepath, errorInterceptorTagFile, handlerTagFile, instrumentInterceptorTagFile, namespaceTagFile, validatorTagFile)
}
