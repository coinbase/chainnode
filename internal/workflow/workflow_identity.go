package workflow

import (
	"encoding/json"
	"strings"

	"golang.org/x/xerrors"
)

type WorkflowIdentity int

const (
	UnknownIdentity WorkflowIdentity = iota
	CoordinatorIdentity
	IngestorIdentity
)

var workflowIdentityToString = map[WorkflowIdentity]string{
	CoordinatorIdentity: "workflow.coordinator",
	IngestorIdentity:    "workflow.ingestor",
}

var workflowIdentities = map[string]WorkflowIdentity{
	"coordinator": CoordinatorIdentity,
	"ingestor":    IngestorIdentity,
}

func GetWorkflowIdentify(name string) WorkflowIdentity {
	return workflowIdentities[name]
}

func (w WorkflowIdentity) String() (string, error) {
	workflowIdentityString, ok := workflowIdentityToString[w]
	if !ok {
		return "", xerrors.Errorf("unsupported workflow identity: %v", w)
	}
	return workflowIdentityString, nil
}

func (w WorkflowIdentity) UnmarshalJsonStringToRequest(str string) (interface{}, error) {
	if str == "" {
		str = "{}"
	}

	decoder := json.NewDecoder(strings.NewReader(str))
	decoder.DisallowUnknownFields()

	var err error
	switch w {
	case CoordinatorIdentity:
		var req CoordinatorRequest
		if err = decoder.Decode(&req); err == nil {
			return req, nil
		}
	case IngestorIdentity:
		var req IngestorRequest
		if err = decoder.Decode(&req); err == nil {
			return req, nil
		}
	default:
		err = xerrors.Errorf("unsupported workflow identity: %v", w)
	}
	return nil, err
}
