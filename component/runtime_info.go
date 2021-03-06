// Package component ...
package component

import (
	"encoding/json"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	pb "github.com/neuromage/kfp-launcher/third_party/ml_metadata"
)

type inputParameter struct {
	ParameterType  string
	ParameterValue string
}

type inputArtifact struct {
	// Where to read MLMD artifact. File is passed using Argo artifacts.
	FileInputPath string

	// The MLMD artifact.
	Artifact *pb.Artifact `json:"-"`

	// Generated by launcher.
	// /tmp/launcher_component_inputs/<name>/data
	LocalArtifactFilePath string `json:"-"`
}

type outputParameter struct {
	// ParameterType should be one of "INT", "STRING" or "DOUBLE".
	ParameterType  string
	FileOutputPath string
}

type outputArtifact struct {
	ArtifactSchema string
	// Where to write MLMD artifact.
	FileOutputPath string

	// Generated by launcher.
	// /tmp/launcher_component_outputs/<name>/data
	LocalArtifactFilePath string `json:"-"`
	// Final location of file.
	// <pipeline_root>/<pipelineName>/<pipelineRunID>/<pipelineTaskID>/data
	URIOutputPath string `json:"-"`
}

type runtimeInfo struct {
	InputParameters  map[string]*inputParameter
	InputArtifacts   map[string]*inputArtifact
	OutputParameters map[string]*outputParameter
	OutputArtifacts  map[string]*outputArtifact
}

func parseRuntimeInfo(jsonEncoded string) (*runtimeInfo, error) {
	r := &runtimeInfo{
		InputParameters:  make(map[string]*inputParameter),
		InputArtifacts:   make(map[string]*inputArtifact),
		OutputParameters: make(map[string]*outputParameter),
		OutputArtifacts:  make(map[string]*outputArtifact),
	}

	if err := json.Unmarshal([]byte(jsonEncoded), r); err != nil {
		return nil, err
	}

	fmt.Printf("Got runtimeinfo: %s", spew.Sdump(r))

	return r, nil
}
