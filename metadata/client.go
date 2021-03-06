package metadata

import (
	"context"
	"errors"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/glog"
	pb "github.com/neuromage/kfp-launcher/third_party/ml_metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v2"
)

const (
	pipelineContextTypeName    = "kfp.Pipeline"
	pipelineRunContextTypeName = "kfp.PipelineRun"
	containerExecutionTypeName = "kfp.ContainerExecution"
)

var (
	// Note: All types are schemaless so we can easily evolve the types as needed.
	pipelineContextType = &pb.ContextType{
		Name: proto.String(pipelineContextTypeName),
	}

	pipelineRunContextType = &pb.ContextType{
		Name: proto.String(pipelineRunContextTypeName),
	}

	containerExecutionType = &pb.ExecutionType{
		Name: proto.String(containerExecutionTypeName),
	}
)

// Client is ..
type Client struct {
	svc pb.MetadataStoreServiceClient
}

// NewClient ...
func NewClient(serverAddress, serverPort string) (*Client, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", serverAddress, serverPort), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &Client{
		svc: pb.NewMetadataStoreServiceClient(conn),
	}, nil
}

type Parameters struct {
	IntParameters    map[string]int64
	StringParameters map[string]string
	DoubleParameters map[string]float64
}

type ExecutionConfig struct {
	InputParameters *Parameters
	InputArtifacts  []*InputArtifact
}

type InputArtifact struct {
	Artifact *pb.Artifact
}
type OutputArtifact struct {
	Artifact *pb.Artifact
	Schema   string
}

type Pipeline struct {
	pipelineCtx    *pb.Context
	pipelineRunCtx *pb.Context
}

type Execution struct {
	execution *pb.Execution
	pipeline  *Pipeline
}

func (c *Client) GetPipeline(ctx context.Context, pipelineName string, pipelineRunID string) (*Pipeline, error) {
	pipelineContext, err := getOrInsertContext(ctx, c.svc, pipelineName, pipelineContextType)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Got pipeline context:\n%+v\n", pipelineContext)

	pipelineRunContext, err := getOrInsertContext(ctx, c.svc, pipelineRunID, pipelineRunContextType)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Got pipeline run context:\n%+v\n", pipelineRunContext)

	return &Pipeline{
		pipelineCtx:    pipelineContext,
		pipelineRunCtx: pipelineRunContext,
	}, nil

}

func (c *Client) getContainerExecutionTypeID(ctx context.Context) (int64, error) {
	eType, err := c.svc.PutExecutionType(ctx, &pb.PutExecutionTypeRequest{
		ExecutionType: containerExecutionType,
	})

	if err != nil {
		return 0, err
	}

	return eType.GetTypeId(), nil
}

func stringValue(s string) *pb.Value {
	return &pb.Value{Value: &pb.Value_StringValue{StringValue: s}}
}

func intValue(i int64) *pb.Value {
	return &pb.Value{Value: &pb.Value_IntValue{IntValue: i}}
}

func doubleValue(f float64) *pb.Value {
	return &pb.Value{Value: &pb.Value_DoubleValue{DoubleValue: f}}
}

func (c *Client) PublishExecution(ctx context.Context, execution *Execution, outputParameters *Parameters, outputArtifacts []*OutputArtifact) error {
	e := execution.execution
	e.LastKnownState = pb.Execution_COMPLETE.Enum()

	// Record output parameters.
	for n, p := range outputParameters.IntParameters {
		e.CustomProperties["output:"+n] = intValue(p)
	}
	for n, p := range outputParameters.DoubleParameters {
		e.CustomProperties["output:"+n] = doubleValue(p)
	}
	for n, p := range outputParameters.StringParameters {
		e.CustomProperties["output:"+n] = stringValue(p)
	}

	req := &pb.PutExecutionRequest{
		Execution: e,
		Contexts:  []*pb.Context{execution.pipeline.pipelineCtx, execution.pipeline.pipelineRunCtx},
	}

	for _, oa := range outputArtifacts {
		aePair := &pb.PutExecutionRequest_ArtifactAndEvent{
			Event: &pb.Event{
				Type:       pb.Event_OUTPUT.Enum(),
				ArtifactId: oa.Artifact.Id,
			},
		}
		req.ArtifactEventPairs = append(req.ArtifactEventPairs, aePair)
	}

	_, err := c.svc.PutExecution(ctx, req)
	return err
}

func (c *Client) CreateExecution(ctx context.Context, pipeline *Pipeline, taskName, taskID, containerImage string, config *ExecutionConfig) (*Execution, error) {
	typeID, err := c.getContainerExecutionTypeID(ctx)
	if err != nil {
		return nil, err
	}

	e := &pb.Execution{
		TypeId: &typeID,
		CustomProperties: map[string]*pb.Value{
			"task_name":       stringValue(taskName),
			"pipeline_name":   stringValue(*pipeline.pipelineCtx.Name),
			"pipeline_run_id": stringValue(*pipeline.pipelineRunCtx.Name),
			"kfp_pod_name":    stringValue(taskID),
			"container_image": stringValue(containerImage),
		},
		LastKnownState: pb.Execution_RUNNING.Enum(),
	}

	for k, v := range config.InputParameters.StringParameters {
		e.CustomProperties["input:"+k] = stringValue(v)
	}
	for k, v := range config.InputParameters.IntParameters {
		e.CustomProperties["input:"+k] = intValue(v)
	}
	for k, v := range config.InputParameters.DoubleParameters {
		e.CustomProperties["input:"+k] = doubleValue(v)
	}

	req := &pb.PutExecutionRequest{
		Execution: e,
		Contexts:  []*pb.Context{pipeline.pipelineCtx, pipeline.pipelineRunCtx},
	}

	for _, ia := range config.InputArtifacts {
		aePair := &pb.PutExecutionRequest_ArtifactAndEvent{
			Event: &pb.Event{
				Type:       pb.Event_INPUT.Enum(),
				ArtifactId: ia.Artifact.Id,
			},
		}
		req.ArtifactEventPairs = append(req.ArtifactEventPairs, aePair)
	}

	res, err := c.svc.PutExecution(ctx, req)
	if err != nil {
		return nil, err
	}

	getReq := &pb.GetExecutionsByIDRequest{
		ExecutionIds: []int64{res.GetExecutionId()},
	}

	getRes, err := c.svc.GetExecutionsByID(ctx, getReq)
	if err != nil {
		return nil, err
	}

	if len(getRes.Executions) != 1 {
		return nil, fmt.Errorf("Expected to get one Execution, got %d instead. Request: %v", len(getRes.Executions), getReq)
	}

	return &Execution{
		pipeline:  pipeline,
		execution: getRes.Executions[0],
	}, nil
}

// GetArtifacts ...
func (c *Client) GetArtifacts(ctx context.Context, ids []int64) ([]*pb.Artifact, error) {
	req := &pb.GetArtifactsByIDRequest{ArtifactIds: ids}
	res, err := c.svc.GetArtifactsByID(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.Artifacts, nil
}

// Only supports schema titles for now.
type schemaObject struct {
	Title string `yaml:"title"`
}

func schemaToArtifactType(schema string) (*pb.ArtifactType, error) {
	so := &schemaObject{}
	if err := yaml.Unmarshal([]byte(schema), so); err != nil {
		return nil, err
	}

	// TODO: Also parse properties.
	if so.Title == "" {
		glog.Fatal("No title specified")
	}
	at := &pb.ArtifactType{Name: proto.String(so.Title)}
	return at, nil
}

// RecordArtifact ...
func (c *Client) RecordArtifact(ctx context.Context, schema string, artifact *pb.Artifact) (*pb.Artifact, error) {
	fmt.Printf("Logging Artifact %s, schema: %s", spew.Sdump(artifact), schema)

	at, err := schemaToArtifactType(schema)
	if err != nil {
		return nil, err
	}
	putTypeRes, err := c.svc.PutArtifactType(ctx, &pb.PutArtifactTypeRequest{ArtifactType: at})
	if err != nil {
		return nil, err
	}
	at.Id = putTypeRes.TypeId

	artifact.TypeId = at.Id

	res, err := c.svc.PutArtifacts(ctx, &pb.PutArtifactsRequest{
		Artifacts: []*pb.Artifact{artifact},
	})
	if err != nil {
		return nil, err
	}
	if len(res.ArtifactIds) != 1 {
		return nil, errors.New("Failed to insert exactly one artifact")
	}

	getRes, err := c.svc.GetArtifactsByID(ctx, &pb.GetArtifactsByIDRequest{ArtifactIds: res.ArtifactIds})
	if err != nil {
		return nil, err
	}
	if len(getRes.Artifacts) != 1 {
		return nil, errors.New("Failed to retrieve exactly one artifact")
	}
	return getRes.Artifacts[0], nil
}

func getOrInsertContext(ctx context.Context, svc pb.MetadataStoreServiceClient, contextName string, contextType *pb.ContextType) (*pb.Context, error) {
	// See if the context already exists.
	getCtxRes, err := svc.GetContextByTypeAndName(ctx, &pb.GetContextByTypeAndNameRequest{TypeName: contextType.Name, ContextName: proto.String(contextName)})

	// Bug in MLMD GetContextsByTypeAndName, where we return status OK even when
	// no context was found.
	if err == nil && getCtxRes.Context != nil {
		return getCtxRes.Context, nil
	}

	// Otherwise, create the Context.
	// First, lookup or create the ContextType.
	var typeID *int64
	getTypeRes, err := svc.GetContextType(ctx, &pb.GetContextTypeRequest{TypeName: contextType.Name})
	if err == nil {
		typeID = getTypeRes.ContextType.Id
	} else {
		if status.Convert(err).Code() != codes.NotFound { // Something else went wrong.
			return nil, err
		}
		// Create the ContextType.
		res, err := svc.PutContextType(ctx, &pb.PutContextTypeRequest{ContextType: contextType})
		if err != nil {
			return nil, err
		}
		typeID = res.TypeId
	}

	// Next, create the Context.
	putReq := &pb.PutContextsRequest{
		Contexts: []*pb.Context{
			{
				Name:   proto.String(contextName),
				TypeId: typeID,
			},
		},
	}
	_, err = svc.PutContexts(ctx, putReq)
	if err != nil {
		return nil, err
	}

	// Get the created context.
	getCtxRes, err = svc.GetContextByTypeAndName(ctx, &pb.GetContextByTypeAndNameRequest{TypeName: contextType.Name, ContextName: proto.String(contextName)})
	return getCtxRes.GetContext(), err
}
