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
)

var (
	pipelineContextType = &pb.ContextType{
		Name: proto.String(pipelineContextTypeName),
	}

	pipelineRunContextType = &pb.ContextType{
		Name: proto.String(pipelineRunContextTypeName),
	}
)

// Client is ..
type Client struct {
	svc           pb.MetadataStoreServiceClient
	pipelineName  string
	pipelineRunID string

	pipelineContext    *pb.Context
	pipelineRunContext *pb.Context

	initialized bool
}

// NewClient ...
func NewClient(serverAddress, serverPort, pipelineName, pipelineRunID string) (*Client, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", serverAddress, serverPort), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &Client{
		svc:           pb.NewMetadataStoreServiceClient(conn),
		pipelineName:  pipelineName,
		pipelineRunID: pipelineRunID,
	}, nil
}

// Init ...
func (c *Client) Init(ctx context.Context) error {
	var err error
	c.pipelineContext, err = getOrInsertContext(ctx, c.svc, c.pipelineName, pipelineContextType)
	if err != nil {
		return err
	}
	fmt.Printf("Got pipeline context:\n%+v\n", c.pipelineContext)

	c.pipelineRunContext, err = getOrInsertContext(ctx, c.svc, c.pipelineRunID, pipelineRunContextType)
	if err != nil {
		return err
	}
	fmt.Printf("Got pipeline run context:\n%+v\n", c.pipelineRunContext)

	c.initialized = true
	return nil
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
