package component

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/glog"
	"github.com/neuromage/kfp-launcher/metadata"
	pb "github.com/neuromage/kfp-launcher/third_party/ml_metadata"
	"google.golang.org/protobuf/encoding/protojson"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
)

// Launcher ...
type Launcher struct {
	options                 *LauncherOptions
	runtimeInfo             *runtimeInfo
	placeholderReplacements map[string]string
	metadata                *metadata.Client
	bucketConfig            *bucketConfig
}

// LauncherOptions ...
type LauncherOptions struct {
	PipelineName      string
	PipelineRoot      string
	PipelineRunID     string
	PipelineTaskID    string
	TaskName          string
	MLMDServerAddress string
	MLMDServerPort    string
}

type bucketConfig struct {
	scheme     string
	bucketName string
	prefix     string
}

func (b *bucketConfig) bucketURL() string {
	u := b.scheme + b.bucketName

	if len(b.prefix) > 0 {
		u = fmt.Sprintf("%s?prefix=%s", u, b.prefix)
	}
	return u
}

func (b *bucketConfig) keyFromURI(uri string) (string, error) {
	prefixedBucket := b.scheme + path.Join(b.bucketName, b.prefix)
	if !strings.HasPrefix(uri, prefixedBucket) {
		return "", fmt.Errorf("URI %q does not have expected bucket prefix %q", uri, prefixedBucket)
	}

	key := strings.TrimLeft(strings.TrimPrefix(uri, prefixedBucket), "/")
	if len(key) == 0 {
		return "", fmt.Errorf("URI %q has empty key given prefixed bucket %q", uri, prefixedBucket)
	}
	return key, nil
}

func (b *bucketConfig) uriFromKey(blobKey string) string {
	return b.scheme + path.Join(b.bucketName, b.prefix, blobKey)
}

var bucketPattern = regexp.MustCompile(`^([a-z][a-z0-9]+:///?)([^/]+)(/[^ ?]*)?$`)

func parseBucketConfig(path string) (*bucketConfig, error) {
	ms := bucketPattern.FindStringSubmatch(path)
	if ms == nil || len(ms) != 4 {
		return nil, fmt.Errorf("Unrecognized pipeline root format: %q", path)
	}

	// TODO: Verify/add support for s3:// and file:///.
	if ms[1] != "gs://" {
		return nil, fmt.Errorf("Unsupported Cloud bucket: %q", path)
	}

	prefix := strings.TrimPrefix(ms[3], "/")
	if len(prefix) > 0 && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	return &bucketConfig{
		scheme:     ms[1],
		bucketName: ms[2],
		prefix:     prefix,
	}, nil
}

func (o *LauncherOptions) validate() error {
	empty := func(s string) bool { return len(s) == 0 }
	err := func(s string) error { return fmt.Errorf("Must specify %s", s) }

	if empty(o.PipelineName) {
		return err("PipelineName")
	}
	if empty(o.PipelineRunID) {
		return err("PipelineRunID")
	}
	if empty(o.PipelineTaskID) {
		return err("PipelineTaskID")
	}
	if empty(o.PipelineRoot) {
		return err("PipelineRoot")
	}
	if empty(o.TaskName) {
		return err("TaskName")
	}
	if empty(o.MLMDServerAddress) {
		return err("MLMDServerAddress")
	}
	if empty(o.MLMDServerPort) {
		return err("MLMDServerPort")
	}
	return nil
}

// NewLauncher ...
func NewLauncher(runtimeInfo string, options *LauncherOptions) (*Launcher, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}

	bc, err := parseBucketConfig(options.PipelineRoot)
	if err != nil {
		return nil, err
	}

	rt, err := parseRuntimeInfo(runtimeInfo)
	if err != nil {
		return nil, err
	}

	// Placeholder replacements.
	pr := make(map[string]string)

	metadata, err := metadata.NewClient(options.MLMDServerAddress, options.MLMDServerPort, options.PipelineName, options.PipelineRunID)
	if err != nil {
		return nil, err
	}

	return &Launcher{
		options:                 options,
		placeholderReplacements: pr,
		runtimeInfo:             rt,
		metadata:                metadata,
		bucketConfig:            bc,
	}, nil
}

func (l *Launcher) prepareInputs(ctx context.Context) error {
	bucket, err := blob.OpenBucket(context.Background(), l.bucketConfig.bucketURL())
	if err != nil {
		return fmt.Errorf("Failed to open bucket %q: %v", l.bucketConfig.bucketName, err)
	}
	defer bucket.Close()

	// Read input artifact metadata.
	for k, v := range l.runtimeInfo.InputArtifacts {
		if len(v.FileInputPath) == 0 {
			return fmt.Errorf("Missing input artifact metadata file for input: %q", k)
		}

		b, err := ioutil.ReadFile(v.FileInputPath)
		if err != nil {
			return fmt.Errorf("Failed to read input artifact metadata file for %q: %v", k, err)
		}

		a := &pb.Artifact{}
		if err := protojson.Unmarshal(b, a); err != nil {
			return fmt.Errorf("Failed to unmarshall input artifact metadata for %q: %v", k, err)
		}

		v.Artifact = a

		// Prepare input uri placeholder.
		key := fmt.Sprintf(`{{$.inputs.artifacts['%s'].uri}}`, k)
		l.placeholderReplacements[key] = v.Artifact.GetUri()

		// Prepare input path placeholder.
		v.LocalArtifactFilePath = path.Join("/tmp/kfp_launcher_inputs", k, "data")
		key = fmt.Sprintf(`{{$.inputs.artifacts['%s'].path}}`, k)
		l.placeholderReplacements[key] = v.LocalArtifactFilePath

		// Copy artifact to local storage.
		// TODO: Selectively copy artifacts for which .path was actually specified
		// on the command line.

		blobKey, err := l.bucketConfig.keyFromURI(v.Artifact.GetUri())
		if err != nil {
			return err
		}
		r, err := bucket.NewReader(ctx, blobKey, nil)
		if err != nil {
			return err
		}
		defer r.Close()

		if err := os.MkdirAll(path.Dir(v.LocalArtifactFilePath), 0644); err != nil {
			return err
		}
		w, err := os.Create(v.LocalArtifactFilePath)
		if err != nil {
			return err
		}
		if _, err := io.Copy(w, r); err != nil {
			return err
		}
	}

	// Prepare input parameter placeholders.
	for k, v := range l.runtimeInfo.InputParameters {
		key := fmt.Sprintf(`{{$.inputs.parameters['%s']}}`, k)
		l.placeholderReplacements[key] = string(v)
	}

	return nil
}

func (l *Launcher) prepareOutputs(ctx context.Context) error {
	for k, v := range l.runtimeInfo.OutputParameters {
		key := fmt.Sprintf(`{{$.outputs.parameters['%s'].output_file}}`, k)
		l.placeholderReplacements[key] = v.FileOutputPath
	}

	for k, v := range l.runtimeInfo.OutputArtifacts {
		// TODO: sanitize k
		v.LocalArtifactFilePath = path.Join("/tmp/kfp_launcher_outputs", k, "data")

		if err := os.MkdirAll(path.Dir(v.LocalArtifactFilePath), 0644); err != nil {
			return err
		}

		blobKey := path.Join(l.options.PipelineName, l.options.PipelineRunID, l.options.PipelineTaskID, "data")
		v.URIOutputPath = l.bucketConfig.uriFromKey(blobKey)

		key := fmt.Sprintf(`{{$.outputs.artifacts['%s'].path}}`, k)
		l.placeholderReplacements[key] = v.LocalArtifactFilePath

		key = fmt.Sprintf(`{{$.outputs.artifacts['%s'].uri}}`, k)
		l.placeholderReplacements[key] = v.URIOutputPath
	}

	return nil
}

// RunComponent ..
func (l *Launcher) RunComponent(ctx context.Context, cmd string, args ...string) error {

	if err := l.prepareInputs(ctx); err != nil {
		return err
	}

	if err := l.prepareOutputs(ctx); err != nil {
		return err
	}

	// Update command.
	for i, v := range args {
		if _, ok := l.placeholderReplacements[v]; ok {
			args[i] = l.placeholderReplacements[v]
		}
	}

	executor := exec.Command(cmd, args...)

	fmt.Println("Running command: ")
	spew.Dump(executor.Args)
	executor.Stdin = os.Stdin
	executor.Stdout = os.Stdout
	executor.Stderr = os.Stderr
	defer glog.Flush()
	if err := executor.Run(); err != nil {
		return err
	}

	bucket, err := blob.OpenBucket(context.Background(), l.bucketConfig.bucketURL())
	if err != nil {
		return fmt.Errorf("Failed to open bucket %q: %v", l.bucketConfig.bucketName, err)
	}
	defer bucket.Close()

	// Register artifacts with MLMD.
	for _, v := range l.runtimeInfo.OutputArtifacts {
		var err error
		artifact := &pb.Artifact{
			Uri: &v.URIOutputPath,
		}

		artifact, err = l.metadata.RecordArtifact(ctx, v.ArtifactSchema, artifact)
		if err != nil {
			return err
		}

		if err := os.MkdirAll(path.Dir(v.FileOutputPath), 0644); err != nil {
			return err
		}

		b, err := protojson.Marshal(artifact)
		if err != nil {
			return err
		}

		if err := ioutil.WriteFile(v.FileOutputPath, b, 0644); err != nil {
			return err
		}

		// copy Artifacts out to remote storage.
		blobKey, err := l.bucketConfig.keyFromURI(v.URIOutputPath)
		if err != nil {
			return err
		}

		w, err := bucket.NewWriter(ctx, blobKey, nil)
		if err != nil {
			return err
		}

		r, err := os.Open(v.LocalArtifactFilePath)
		if err != nil {
			return err
		}
		defer r.Close()

		if _, err = io.Copy(w, r); err != nil {
			return err
		}

		if err = w.Close(); err != nil {
			return err
		}
	}

	return nil
}
