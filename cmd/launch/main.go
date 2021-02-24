package main

import (
	"context"
	"flag"

	"github.com/golang/glog"
	"github.com/neuromage/kfp-launcher/component"
)

var (
	mlmdServerAddress = flag.String("mlmd_server_address", "", "")
	mlmdServerPort    = flag.String("mlmd_server_port", "8080", "")
	runtimeInfoJSON   = flag.String("runtime_info_json", "", "")
	containerImage    = flag.String("container_image", "", "")
	taskName          = flag.String("task_name", "", "")
	pipelineName      = flag.String("pipeline_name", "", "")
	pipelineRunID     = flag.String("pipeline_run_id", "", "")
	pipelineTaskID    = flag.String("pipeline_task_id", "", "")
	pipelineRoot      = flag.String("pipeline_root", "", "")
)

func check(err error) {
	if err != nil {
		glog.Fatalf("CHECK-fail: %s", err)
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()

	opts := &component.LauncherOptions{
		PipelineName:      *pipelineName,
		PipelineRunID:     *pipelineRunID,
		PipelineTaskID:    *pipelineTaskID,
		PipelineRoot:      *pipelineRoot,
		TaskName:          *taskName,
		ContainerImage:    *containerImage,
		MLMDServerAddress: *mlmdServerAddress,
		MLMDServerPort:    *mlmdServerPort,
	}
	launcher, err := component.NewLauncher(*runtimeInfoJSON, opts)
	check(err)

	check(launcher.RunComponent(ctx, flag.Args()[0], flag.Args()[1:]...))
}
