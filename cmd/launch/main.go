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
	annotationsFile   = flag.String("annotations_file", "/kfp-v2/task_spec", "")
	taskSpecJSON      = flag.String("task_spec_json", "", "")
	componentSpecJSON = flag.String("component_spec_json", "", "")
	taskName          = flag.String("task_name", "", "")
	pipelineName      = flag.String("pipeline_name", "", "")
	pipelineRunID     = flag.String("pipeline_run_id", "", "")
	pipelineTaskID    = flag.String("pipeline_task_id", "", "")
	pipelineRoot      = flag.String("pipeline_root", "", "")
	runtimeInfoJSON   = flag.String("runtime_info_json", "", "")
)

// func parseTaskSpecJSON(s string) (*specPb.PipelineTaskSpec, error) {
// 	res := &specPb.PipelineTaskSpec{}

// 	if err := protojson.Unmarshal([]byte(s), res); err != nil {
// 		return nil, err
// 	}
// 	fmt.Printf("GOT TaskSpec:\n%s\n", res)
// 	return res, nil
// }

// func parseComponentSpecJSON(s string) (*specPb.ComponentSpec, error) {
// 	res := &specPb.ComponentSpec{}

// 	if err := protojson.Unmarshal([]byte(s), res); err != nil {
// 		return nil, err
// 	}
// 	fmt.Printf("GOT ComponentSpec:\n%s\n", res)
// 	return res, nil
// }

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
		MLMDServerAddress: *mlmdServerAddress,
		MLMDServerPort:    *mlmdServerPort,
	}
	launcher, err := component.NewLauncher(*runtimeInfoJSON, opts)
	check(err)

	check(launcher.RunComponent(ctx, flag.Args()[0], flag.Args()[1:]...))

	// conn, err := grpc.Dial(fmt.Sprintf("%s:%s", *mlmdServerAddress, *mlmdServerPort), grpc.WithInsecure())
	// check(err)

	// cli := pb.NewMetadataStoreServiceClient(conn)

	// taskSpec, err := parseTaskSpecJSON(*taskSpecJSON)
	// check(err)

	// componentSpec, err := parseComponentSpecJSON(*componentSpecJSON)
	// check(err)

	// runtimeInfo, err := parseRuntimeInfo(*runtimeInfoJSON)
	// check(err)
	// spew.Dump(runtimeInfo)

	// m := metadataClient{
	// 	cli: cli,
	// 	// taskSpec:      taskSpec,
	// 	// componentSpec: componentSpec,
	// 	pipelineName:  *pipelineName,
	// 	pipelineRunID: *pipelineRunID,
	// 	runtimeInfo:   runtimeInfo,
	// }

	// req := &pb.GetContextsRequest{}
	// res, err := cli.GetContexts(context.Background(), req)
	// fmt.Printf("Contexts response:\n%+v\n\n", res)
	// req2 := &pb.GetContextTypesRequest{}
	// res2, err := cli.GetContextTypes(context.Background(), req2)
	// fmt.Printf("ContextTypes response:\n%+v\n\n", res2)
	// check(err)

	// dat, err := ioutil.ReadFile("/kfp-v2/task_spec")
	// check(err)
	// fmt.Printf("Data\n%s\n\n", dat)

	// job := parsePipelineJob()
	// fmt.Printf("PipelineJob:\n %v\n", job)

}

// func parsePipelineJob() *specPb.PipelineJob {
// 	res := &specPb.PipelineJob{}

//   f, err := os.Open(*annotationsFile)
// 	check(err)
// 	scanner := bufio.NewScanner(f)
// 	scanner.Split(bufio.ScanLines)

// 	for scanner.Scan() {
// 		l := scanner.Text()
// 		tokens := strings.SplitN(l, "=", 2)
// 		fmt.Println(tokens[0])

// 		if tokens[0]  == "pipelines.kubeflow.org/v2_task_spec" {
// 			val, err := strconv.Unquote(tokens[1])
// 			check(err)
// 			check(protojson.Unmarshal([]byte(val), res))
// 		}
// 	}

// 	return res
// }
