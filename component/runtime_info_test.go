// Package component ...
package component

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"
)

func Test_parseRuntimeInfo(t *testing.T) {
	tests := []struct {
		name        string
		jsonEncoded string
		want        *runtimeInfo
		wantErr     bool
	}{
		{
			name: "Parses OutputParameters Correctly",
			jsonEncoded: `{
				"outputParameters": {
					"my_param": {
						"parameterType": "INT",
						"fileOutputPath": "/tmp/outputs/my_param/data"
					}
				}
			}`,
			want: &runtimeInfo{
				OutputParameters: map[string]*outputParameter{
					"my_param": {
						ParameterType:  "INT",
						FileOutputPath: "/tmp/outputs/my_param/data",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Parses OutputArtifacts Correctly",
			jsonEncoded: `{
				"outputArtifacts": {
					"my_artifact": {
						"artifactSchema": "properties:\ntitle: kfp.Dataset\ntype: object\n",
					  "fileOutputPath": "/tmp/outputs/my_artifact/data"
					}
				}
			}`,
			want: &runtimeInfo{
				OutputArtifacts: map[string]*outputArtifact{
					"my_artifact": {
						ArtifactSchema: "properties:\ntitle: kfp.Dataset\ntype: object\n",
						FileOutputPath: "/tmp/outputs/my_artifact/data",
					},
				},
			},
			wantErr: false,
		},
		// TODO add tests for input params, artifacts.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRuntimeInfo(tt.jsonEncoded)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRuntimeInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateEmpty(), protocmp.Transform()); diff != "" {
				t.Errorf("parseRuntimeInfo() = %+v, want %+v\nDiff (-want, +got)\n%s", got, tt.want, diff)
				s, _ := json.MarshalIndent(tt.want, "", "  ")
				fmt.Printf("Want %s", s)
			}

		})
	}

	// r := `{"inputParameters": {"num_steps": "1234"}, "inputArtifacts": {"dataset_one": { "id":  "109", "typeId":  "46", "uri":  "gs:/my-pipeline-artifacts/my-test-pipeline/my-test-pipeline-frj96/my-test-pipeline-frj96-4214550545", "createTimeSinceEpoch":  "1613368292101", "lastUpdateTimeSinceEpoch":  "1613368292101" }}, "outputParameters": {}, "outputArtifacts": {"dataset_two": {"artifactSchema": "properties:\ntitle: kfp.Dataset\ntype: object\n", "fileOutputPath": "/tmp/outputs/dataset_two/data"}}}`

	// artifact := &pb.Artifact{
	// 	Id:     proto.Int64(123),
	// 	TypeId: proto.Int64(1223444),
	// 	Uri:    proto.String("1ljklkjlkj"),
	// }
	// b, err := protojson.Marshal(artifact)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// fmt.Printf("S: %s", strconv.Quote(string(b)))
	// _, err = parseRuntimeInfo(r)
	// if err != nil {
	// 	t.Fatal(err)
	// }
}
