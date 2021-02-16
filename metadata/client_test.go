package metadata

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	pb "github.com/neuromage/kfp-launcher/third_party/ml_metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func Test_schemaToArtifactType(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		want    *pb.ArtifactType
		wantErr bool
	}{
		{
			name:   "Parses Schema Title Correctly",
			schema: "properties:\ntitle: kfp.Dataset\ntype: object\n",
			want: &pb.ArtifactType{
				Name: proto.String("kfp.Dataset"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := schemaToArtifactType(tt.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("schemaToArtifactType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want, cmpopts.EquateEmpty(), protocmp.Transform()); diff != "" {
				t.Errorf("schemaToArtifactType() = %+v, want %+v\nDiff (-want, +got)\n%s", got, tt.want, diff)
			}
		})
	}
}
