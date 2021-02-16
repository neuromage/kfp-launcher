package component

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
)

func TestOpenBucket(t *testing.T) {
	// blob.OpenBucket creates a *blob.Bucket from a URL.
	// This URL will open the bucket "my-bucket" using default credentials.
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "gs://ml-pipeline-artifacts?prefix=custom_dir/")
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	// Open the key "foo.txt" for writing with the default options.
	w, err := bucket.NewWriter(ctx, "foo.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := fmt.Fprintln(w, "Hello, World!")
	// Always check the return value of Close when writing.
	closeErr := w.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
}

func TestCopyToLocal(t *testing.T) {
	// blob.OpenBucket creates a *blob.Bucket from a URL.
	// This URL will open the bucket "my-bucket" using default credentials.
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "gs://ml-pipeline-artifacts?prefix=custom_dir/")
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	// Open the key "foo.txt" for writing with the default options.
	r, err := bucket.NewReader(ctx, "foo.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	w, err := os.Create("dir1/dir2/foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(w, r); err != nil {
		t.Fatal(err)
	}
}

func Test_parseCloudBucket(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    *bucketConfig
		wantErr bool
	}{
		{
			name: "Parses GCS - Just the bucket",
			path: "gs://my-bucket",
			want: &bucketConfig{
				scheme:     "gs://",
				bucketName: "my-bucket",
				prefix:     "",
			},
			wantErr: false,
		},
		{
			name: "Parses GCS - Just the bucket with trailing slash",
			path: "gs://my-bucket/",
			want: &bucketConfig{
				scheme:     "gs://",
				bucketName: "my-bucket",
				prefix:     "",
			},
			wantErr: false,
		},
		{
			name: "Parses GCS - Bucket with prefix",
			path: "gs://my-bucket/my-path",
			want: &bucketConfig{
				scheme:     "gs://",
				bucketName: "my-bucket",
				prefix:     "my-path/",
			},
			wantErr: false,
		},
		{
			name: "Parses GCS - Bucket with prefix and trailing slash",
			path: "gs://my-bucket/my-path/",
			want: &bucketConfig{
				scheme:     "gs://",
				bucketName: "my-bucket",
				prefix:     "my-path/",
			},
			wantErr: false,
		},
		{
			name: "Parses GCS - Bucket with multiple path components in prefix",
			path: "gs://my-bucket/my-path/123",
			want: &bucketConfig{
				scheme:     "gs://",
				bucketName: "my-bucket",
				prefix:     "my-path/123/",
			},
			wantErr: false,
		},
		{
			name: "Parses GCS - Bucket with multiple path components in prefix and trailing slash",
			path: "gs://my-bucket/my-path/123/",
			want: &bucketConfig{
				scheme:     "gs://",
				bucketName: "my-bucket",
				prefix:     "my-path/123/",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBucketConfig(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("%q: parseCloudBucket() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("%q: parseCloudBucket() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func Test_bucketConfig_keyFromURI(t *testing.T) {
	type fields struct {
		scheme     string
		bucketName string
		prefix     string
	}

	tests := []struct {
		name         string
		bucketConfig *bucketConfig
		uri          string
		want         string
		wantErr      bool
	}{
		{
			name:         "Bucket with empty prefix",
			bucketConfig: &bucketConfig{scheme: "gs://", bucketName: "my-bucket", prefix: ""},
			uri:          "gs://my-bucket/path1/path2",
			want:         "path1/path2",
			wantErr:      false,
		},
		{
			name:         "Bucket with non-empty prefix ",
			bucketConfig: &bucketConfig{scheme: "gs://", bucketName: "my-bucket", prefix: "path0/"},
			uri:          "gs://my-bucket/path0/path1/path2",
			want:         "path1/path2",
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.bucketConfig.keyFromURI(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Errorf("%q: buckerConfig.keyFromURI() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("bucketConfig.keyFromURI() = %v, want %v", got, tt.want)
			}
		})
	}
}
