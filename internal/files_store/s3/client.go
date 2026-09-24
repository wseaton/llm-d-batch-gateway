/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package s3 provides an S3-based implementation of the BatchFilesClient interface.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	fsio "github.com/llm-d/llm-d-batch-gateway/internal/files_store/io"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

const (
	defaultTimeout = 30 * time.Second
)

type s3API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

type uploaderAPI interface {
	Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) //nolint:staticcheck // TODO: migrate to feature/s3/transfermanager
}

// Client implements api.BatchFilesClient using S3 storage.
// All objects are stored in a single configured bucket. The folderName parameter
// in Store/Retrieve/Delete is used as part of the S3 key for tenant isolation.
type Client struct {
	s3Client s3API
	uploader uploaderAPI
	prefix   string
	bucket   string
}

var (
	_ api.BatchFilesClient = (*Client)(nil)
	_ api.ObjectAdopter    = (*Client)(nil)
)

// Config holds configuration for the S3 client.
type Config struct {
	Region           string `yaml:"region"`
	Bucket           string `yaml:"bucket"`
	Endpoint         string `yaml:"endpoint"`
	AccessKeyID      string `yaml:"access_key_id"`
	SecretAccessKey  string `yaml:"-"`
	Prefix           string `yaml:"prefix"`
	UsePathStyle     bool   `yaml:"use_path_style"`
	AutoCreateBucket bool   `yaml:"auto_create_bucket"`
}

// Validate checks that all required fields are set.
func (c *Config) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("s3.region cannot be empty")
	}
	if c.Bucket == "" {
		return fmt.Errorf("s3.bucket cannot be empty")
	}
	return nil
}

// New creates a new S3-based BatchFilesClient.
func New(ctx context.Context, cfg Config) (*Client, error) {
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(cfg.Region))

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}
	if cfg.UsePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	s3Client := s3.NewFromConfig(awsCfg, s3Opts...)

	c := &Client{
		s3Client: s3Client,
		uploader: manager.NewUploader(s3Client), //nolint:staticcheck // TODO: migrate to feature/s3/transfermanager
		prefix:   cfg.Prefix,
		bucket:   cfg.Bucket,
	}

	if err := c.ensureBucket(ctx, cfg.AutoCreateBucket); err != nil {
		return nil, fmt.Errorf("ensure bucket %s: %w", cfg.Bucket, err)
	}

	return c, nil
}

// buildKey constructs the full S3 key from the folder name and file name.
func (c *Client) buildKey(folderName, fileName string) string {
	key := folderName + "/" + fileName
	if c.prefix != "" {
		key = c.prefix + "/" + key
	}
	return key
}

// ensureBucket checks that the configured bucket exists. When autoCreate is
// true, it creates the bucket if it does not exist; otherwise it returns an
// error. Called once at startup.
func (c *Client) ensureBucket(ctx context.Context, autoCreate bool) error {
	_, err := c.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err == nil {
		return nil
	}

	var notFound *types.NotFound
	if !errors.As(err, &notFound) {
		var noSuchBucket *types.NoSuchBucket
		if !errors.As(err, &noSuchBucket) {
			return fmt.Errorf("head bucket %s: %w", c.bucket, err)
		}
	}

	if !autoCreate {
		return fmt.Errorf("bucket %s does not exist", c.bucket)
	}

	_, err = c.s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err != nil {
		var alreadyOwned *types.BucketAlreadyOwnedByYou
		if errors.As(err, &alreadyOwned) {
			return nil
		}
		return fmt.Errorf("create bucket %s: %w", c.bucket, err)
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("Bucket created", "bucket", c.bucket)
	return nil
}

// Store stores a file in S3.
// The folderName parameter is used as a key prefix for tenant isolation.
func (c *Client) Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (
	*api.BatchFileMetadata, error,
) {
	key := c.buildKey(folderName, fileName)

	_, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return nil, fmt.Errorf("%w: %s", api.ErrFileExists, key)
	}

	var notFound *types.NotFound
	if !errors.As(err, &notFound) {
		return nil, err
	}

	countingReader := &fsio.LimitedCountingReader{
		Reader:    reader,
		SizeLimit: fileSizeLimit,
		LineLimit: lineNumLimit,
	}

	_, err = c.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   countingReader,
	})
	if err != nil {
		switch {
		case errors.Is(err, api.ErrFileTooLarge):
			return nil, api.ErrFileTooLarge
		case errors.Is(err, api.ErrTooManyLines):
			return nil, api.ErrTooManyLines
		default:
			return nil, err
		}
	}

	metadata := &api.BatchFileMetadata{
		Location:    key,
		Size:        countingReader.BytesRead,
		LinesNumber: countingReader.LineCount,
		ModTime:     time.Now(),
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File stored successfully",
		"bucket", c.bucket, "key", key, "size", metadata.Size, "lines", metadata.LinesNumber)

	return metadata, nil
}

// Retrieve retrieves a file from S3.
// The folderName parameter is used as a key prefix for tenant isolation.
func (c *Client) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *api.BatchFileMetadata, error) {
	key := c.buildKey(folderName, fileName)

	out, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		var noSuchBucket *types.NoSuchBucket
		if errors.As(err, &noSuchKey) || errors.As(err, &noSuchBucket) {
			return nil, nil, os.ErrNotExist
		}
		return nil, nil, err
	}

	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}

	modTime := time.Now()
	if out.LastModified != nil {
		modTime = *out.LastModified
	}

	metadata := &api.BatchFileMetadata{
		Location:    key,
		Size:        size,
		ModTime:     modTime,
		ContentType: aws.ToString(out.ContentType),
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File retrieved successfully",
		"bucket", c.bucket, "key", key, "size", metadata.Size)

	return out.Body, metadata, nil
}

// Adopt copies the object sourceRef names to fileName in folderName inside the same bucket,
// keeping its Content-Type, then deletes the source. References to other buckets are refused.
func (c *Client) Adopt(ctx context.Context, sourceRef, fileName, folderName string) (*api.BatchFileMetadata, error) {
	bucket, srcKey, ok := strings.Cut(strings.TrimPrefix(sourceRef, "s3://"), "/")
	if !strings.HasPrefix(sourceRef, "s3://") || !ok || srcKey == "" {
		return nil, fmt.Errorf("adopt %q: not an s3://bucket/key reference", sourceRef)
	}
	if bucket != c.bucket {
		return nil, fmt.Errorf("adopt %q: object is outside the files bucket %s", sourceRef, c.bucket)
	}
	key := c.buildKey(folderName, fileName)
	out, err := c.s3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(c.bucket),
		Key:        aws.String(key),
		CopySource: aws.String(c.bucket + "/" + escapeKey(srcKey)),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
			return nil, fmt.Errorf("adopt %q: %w", sourceRef, os.ErrNotExist)
		}
		return nil, fmt.Errorf("adopt %q: copy to %s: %w", sourceRef, key, err)
	}
	if _, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.bucket), Key: aws.String(srcKey)}); err != nil {
		logr.FromContextOrDiscard(ctx).Error(err, "Adopted object's source was not deleted", "source", sourceRef)
	}
	modTime := time.Now()
	if out.CopyObjectResult != nil && out.CopyObjectResult.LastModified != nil {
		modTime = *out.CopyObjectResult.LastModified
	}
	return &api.BatchFileMetadata{Location: key, ModTime: modTime}, nil
}

// escapeKey URL-encodes each segment of an object key for CopySource.
func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// Delete deletes a file from S3.
// The folderName parameter is used as a key prefix for tenant isolation.
func (c *Client) Delete(ctx context.Context, fileName, folderName string) error {
	key := c.buildKey(folderName, fileName)

	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noSuchBucket *types.NoSuchBucket
		if errors.As(err, &noSuchBucket) {
			return os.ErrNotExist
		}
		return err
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File deleted successfully",
		"bucket", c.bucket, "key", key)

	return nil
}

// GetContext returns a derived context with a timeout.
func (c *Client) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	if timeLimit == 0 {
		timeLimit = defaultTimeout
	}
	return context.WithTimeout(parentCtx, timeLimit)
}

// Close closes the client.
func (c *Client) Close() error {
	return nil
}
