// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package object_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/common/base58"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite"
	"storj.io/storj/satellite/internalpb"
	"storj.io/uplink"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/object"
)

func TestBeginUpload(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		_, err = object.BeginUpload(ctx, project, "not-existing-testbucket", "multipart-object", nil)
		require.Error(t, err)
		require.True(t, errors.Is(err, uplink.ErrBucketNotFound))

		err = planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], "testbucket")
		require.NoError(t, err)

		// assert there is no pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil)

		info, err := object.BeginUpload(ctx, project, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		// assert there is only one pending multipart upload
		assertUploadList(ctx, t, project, "testbucket", nil, "multipart-object")

		// we allow to start several multipart uploads for the same key
		_, err = object.BeginUpload(ctx, project, "testbucket", "multipart-object", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		info, err = object.BeginUpload(ctx, project, "testbucket", "multipart-object-1", nil)
		require.NoError(t, err)
		require.NotNil(t, info.UploadID)

		// assert there are two pending multipart uploads
		assertUploadList(ctx, t, project, "testbucket", nil, "multipart-object", "multipart-object-1")
	})
}

func TestBeginUploadWithMetadata(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount:   1,
		StorageNodeCount: 0,
		UplinkCount:      1,
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		project, err := planet.Uplinks[0].OpenProject(ctx, planet.Satellites[0])
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		err = planet.Uplinks[0].CreateBucket(ctx, planet.Satellites[0], "testbucket")
		require.NoError(t, err)

		expectedMetadata := map[string]uplink.CustomMetadata{
			"1/nil":   nil,
			"2/empty": {},
			"3/not-empty": {
				"key": "value",
			},
		}

		for name, metadata := range expectedMetadata {
			t.Run(name, func(t *testing.T) {
				info, err := object.BeginUpload(ctx, project, "testbucket", name, &object.MultipartUploadOptions{
					UserData: metaclient.ObjectUserData{
						Custom: metadata,
					},
				})
				require.NoError(t, err)
				require.NotNil(t, info.UploadID)

				list := project.ListUploads(ctx, "testbucket", &uplink.ListUploadsOptions{
					Prefix: name[:2],
					Custom: true,
				})
				require.True(t, list.Next())

				if metadata == nil {
					require.Empty(t, list.Item().Custom)
				} else {
					require.Equal(t, metadata, list.Item().Custom)
				}
				require.False(t, list.Next())
				require.NoError(t, list.Err())
			})
		}
	})
}

func TestChecksum_Multipart(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.ChecksumsEnabled = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		up := planet.Uplinks[0]

		objectKey := "test-object"

		checksum := metaclient.ObjectChecksum{
			Algorithm:   storj.ObjectChecksumAlgorithmCRC32,
			IsComposite: true,
			Value:       []byte("checksum"),
		}
		partChecksum := []byte("part checksum")

		project, err := up.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		requireNoPendingObjects := func(t *testing.T, bucketName string, msgAndArgs ...any) {
			iter := project.ListUploads(ctx, bucketName, &uplink.ListUploadsOptions{})
			require.False(t, iter.Next(), msgAndArgs...)
		}

		beginUploadWithPart := func(bucketName, objectKey string) (uploadID string, _ error) {
			upload, err := object.BeginUpload(ctx, project, bucketName, objectKey, nil)
			if err != nil {
				return "", errs.Wrap(err)
			}
			part, err := object.UploadPart(ctx, project, bucketName, objectKey, upload.UploadID, 1)
			if err != nil {
				return "", errs.Wrap(err)
			}
			if _, err = part.Write(testrand.Bytes(32)); err != nil {
				return "", errs.Wrap(err)
			}
			if err := part.Commit(); err != nil {
				return "", errs.Wrap(err)
			}
			return upload.UploadID, nil
		}

		requireNoCommittedObject := func(t *testing.T, bucketName, objectKey string, msgAndArgs ...any) {
			_, err := object.StatObject(ctx, project, bucketName, objectKey, nil)
			require.ErrorIs(t, err, uplink.ErrObjectNotFound, msgAndArgs...)
		}

		t.Run("Begin upload", func(t *testing.T) {
			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			_, err := object.BeginUpload(ctx, project, bucketName, objectKey, &object.MultipartUploadOptions{
				UserData: metaclient.ObjectUserData{
					Checksum: checksum,
				},
			})
			require.NoError(t, err)

			iter := object.ListUploads(ctx, project, bucketName, &object.ListUploadsOptions{
				Checksum: true,
			})
			require.True(t, iter.Next())
			require.Equal(t, checksum, iter.Item().Checksum)
		})

		t.Run("Begin upload - Invalid checksum options", func(t *testing.T) {
			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			for _, tt := range invalidChecksumScenarios {
				// The checksum value is allowed to be omitted when beginning uploads.
				if tt.checksum.Algorithm != storj.ObjectChecksumAlgorithmNone && len(tt.checksum.Value) == 0 {
					continue
				}

				_, err := object.BeginUpload(ctx, project, bucketName, objectKey, &object.MultipartUploadOptions{
					UserData: metaclient.ObjectUserData{
						Checksum: tt.checksum,
					},
				})
				require.ErrorContains(t, err, tt.errMsg, "test case: %q", tt.errMsg)
				requireNoPendingObjects(t, bucketName, "test case: %q", tt.errMsg)
			}
		})

		t.Run("Begin upload - Incomplete checksum options", func(t *testing.T) {
			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			incompleteChecksum := checksum
			incompleteChecksum.Value = nil

			_, err := object.BeginUpload(ctx, project, bucketName, objectKey, &object.MultipartUploadOptions{
				UserData: metaclient.ObjectUserData{
					Checksum: incompleteChecksum,
				},
			})
			require.NoError(t, err)
		})

		t.Run("Begin upload - Checksums unsupported", func(t *testing.T) {
			sat.Metainfo.Endpoint.TestingSetChecksumsEnabled(false)
			defer sat.Metainfo.Endpoint.TestingSetChecksumsEnabled(true)

			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			_, err := object.BeginUpload(ctx, project, bucketName, objectKey, &object.MultipartUploadOptions{
				UserData: metaclient.ObjectUserData{
					Checksum: checksum,
				},
			})
			require.ErrorIs(t, err, object.ErrChecksumsUnsupported)
		})

		t.Run("Upload part", func(t *testing.T) {
			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			upload, err := object.BeginUpload(ctx, project, bucketName, objectKey, nil)
			require.NoError(t, err)

			part, err := object.UploadPart(ctx, project, bucketName, objectKey, upload.UploadID, 1)
			require.NoError(t, err)

			_, err = part.Write(testrand.Bytes(32))
			require.NoError(t, err)

			require.NoError(t, part.SetChecksum(partChecksum))

			require.NoError(t, part.Commit())

			iter := object.ListUploadParts(ctx, project, bucketName, objectKey, upload.UploadID, nil)
			require.True(t, iter.Next())
			require.Equal(t, partChecksum, iter.Item().Checksum)
		})

		t.Run("Commit part - Checksums unsupported", func(t *testing.T) {
			sat.Metainfo.Endpoint.TestingSetChecksumsEnabled(false)
			defer sat.Metainfo.Endpoint.TestingSetChecksumsEnabled(true)

			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			upload, err := object.BeginUpload(ctx, project, bucketName, objectKey, nil)
			require.NoError(t, err)

			part, err := object.UploadPart(ctx, project, bucketName, objectKey, upload.UploadID, 1)
			require.NoError(t, err)

			_, err = part.Write(testrand.Bytes(32))
			require.NoError(t, err)

			require.NoError(t, part.SetChecksum(partChecksum))

			require.ErrorIs(t, part.Commit(), object.ErrChecksumsUnsupported)
		})

		t.Run("Commit upload", func(t *testing.T) {
			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			uploadID, err := beginUploadWithPart(bucketName, objectKey)
			require.NoError(t, err)

			_, err = object.CommitUpload(ctx, project, bucketName, objectKey, uploadID, &metaclient.CommitUploadOptions{
				UserData: metaclient.ObjectUserData{
					Checksum: checksum,
				},
			})
			require.NoError(t, err)

			statObj, err := object.StatObject(ctx, project, bucketName, objectKey, nil)
			require.NoError(t, err)

			require.EqualValues(t, checksum, statObj.Checksum)
		})

		t.Run("Commit upload - Invalid checksum options", func(t *testing.T) {
			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			uploadID, err := beginUploadWithPart(bucketName, objectKey)
			require.NoError(t, err)

			for _, tt := range invalidChecksumScenarios {
				_, err = object.CommitUpload(ctx, project, bucketName, objectKey, uploadID, &metaclient.CommitUploadOptions{
					UserData: metaclient.ObjectUserData{
						Checksum: tt.checksum,
					},
				})
				require.ErrorContains(t, err, tt.errMsg, "test case: %q", tt.errMsg)
				requireNoCommittedObject(t, bucketName, objectKey, "test case: %q", tt.errMsg)
			}
		})

		t.Run("Commit upload - Checksums unsupported", func(t *testing.T) {
			sat.Metainfo.Endpoint.TestingSetChecksumsEnabled(false)
			defer sat.Metainfo.Endpoint.TestingSetChecksumsEnabled(true)

			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			uploadID, err := beginUploadWithPart(bucketName, objectKey)
			require.NoError(t, err)

			_, err = object.CommitUpload(ctx, project, bucketName, objectKey, uploadID, &metaclient.CommitUploadOptions{
				UserData: metaclient.ObjectUserData{
					Checksum: checksum,
				},
			})
			require.ErrorIs(t, err, object.ErrChecksumsUnsupported)
		})
	})
}

func TestGetUploadMetadata(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: func(log *zap.Logger, index int, config *satellite.Config) {
				config.Metainfo.ChecksumsEnabled = true
			},
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		sat := planet.Satellites[0]
		up := planet.Uplinks[0]

		objectKey := "test-object"

		project, err := up.OpenProject(ctx, sat)
		require.NoError(t, err)
		defer ctx.Check(project.Close)

		expectedUserData := metaclient.ObjectUserData{
			Custom: map[string]string{
				"key": "value",
			},
			ETag: []byte("etag"),
			Checksum: metaclient.ObjectChecksum{
				Algorithm:   storj.ObjectChecksumAlgorithmCRC32,
				IsComposite: true,
				Value:       []byte("checksum"),
			},
		}

		t.Run("Success", func(t *testing.T) {
			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			upload, err := object.BeginUpload(ctx, project, bucketName, objectKey, &object.MultipartUploadOptions{
				UserData: expectedUserData,
			})
			require.NoError(t, err)

			userData, err := object.GetUploadMetadata(ctx, project, bucketName, objectKey, upload.UploadID)
			require.NoError(t, err)
			require.Equal(t, expectedUserData, userData)
		})

		t.Run("Missing object", func(t *testing.T) {
			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			upload, err := object.BeginUpload(ctx, project, bucketName, objectKey, &object.MultipartUploadOptions{
				UserData: expectedUserData,
			})
			require.NoError(t, err)

			require.NoError(t, project.AbortUpload(ctx, bucketName, objectKey, upload.UploadID))

			_, err = object.GetUploadMetadata(ctx, project, testrand.BucketName(), objectKey, upload.UploadID)
			require.ErrorIs(t, err, uplink.ErrObjectNotFound)
		})

		t.Run("Invalid upload ID", func(t *testing.T) {
			bucketName := testrand.BucketName()
			require.NoError(t, up.CreateBucket(ctx, sat, bucketName))

			upload, err := object.BeginUpload(ctx, project, bucketName, objectKey, &object.MultipartUploadOptions{
				UserData: expectedUserData,
			})
			require.NoError(t, err)

			// Invalid base58 string
			uploadID := "!@#$%"
			_, err = object.GetUploadMetadata(ctx, project, bucketName, objectKey, uploadID)
			require.ErrorIs(t, err, uplink.ErrUploadIDInvalid)

			// Invalid encoded data
			uploadID = base58.Encode(testrand.Bytes(32))
			_, err = object.GetUploadMetadata(ctx, project, bucketName, objectKey, uploadID)
			require.ErrorIs(t, err, uplink.ErrUploadIDInvalid)

			// The satellite returns an "invalid stream ID" error if the stream ID fails
			// signature verification. Confirm that we properly translate this error.
			uploadIDBytes, version, err := base58.CheckDecode(upload.UploadID)
			require.NoError(t, err)

			var internalStreamID internalpb.StreamID
			require.NoError(t, pb.Unmarshal(uploadIDBytes, &internalStreamID))
			internalStreamID.SatelliteSignature = testrand.Bytes(32)

			uploadIDBytes, err = pb.Marshal(&internalStreamID)
			require.NoError(t, err)

			uploadID = base58.CheckEncode(uploadIDBytes, version)
			_, err = object.GetUploadMetadata(ctx, project, bucketName, objectKey, uploadID)
			require.ErrorIs(t, err, uplink.ErrUploadIDInvalid)
		})
	})
}

func assertUploadList(ctx context.Context, t *testing.T, project *uplink.Project, bucket string, options *uplink.ListUploadsOptions, objectKeys ...string) {
	list := project.ListUploads(ctx, bucket, options)
	require.NoError(t, list.Err())
	require.Nil(t, list.Item())

	itemKeys := make(map[string]struct{})
	for list.Next() {
		require.NoError(t, list.Err())
		require.NotNil(t, list.Item())
		require.False(t, list.Item().IsPrefix)
		itemKeys[list.Item().Key] = struct{}{}
	}

	for _, objectKey := range objectKeys {
		if assert.Contains(t, itemKeys, objectKey) {
			delete(itemKeys, objectKey)
		}
	}

	require.Empty(t, itemKeys)

	require.False(t, list.Next())
	require.NoError(t, list.Err())
	require.Nil(t, list.Item())
}
