package types

import (
	"encoding/json"
	"fmt"
	"hash/maphash"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type ObjectHash uint64

type HashedNamer interface {
	fmt.Stringer
	HashedName() ObjectHash
}

type BucketReplicationConfiguration struct {
	Destination string
}

type BucketVersioningStatus string

const (
	BucketVersioningStatusEnabled   BucketVersioningStatus = "Enabled"
	BucketVersioningStatusSuspended                        = "Suspended"
	BucketVersioningStatusNone                             = ""
)

type BucketVersioningConfiguration struct {
	Status BucketVersioningStatus
}

type BucketMetadata struct {
	ReplicationConfiguration BucketReplicationConfiguration
	VersioningConfiguration  BucketVersioningConfiguration
}

type ReplicationStatus string
type SiteName string

const (
	ReplicationStatusPending    ReplicationStatus = "PENDING"
	ReplicationStatusProcessing ReplicationStatus = "PROCESSING"
	ReplicationStatusCompleted  ReplicationStatus = "COMPLETED"
	ReplicationStatusFailed     ReplicationStatus = "FAILED"
	ReplicationStatusReplica    ReplicationStatus = "REPLICA"
)

type ReplicationBackendStatus struct {
	Site               SiteName          `json:"site"`
	Status             ReplicationStatus `json:"status"`
	DataStoreVersionID string            `json:"dataStoreVersionId"`
}

type ObjectReplicationInfo struct {
	Status   ReplicationStatus          `json:"status"`
	Backends []ReplicationBackendStatus `json:"backends"`
}

var HashSeed = maphash.MakeSeed()

func HashString(s string) ObjectHash {
	var h maphash.Hash
	h.SetSeed(HashSeed)
	h.WriteString(s)
	return ObjectHash(h.Sum64())
}

func (objectReplicationInfo *ObjectReplicationInfo) String() string {
	backends, err := json.Marshal(objectReplicationInfo.Backends)
	if err != nil {
		backends = []byte("(invalid)")
	}

	return fmt.Sprintf("status=%s backends=%s", objectReplicationInfo.Status, backends)
}

type ObjectMetadata struct {
	ReplicationInfo *ObjectReplicationInfo `json:"replicationInfo"`

	// Not present in the actual blob but needed for name hashing
	Key    string
	Bucket string
}

func (objectMetadata *ObjectMetadata) String() string {
	if objectMetadata.Key == "" {
		panic("empty key")
	}

	if objectMetadata.Bucket == "" {
		panic("empty bucket")
	}

	return fmt.Sprintf("replicationInfo=(%s) key=(%s) bucket=(%s)", objectMetadata.ReplicationInfo, objectMetadata.Key, objectMetadata.Bucket)
}

func (objectMetadata *ObjectMetadata) HashedName() ObjectHash {
	if objectMetadata.Key == "" {
		panic("empty key")
	}

	if objectMetadata.Bucket == "" {
		panic("empty bucket")
	}

	key := "b:" + objectMetadata.Bucket + ",k:" + objectMetadata.Key
	return HashString(key)
}

func readObjectMetadata(bucket string, key string, b string) (*ObjectMetadata, error) {
	md := &ObjectMetadata{}

	err := json.Unmarshal([]byte(b), md)
	if err != nil {
		return nil, errors.Wrapf(err, "unmarshal object md '%s'", b)
	}

	md.Bucket = bucket
	md.Key = key

	return md, nil
}

func ParseObjectMetadata(bucket string, key string, blob string) (*ObjectMetadata, error) {
	return readObjectMetadata(bucket, key, blob)
}

type ReplicationMessage struct {
	Type        string `json:"type"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	StringValue string `json:"value"`
	ParsedValue *ObjectMetadata
}

func (replicationMessage *ReplicationMessage) String() string {
	return fmt.Sprintf("type=%s bucket=%s key=%s value=(%s)",
		replicationMessage.Type, replicationMessage.Bucket, replicationMessage.Key, replicationMessage.ParsedValue)
}

func (m *ReplicationMessage) HashedName() ObjectHash {
	key := "b:" + m.Bucket + ",k:" + m.Key
	return HashString(key)
}

func ParseReplicationMessage(b []byte) (*ReplicationMessage, error) {
	m := &ReplicationMessage{}

	err := json.Unmarshal(b, m)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal replication Message")
	}

	if m.StringValue != "" {
		md, err := readObjectMetadata(m.Bucket, m.Key, m.StringValue)
		if err != nil {
			return nil, errors.Wrap(err, "read embedded object metadata")
		}

		m.ParsedValue = md
		m.StringValue = ""
	}

	return m, nil
}

type FailedMessage struct {
	Key    string `json:"key"`
	Member string `json:"member"`
	Score  int64  `json:"score"`
}

func (m *FailedMessage) String() string {
	scoreAsDate := time.Unix(m.Score/1000, 0)

	return fmt.Sprintf("member=%v key=%v score=(value=%v asdate=%v)", m.Member, m.Key, m.Score, scoreAsDate)
}

func (m *FailedMessage) HashedName() ObjectHash {
	// example member: "repl-source:obj-key:393833393136383839373034363039393939393952473030312020382e3737342e313138:arn%3Aaws%3Aiam%3A%3A401219569273%3Arole%2Fbb-replication-1607386758902"
	bucket := strings.Split(m.Member, ":")[0] // TODO handle malformed
	key := "b:" + bucket + ",k:" + m.Key
	return HashString(key)
}

func ParseFailedMessage(b []byte) (*FailedMessage, error) {
	m := &FailedMessage{}

	err := json.Unmarshal(b, m)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal failed Message")
	}

	return m, nil
}

type StatusMessage struct {
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	StringValue string `json:"value"`
	ParsedValue *ObjectMetadata
}

func (m *StatusMessage) String() string {
	return fmt.Sprintf("bucket=%s key=%s value=(%s)", m.Bucket, m.Key, m.ParsedValue)
}

func (m *StatusMessage) HashedName() ObjectHash {
	key := "b:" + m.Bucket + ",k:" + m.Key
	return HashString(key)
}

func ParseStatusMessage(b []byte) (*StatusMessage, error) {
	m := &StatusMessage{}

	err := json.Unmarshal(b, m)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal status Message")
	}

	if m.StringValue != "" {
		md, err := readObjectMetadata(m.Bucket, m.Key, m.StringValue)
		if err != nil {
			return nil, errors.Wrap(err, "read embedded object metadata")
		}

		m.ParsedValue = md
		m.StringValue = ""
	}

	return m, nil
}
