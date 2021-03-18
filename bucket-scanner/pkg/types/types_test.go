package types_test

import (
	"encoding/json"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/scality/backbeat/bucket-scanner/pkg/types"
)

func unescapeJSON(s string) []byte {
	var u string
	_ = json.Unmarshal([]byte(s), &u)
	return []byte(u)
}

var _ = Describe("Types", func() {
	objectMD := &types.ObjectMetadata{
		ReplicationInfo: &types.ObjectReplicationInfo{
			Status: types.ReplicationStatusFailed,
			Backends: []types.ReplicationBackendStatus{
				{Site: "site1", Status: types.ReplicationStatusPending, DataStoreVersionID: "v1"},
				{Site: "site2", Status: types.ReplicationStatusFailed},
			},
		},
		Key:    "k",
		Bucket: "b",
	}
	objectMDStr := `"{\"replicationInfo\":{\"status\":\"FAILED\",\"backends\":[{\"site\":\"site1\",\"status\":\"PENDING\",\"dataStoreVersionId\":\"v1\"},{\"site\":\"site2\",\"status\":\"FAILED\"}]}}"`
	memberStr := "b:obj-key:393833393136383839373034363039393939393952473030312020382e3737342e313138:arn%3Aaws%3Aiam%3A%3A401219569273%3Arole%2Fbb-replication-1607386758902"

	expectedHash := types.HashString("b:b,k:k")

	Describe("Parsers", func() {
		cases := []struct {
			name   string
			parser func([]byte) (interface{}, error)
			input  []byte
			ref    interface{}
		}{
			{
				name: "ObjectMD",
				parser: func(buf []byte) (interface{}, error) {
					return types.ParseObjectMetadata("b", "k", string(buf))
				},
				input: unescapeJSON(objectMDStr),
				ref:   objectMD,
			},
			{
				name: "StatusMessage",
				parser: func(buf []byte) (interface{}, error) {
					return types.ParseStatusMessage(buf)
				},
				input: []byte(fmt.Sprintf(`{
					"bucket": "b",
					"key": "k",
					"value": %s
				}`, objectMDStr)),
				ref: &types.StatusMessage{
					Bucket:      "b",
					Key:         "k",
					ParsedValue: objectMD,
				},
			},
			{
				name: "ReplicationMessage",
				parser: func(buf []byte) (interface{}, error) {
					return types.ParseReplicationMessage(buf)
				},
				input: []byte(fmt.Sprintf(`{
					"type": "put",
					"bucket": "b",
					"key": "k",
					"value": %s
				}`, objectMDStr)),
				ref: &types.ReplicationMessage{
					Type:        "put",
					Bucket:      "b",
					Key:         "k",
					ParsedValue: objectMD,
				},
			},
			{
				name: "FailedMessage",
				parser: func(buf []byte) (interface{}, error) {
					return types.ParseFailedMessage(buf)
				},
				input: []byte(fmt.Sprintf(`{
					"member": "%s",
					"key": "k",
					"score": 2
				}`, memberStr)),
				ref: &types.FailedMessage{
					Key:    "k",
					Score:  2,
					Member: memberStr,
				},
			},
		}

		for _, c := range cases {
			c := c
			It("should parse "+c.name, func() {
				o, err := c.parser(c.input)
				Expect(err).NotTo(HaveOccurred())
				Expect(o).To(Equal(c.ref))
			})

			It("should hash "+c.name, func() {
				o, err := c.parser(c.input)
				Expect(err).NotTo(HaveOccurred())
				Expect(o.(types.HashedNamer).HashedName()).To(Equal(expectedHash))
			})
		}
	})

})
