package deletetopics_test

import (
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/deletetopics"
	"github.com/segmentio/kafka-go/protocol/prototest"
	"github.com/stretchr/testify/assert"
)

const (
	v0 = 0
	v1 = 1
	v3 = 3
)

func TestSplitSmallRequest(t *testing.T) {
	request := &deletetopics.Request{
		TopicNames: []string{"foo", "bar"},
		TimeoutMs:  500,
	}
	dummyCluster := protocol.Cluster{}
	messages, _, _ := request.Split(dummyCluster)
	assert.Equal(t, 1, len(messages))
}

func TestSplitEvenRequest(t *testing.T) {
	request := &deletetopics.Request{
		TopicNames: []string{},
		TimeoutMs:  500,
	}

	for i := range 20 {
		request.TopicNames = append(request.TopicNames, fmt.Sprintf("%d", i))
	}

	dummyCluster := protocol.Cluster{}
	messages, _, _ := request.Split(dummyCluster)

	assert.Equal(t, 1, len(messages))
	for i := range 20 {
		assert.Equal(t, messages[0].(*deletetopics.Request).TopicNames[i], fmt.Sprintf("%d", i))
	}
}

func TestSplitUnevenRequest(t *testing.T) {
	request := &deletetopics.Request{
		TopicNames: []string{},
		TimeoutMs:  500,
	}

	for i := range 23 {
		request.TopicNames = append(request.TopicNames, fmt.Sprintf("%d", i))
	}

	dummyCluster := protocol.Cluster{}
	messages, _, _ := request.Split(dummyCluster)

	assert.Equal(t, 2, len(messages))
	for i := range 20 {
		assert.Equal(t, fmt.Sprintf("%d", i), messages[0].(*deletetopics.Request).TopicNames[i])
	}
	for i := 0; i < 3; i++ {
		assert.Equal(t, fmt.Sprintf("%d", i+20), messages[1].(*deletetopics.Request).TopicNames[i])
	}
}

func TestDeleteTopicsRequest(t *testing.T) {
	prototest.TestRequest(t, v0, &deletetopics.Request{
		TopicNames: []string{"foo", "bar"},
		TimeoutMs:  500,
	})

	prototest.TestRequest(t, v1, &deletetopics.Request{
		TopicNames: []string{"foo", "bar"},
		TimeoutMs:  500,
	})

	prototest.TestRequest(t, v3, &deletetopics.Request{
		TopicNames: []string{"foo", "bar"},
		TimeoutMs:  500,
	})
}

func TestDeleteTopicsResponse(t *testing.T) {
	prototest.TestResponse(t, v0, &deletetopics.Response{
		Responses: []deletetopics.ResponseTopic{
			{
				Name:      "foo",
				ErrorCode: 1,
			},
			{
				Name:      "bar",
				ErrorCode: 1,
			},
		},
	})

	prototest.TestResponse(t, v1, &deletetopics.Response{
		ThrottleTimeMs: 500,
		Responses: []deletetopics.ResponseTopic{
			{
				Name:      "foo",
				ErrorCode: 1,
			},
			{
				Name:      "bar",
				ErrorCode: 1,
			},
		},
	})

	prototest.TestResponse(t, v3, &deletetopics.Response{
		ThrottleTimeMs: 500,
		Responses: []deletetopics.ResponseTopic{
			{
				Name:      "foo",
				ErrorCode: 1,
			},
			{
				Name:      "bar",
				ErrorCode: 1,
			},
		},
	})
}
