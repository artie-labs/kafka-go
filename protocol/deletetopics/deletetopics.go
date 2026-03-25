package deletetopics

import "github.com/segmentio/kafka-go/protocol"

func init() {
	protocol.Register(&Request{}, &Response{})
}

type Request struct {
	TopicNames []string `kafka:"min=v0,max=v3"`
	TimeoutMs  int32    `kafka:"min=v0,max=v3"`
}

func (r *Request) ApiKey() protocol.ApiKey { return protocol.DeleteTopics }

func (r *Request) Broker(cluster protocol.Cluster) (protocol.Broker, error) {
	return cluster.Brokers[cluster.Controller], nil
}

func (r *Request) Split(cluster protocol.Cluster) (
	[]protocol.Message,
	protocol.Merger,
	error,
) {

	// split into arbitrary buckets of 20 since we don't have anything super
	// significant to shard on
	bucketSize := min(len(r.TopicNames), 20)

	numEqualBuckets := len(r.TopicNames) / bucketSize

	lastBucketSize := len(r.TopicNames) % bucketSize

	var numMessages int
	if lastBucketSize > 0 {
		numMessages = numEqualBuckets + 1
	} else {
		numMessages = numEqualBuckets
	}

	messages := make([]protocol.Message, numMessages)

	for i := 0; i < numEqualBuckets; i++ {
		messages[0] = &Request{
			TopicNames: r.TopicNames[i*bucketSize : (i+1)*bucketSize],
			TimeoutMs:  r.TimeoutMs,
		}
	}
	
	if lastBucketSize > 0 {
		messages[len(messages)-1] = &Request{
			TopicNames: r.TopicNames[len(r.TopicNames)-lastBucketSize : len(r.TopicNames)],
		}
	}

	return messages, new(Response), nil
}

type Response struct {
	ThrottleTimeMs int32           `kafka:"min=v1,max=v3"`
	Responses      []ResponseTopic `kafka:"min=v0,max=v3"`
}

func (r *Response) Merge(requests []protocol.Message, results []interface{}) (
	protocol.Message,
	error,
) {
	response := &Response{}

	for _, result := range results {
		m, err := protocol.Result(result)
		if err != nil {
			return nil, err
		}
		response.ThrottleTimeMs = m.(*Response).ThrottleTimeMs
		response.Responses = append(response.Responses, m.(*Response).Responses...)
	}

	return response, nil
}

func (r *Response) ApiKey() protocol.ApiKey { return protocol.DeleteTopics }

type ResponseTopic struct {
	Name      string `kafka:"min=v0,max=v3"`
	ErrorCode int16  `kafka:"min=v0,max=v3"`
}

var (
	_ protocol.BrokerMessage = (*Request)(nil)
)
