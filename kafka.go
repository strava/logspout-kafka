package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/gliderlabs/logspout/router"
	"gopkg.in/Shopify/sarama.v1"
)

func init() {
	router.AdapterFactories.Register(NewKafkaAdapter, "kafka")
}

type KafkaAdapter struct {
	route    *router.Route
	brokers  []string
	topic    string
	producer sarama.AsyncProducer
	tmpl     *template.Template
}

// Strava specific structs
type DockerFields struct {
	Name       string            `json:"name"`
	CID        string            `json:"cid"`
	Image      string            `json:"image"`
	ImageTag   string            `json:"image_tag,omitempty"`
	Source     string            `json:"source"`
	// DockerHost string            `json:"docker_host,omitempty"` // I think this is a redis specific thing?
	Labels     map[string]string `json:"labels,omitempty"`
}

type MarathonFields struct {
	Id      *string `json:"id,omitempty"`
	Version *string `json:"version,omitempty"`
}

type MesosFields struct {
	TaskId *string `json:"task_id,omitempty"`
}

type LogstashFields struct {
	Docker DockerFields `json:"docker"`
}

type LogstashMessage struct {
	// Type       string  `json:"type"`
	Timestamp  string  `json:"@timestamp"`
	Sourcehost *string `json:"host,omitempty"`

	Data           map[string]interface{} `json:"data"`
	DockerFields   DockerFields           `json:"docker"`
	MarathonFields MarathonFields         `json:"marathon"`
	MesosFields    MesosFields            `json:"mesos"`
}

func NewKafkaAdapter(route *router.Route) (router.LogAdapter, error) {
	brokers := readBrokers(route.Address)
	if len(brokers) == 0 {
		return nil, errorf("The Kafka broker host:port is missing. Did you specify it as a route address?")
	}

	topic := readTopic(route.Address, route.Options)
	if topic == "" {
		return nil, errorf("The Kafka topic is missing. Did you specify it as a route option?")
	}

	var err error
	var tmpl *template.Template
	if text := os.Getenv("KAFKA_TEMPLATE"); text != "" {
		tmpl, err = template.New("kafka").Parse(text)
		if err != nil {
			return nil, errorf("Couldn't parse Kafka message template. %v", err)
		}
	}

	if os.Getenv("DEBUG") != "" {
		log.Printf("Starting Kafka producer for address: %s, topic: %s.\n", brokers, topic)
	}

	var retries int
	retries, err = strconv.Atoi(os.Getenv("KAFKA_CONNECT_RETRIES"))
	if err != nil {
		retries = 3
	}
	var producer sarama.AsyncProducer
	for i := 0; i < retries; i++ {
		producer, err = sarama.NewAsyncProducer(brokers, newConfig())
		if err != nil {
			if os.Getenv("DEBUG") != "" {
				log.Println("Couldn't create Kafka producer. Retrying...", err)
			}
			if i == retries-1 {
				return nil, errorf("Couldn't create Kafka producer. %v", err)
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}

	return &KafkaAdapter{
		route:    route,
		brokers:  brokers,
		topic:    topic,
		producer: producer,
		tmpl:     tmpl,
	}, nil
}

func (a *KafkaAdapter) Stream(logstream chan *router.Message) {
	defer a.producer.Close()
	for rm := range logstream {
		// filter for JSON messages here
		log.Println(rm)
		if !json.Valid([]byte(rm.Data)) {
			continue
		}

		// message, err := a.formatMessage(rm)
		message, err := a.formatToLogstashMessage(rm)
		if err != nil {
			log.Println("kafka:", err)
			a.route.Close()
			break
		}

		a.producer.Input() <- message
	}
}

func newConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = "logspout"
	config.Producer.Return.Errors = false
	config.Producer.Return.Successes = false
	config.Producer.Flush.Frequency = 1 * time.Second
	config.Producer.RequiredAcks = sarama.WaitForLocal

	if opt := os.Getenv("KAFKA_COMPRESSION_CODEC"); opt != "" {
		switch opt {
		case "gzip":
			config.Producer.Compression = sarama.CompressionGZIP
		case "snappy":
			config.Producer.Compression = sarama.CompressionSnappy
		}
	}

	return config
}

/**
// Original method
func (a *KafkaAdapter) formatMessage(message *router.Message) (*sarama.ProducerMessage, error) {
	var encoder sarama.Encoder
	if a.tmpl != nil {
		var w bytes.Buffer
		if err := a.tmpl.Execute(&w, message); err != nil { // where does a.tmpl.Execute output to?
			return nil, err
		}
		encoder = sarama.ByteEncoder(w.Bytes()) // encode the bytes.Buffer
	} else {
		encoder = sarama.StringEncoder(message.Data)
	}

	return &sarama.ProducerMessage{
		Topic: a.topic,
		Value: encoder,
	}, nil
}
**/

/* *
	Strava Logstash Message
	Annotated with the marathon/mesos information that's only available on the particular instance
	https://github.com/gliderlabs/logspout/blob/5abc836e8cabcaebd862d981fa7fbea8798ff4d0/router/types.go#L52
	Logspout Message Struct

	// Message is a log messages
	type Message struct {
		Container *docker.Container // the fsouza docker container
		Source    string // stdout, stdin etc
		Data      string // the actual data
		Time      time.Time
	}
* */
func (a *KafkaAdapter) formatToLogstashMessage(message *router.Message) (*sarama.ProducerMessage, error) {
	var encoder sarama.Encoder

	// ignore the template variable
	image_name, image_tag := splitImage(m.Container.Config.Image)

	logstash_message := LogstashMessage{
		// Type:, // Might remove this
		Data: message.data,
		Timestamp: message.Time.Format(time.RFC3339Nano), // Use the timestamp in the message
		Sourcehost: envValue("HOST", message.Container.Config.Env),
		DockerFields: DockerFields{
			CID: message.Container.ID[0:12],
			Name: message.Container.Name[1:],
			Image: image_name,
			ImageTag: image_tag,
			Source: message.Source,
			// DockerHost: docker_host,
		},
		MarathonFields: MarathonFields{
			Id: appId := envValue("MARATHON_APP_ID", m.Container.Config.Env),
			Version: envValue("MARATHON_APP_VERSION", m.Container.Config.Env),
		},
		MesosFields: MesosFields{
			// Set by marathon, but general to mesos
			TaskId: envValue("MESOS_TASK_ID", m.Container.Config.Env),
		}
	}

	encoder = sarama.ByteEncoder(json.Marshal(logstash_message))

	// Note: ProducerMessage also has a "Timestamp" field
	// https://github.com/Shopify/sarama/blob/65f0fec86aabe011db77ad641d31fddf14f3ca41/async_producer.go
	return &sarama.ProducerMessage{
		Topic: a.topic,
		Value: encoder,
	}, nil
}

func envValue(target string, envVars []string) *string {
	for _, envVar := range envVars {
		s := strings.Split(envVar, "=")
		name := s[0]
		value := s[len(s)-1]
		if name == target {
			return &value
		}
	}
	return nil
}

func readBrokers(address string) []string {
	if strings.Contains(address, "/") {
		slash := strings.Index(address, "/")
		address = address[:slash]
	}

	return strings.Split(address, ",")
}

func readTopic(address string, options map[string]string) string {
	var topic string
	if !strings.Contains(address, "/") {
		topic = options["topic"]
	} else {
		slash := strings.Index(address, "/")
		topic = address[slash+1:]
	}

	return topic
}

func errorf(format string, a ...interface{}) (err error) {
	err = fmt.Errorf(format, a...)
	if os.Getenv("DEBUG") != "" {
		fmt.Println(err.Error())
	}
	return
}
