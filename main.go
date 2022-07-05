package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

var (
	brokers             = []string{"127.0.0.1:9092"}
	topic   goka.Stream = "random-no-upto-five"
	group   goka.Group  = "mini-random-group-upto-five"

	tmc *goka.TopicManagerConfig
)

// A user is the object that is stored in the processor's group table
type user struct {
	EarlierTime string
	LatestTime  string
	TotalValue  int64
}

// This codec allows marshalling (encode) and unmarshalling (decode) the user to and from the
// group table.
type userCodec struct{}

func init() {
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

// Encodes a user into []byte
func (jc *userCodec) Encode(value interface{}) ([]byte, error) {
	if _, isUser := value.(*user); !isUser {
		return nil, fmt.Errorf("Codec requires value *user, got %T", value)
	}
	return json.Marshal(value)
}

// Decodes a user from []byte to it's go representation.
func (jc *userCodec) Decode(data []byte) (interface{}, error) {
	var (
		c   user
		err error
	)
	err = json.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling user: %v", err)
	}
	return &c, nil
}

func runEmitter() {
	emitter, err := goka.NewEmitter(brokers, topic,
		new(codec.String))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	var i int
	for range t.C {
		var u user
		u.LatestTime = time.Now().String()
		u.TotalValue = int64(i % 10)
		key := fmt.Sprintf("%d", i%6)
		emitter, err := goka.NewEmitter(brokers, topic, new(userCodec))
		if err != nil {
			panic(err)
		}
		defer emitter.Finish()
		emitter.EmitSync(key, &u)
		i++
	}
}

func process(ctx goka.Context, msg interface{}) {
	var u *user
	m := msg.(*user)
	if val := ctx.Value(); val != nil {
		u = val.(*user)
		if u.EarlierTime == "" {
			u.EarlierTime = m.LatestTime
		}
	} else {
		u = new(user)
		u.EarlierTime = m.LatestTime
	}

	u.LatestTime = m.LatestTime
	u.TotalValue += m.TotalValue
	ctx.SetValue(u)
	fmt.Printf("[proc] key: %s clicks: %d, msg: %v\n", ctx.Key(), u.TotalValue, msg)
}

func runProcessor(initialized chan struct{}) {
	g := goka.DefineGroup(group,
		goka.Input(topic, new(userCodec), process),
		goka.Persist(new(userCodec)),
	)
	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		panic(err)
	}

	close(initialized)

	p.Run(context.Background())
}

func runView(initialized chan struct{}) {
	<-initialized

	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(userCodec),
	)
	if err != nil {
		panic(err)
	}

	root := mux.NewRouter()
	root.HandleFunc("/features/{key}", func(w http.ResponseWriter, r *http.Request) {
		value, _ := view.Get(mux.Vars(r)["key"])
		data, _ := json.Marshal(value)
		w.Write(data)
	})
	fmt.Println("View opened at http://localhost:9095/")
	go http.ListenAndServe(":9095", root)

	view.Run(context.Background())
}

func main() {
	tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()
	err = tm.EnsureStreamExists(string(topic), 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", topic, err)
	}

	// When this example is run the first time, wait for creation of all internal topics (this is done
	// by goka.NewProcessor)
	initialized := make(chan struct{})

	go runEmitter()
	go runProcessor(initialized)
	runView(initialized)
}
