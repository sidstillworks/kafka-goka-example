package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/linkedin/goavro"
	"github.com/lovoo/goka"
)

var (
	brokers             = []string{"127.0.0.1:9092"}
	topic   goka.Stream = "random-no-upto-five"
	group   goka.Group  = "mini-random-group-upto-five"

	tmc       *goka.TopicManagerConfig
	avrocodec *goavro.Codec
)

// A user is the object that is stored in the processor's group table
type user struct {
	EarlierTime string
	LatestTime  string
	TotalValue  int32
}

// This codec allows marshalling (encode) and unmarshalling (decode) the user to and from the
// group table.
type userCodec struct{}

func init() {
	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
}

func serialiseMap(value interface{}) map[string]interface{} {
	userMap := make(map[string]interface{})

	userMap["EarlierTime"] = value.(*user).EarlierTime
	userMap["LatestTime"] = value.(*user).LatestTime
	userMap["TotalValue"] = value.(*user).TotalValue

	return userMap
}

func deSerialiseMap(value interface{}) user {
	var newUser user

	newUser.EarlierTime = value.(map[string]interface{})["EarlierTime"].(string)
	newUser.LatestTime = value.(map[string]interface{})["LatestTime"].(string)
	newUser.TotalValue = value.(map[string]interface{})["TotalValue"].(int32)

	return newUser
}

// Encodes a user into []byte
func (codec userCodec) Encode(value interface{}) ([]byte, error) {
	if _, isUser := value.(*user); !isUser {
		return nil, fmt.Errorf("Codec requires value *user, got %T", value)
	}

	userMap := serialiseMap(value)

	binary, err := avrocodec.BinaryFromNative(nil, userMap)
	if err != nil {
		return nil, fmt.Errorf("Error marshaling user: %v", err)
	}
	return binary, nil
}

// Decodes a user from []byte to it's go representation.
func (codec userCodec) Decode(data []byte) (interface{}, error) {
	native, _, err := avrocodec.NativeFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling user: %v", err)
	}

	newUser := deSerialiseMap(native)
	return &newUser, nil
}

func runEmitter(avrocodec *goavro.Codec) {
	emitter, err := goka.NewEmitter(brokers, topic,
		new(userCodec))
	if err != nil {
		panic(err)
	}
	defer emitter.Finish()

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	var i int
	for range t.C {
		var u user
		u.LatestTime = time.Now().String()
		u.TotalValue = int32(i % 10)
		key := fmt.Sprintf("%d", i%6)
		emitter.EmitSync(key, &u)
		i++
	}
}

func process(ctx goka.Context, msg interface{}) {
	var u *user
	m := msg.(*user)
	if val := ctx.Value(); val != nil {
		u = val.(*user)
		if u.EarlierTime == "" || len(u.EarlierTime) <= 0 {
			u.EarlierTime = m.LatestTime
		}
	} else {
		u = new(user)
		u.EarlierTime = m.LatestTime
	}

	u.LatestTime = m.LatestTime
	u.TotalValue += m.TotalValue
	ctx.SetValue(u)
	fmt.Printf("[proc] key: %s clicks: %d, msg: %v, userTable: %v, ctxVal: %v\n", ctx.Key(), u.TotalValue, msg, u, ctx.Value())
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
	avrocodec, _ = goavro.NewCodec(`
		{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "EarlierTime", "type" : "string", "default": ""},
				{"name": "LatestTime", "type" : "string", "default": ""},
				{"name": "TotalValue", "type" : "int"}
			]
		}`)

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

	go runEmitter(avrocodec)
	go runProcessor(initialized)
	runView(initialized)
}
