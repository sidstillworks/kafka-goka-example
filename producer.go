package main

import (
	"fmt"
	"log"
	"time"

	"github.com/linkedin/goavro"
	"github.com/lovoo/goka"
)

func runEmitter(avrocodec *goavro.Codec) {
	fmt.Println("Emitter started...")
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
		if err := emitter.EmitSync(key, &u); err != nil {
			log.Fatalf("Emit error: %v", err)
		}
		i++
		fmt.Println("msg produced...")
	}
}
