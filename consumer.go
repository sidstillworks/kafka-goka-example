package main

import (
	"context"
	"fmt"

	"github.com/lovoo/goka"
)

func process(ctx goka.Context, msg interface{}) {
	var u *user
	m := msg.(*user)
	fmt.Println("message: ", m)
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

	fmt.Println("consumer started..")

	close(initialized)

	p.Run(context.Background())
}
