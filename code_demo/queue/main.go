package main

import (
	"context"
	"fmt"
	"time"

	"Golang_Dream/code_demo/queue/mq"
)

var (
	topic = "Golang梦工厂"
)

func main() {
	OnceTopic()
	ManyTopic()
}

// OnceTopic 一个topic 测试
func OnceTopic() {
	ctx, cancel := context.WithCancel(context.Background())
	m := mq.NewClient()
	m.SetConditions(10)
	ch, err := m.Subscribe(topic)
	if err != nil {
		fmt.Println("subscribe failed")
		return
	}
	go OncePub(ctx, m)
	go OnceSub(ctx, ch, m)
	defer m.Close()

	time.Sleep(5 * time.Second)
	cancel()
}

// OncePub 定时推送
func OncePub(ctx context.Context, c *mq.Client) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := c.Publish(topic, "asong真帅")
			if err != nil {
				fmt.Printf("pub message failed, err: %v", err)
			}
		default:

		}
	}
}

// OnceSub 接受订阅消息
func OnceSub(ctx context.Context, m <-chan interface{}, c *mq.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			val := c.GetPayLoad(m)
			fmt.Printf("get message is %s\n", val)
		}
	}
}

// ManyTopic 多个topic测试
func ManyTopic() {
	ctx, cancel := context.WithCancel(context.Background())
	m := mq.NewClient()
	defer m.Close()
	m.SetConditions(10)
	top := ""
	for i := 0; i < 10; i++ {
		top = fmt.Sprintf("Golang梦工厂_%02d", i)
		go Sub(ctx, m, top)
	}
	go ManyPub(ctx, m)
	time.Sleep(5 * time.Second)
	cancel()
}

func ManyPub(ctx context.Context, c *mq.Client) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for i := 0; i < 10; i++ {
				//多个topic 推送不同的消息
				top := fmt.Sprintf("Golang梦工厂_%02d", i)
				payload := fmt.Sprintf("asong真帅_%02d", i)
				err := c.Publish(top, payload)
				if err != nil {
					//fmt.Printf("pub message failed, err: %v", err)
					fmt.Printf("pub message failed\n")
				}
			}
		default:

		}
	}
}

func Sub(ctx context.Context, c *mq.Client, top string) {
	ch, err := c.Subscribe(top)
	if err != nil {
		fmt.Printf("sub top:%s failed\n", top)
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			val := c.GetPayLoad(ch)
			if val != nil {
				fmt.Printf("%s get message is %s\n", top, val)
			}
		}
	}
}
