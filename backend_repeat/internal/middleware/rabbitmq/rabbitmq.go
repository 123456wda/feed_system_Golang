package rabbitmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/observability"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
}

func NewRabbitMQ(cfg *config.RabbitMQConfig) (*RabbitMQ, error) {
	if cfg == nil {
		return nil, errors.New("rabbitmq config is nil")
	}
	url := "amqp://" + cfg.Username + ":" + cfg.Password + "@" + cfg.Host + ":" + strconv.Itoa(cfg.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &RabbitMQ{Conn: conn, Ch: ch}, nil
}

func (r *RabbitMQ) Close() error {
	if r == nil || r.Ch == nil || r.Conn == nil {
		return nil
	}
	if err := r.Ch.Close(); err != nil {
		return err
	}
	if err := r.Conn.Close(); err != nil {
		return err
	}
	return nil
}

// 通常是使用消息队列前或者在启动链路提前声明好要用到的队列和交换机
func (r *RabbitMQ) DeclareTopic(exchange string, queue string, bindingKey string) error {
	if r == nil || r.Ch == nil || r.Conn == nil {
		return errors.New("RabbitMQ is not initialized")
	}

	if exchange == "" || queue == "" || bindingKey == "" {
		return errors.New("exchange,queue,bindingKey cannot be empty!")
	}

	// 声明交换机
	if err := r.Ch.ExchangeDeclare(
		exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	// 声明一个消息队列
	q, err := r.Ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 绑定队列
	if err := r.Ch.QueueBind(
		q.Name,
		bindingKey,
		exchange,
		false,
		nil,
	); err != nil {
		return err
	}

	return nil
}

// 封装好以json格式发送消息的函数
func (r *RabbitMQ) PublishJSON(ctx context.Context, exchange string, routineKey string, data any) error {
	if r == nil || r.Ch == nil || r.Conn == nil {
		return errors.New("RabbitMQ is not initialized")
	}

	if exchange == "" || routineKey == "" || data == nil {
		return errors.New("exchange,routineKey,data cannot be empty!")
	}

	// json化消息
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// 发送消息
	err = r.Ch.PublishWithContext(ctx, exchange, routineKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Timestamp:    time.Now(),
	})
	if err != nil {
		return err
	}
	observability.MQMessagesPublished.WithLabelValues(exchange, routineKey).Inc()
	return nil
}

func newEventID(n int) (string, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// IncrConsumed 递增消费计数，供 Worker 调用
func IncrConsumed(queue string) {
	observability.MQMessagesConsumed.WithLabelValues(queue).Inc()
}
