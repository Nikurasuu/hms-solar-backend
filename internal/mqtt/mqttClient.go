package mqtt

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nikurasuu/hms-solar-backend/internal/entity"
	"github.com/sirupsen/logrus"
)

type Client struct {
	client mqtt.Client
	logger *logrus.Logger
}

func NewClient(broker string, clientId string, logger *logrus.Logger) *Client {
	opts := mqtt.NewClientOptions().AddBroker(broker).SetClientID(clientId)
	opts.SetKeepAlive(2 * time.Second)
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		logger.Infof("Received message: %s from topic: %s", msg.Payload(), msg.Topic())
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		logger.Fatalf("Error connecting to the broker: %v", token.Error())
		os.Exit(1)
	}

	logger.Infof("Connected to the broker: %s", broker)

	return &Client{client: client, logger: logger}
}

func (c *Client) SubscribeToSolarData(solarData *entity.SolarData) {
	c.client.Subscribe("hms800wt2/#", 0, func(client mqtt.Client, msg mqtt.Message) {
		topicParts := strings.Split(msg.Topic(), "/")
		topicKey := topicParts[len(topicParts)-1]

		switch topicKey {
		case entity.PvCurrentPowerTopic:
			if err := json.Unmarshal(msg.Payload(), &solarData.PvCurrentPower); err != nil {
				c.logger.Errorf("Error unmarshalling the payload: %v", err)
				return
			}
		case entity.PvDailyYieldTopic:
			if err := json.Unmarshal(msg.Payload(), &solarData.PvDailyYield); err != nil {
				c.logger.Errorf("Error unmarshalling the payload: %v", err)
				return
			}
		}
	})
}
