package main

import (
	"io/ioutil"
	"encoding/json"
	"fmt"
	"log"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type GameData struct {
	TotalGames int `json:"totalGames"`
  	Dates []*GameDates `json:"dates"`
}

type GameDates struct {
    Date string `json:"date"`
    Games []*Game `json:"games"`
}

type Game struct {
	GamePk int `json:"gamePk"`
	Link string `json:"link"`
	GameType string `json:"gameType"`
	Season string `json:"season"`
	GameDate string `json:"gameDate"`
	GamesInSeries int `json:"gamesInSeries"`
	SeriesGameNumber int `json:"seriesGameNumber"`
    Status *GameStatus `json:"status"`
    Teams *GameTeams `json:"teams"`
    LineScore *LineScore `json:"linescore"`
}

type GameStatus struct {
	StatusCode string `json:"statusCode"`
}

type GameTeams struct {
	Home *GameTeam `json:"away"`
	Away *GameTeam `json:"home"`
}

type GameTeam struct {
	Score int `json:"score"`
	SeriesNumber int `json:"seriesNumber"`
	Team *Team `json:"team"`
}

type Team struct {
	Id int `json:"id"`
	Name string `json:"name"`
	Abbreviation string `json:"abbreviation"`
}

type LineScore struct {
	Innings []*Inning `json:"innings"`
	Teams *ScoreTeams `json:"teams"`
}

type Inning struct {
	Num int `json:"num"`
	OrdinalNum string `json:"ordinalNum"`
	Home *Score `json:"home"`
	Away *Score `json:"away"`
}

type ScoreTeams struct {
	Home *Score `json:"home"`
	Away *Score `json:"away"`
}

type Score struct {
	Runs int `json:"runs"`
	Hits int `json:"hits"`
	Errors int `json:"errors"`
}

func main() {

	var broker string = "localhost:9092"
	var topic string = "mlb"

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
    	log.Fatal(err)
    }


	j, err := ioutil.ReadFile("data/2017-05-27.json")
    if err != nil {
    	log.Fatal(err)
    }
    
	dat := GameData{}

	if err := json.Unmarshal(j, &dat); err != nil {
		log.Fatal(err)
	}

	deliveryChan := make(chan bool)

	go func() {
	outer:
		for e := range p.Events() {
			fmt.Println("x")
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				break outer

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}

		close(deliveryChan)
	}()

	for _, date := range dat.Dates {
		for _, game := range date.Games {
			gameJson, _ := json.Marshal(game)
			p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(string(gameJson))}
		}
	}

	_ = <- deliveryChan

	p.Close()
}
