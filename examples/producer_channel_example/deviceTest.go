// Example channel-based Apache Kafka producer
package main

/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
)

func main() {

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	topic := os.Args[2]

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				return

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	//value := "Hello Go!"
   for i := 0; i < 10000; i++ {
      value := `{"Imei":352094085830856,"Tstamp":"2018-11-19T09:49:19.548576642+07:00","NumRec":5,"DeviceRecords":[{"Tstamp":"2018-11-16T01:08:22+07:00","TstampInt":1542305302000,"Imei":352094085830856,"Longitude":106.7879433,"Latitude":-6.263135,"Altitude":46,"Angle":299,"Satelites":16,"Speed":0,"TagDevices":{"Ain1":{"TagName":"Ain1","TagId":"9","TagDataType":"int","TagVal":"121"},"Ain2":{"TagName":"Ain2","TagId":"10","TagDataType":"int","TagVal":"21"},"BatteryVolt":{"TagName":"BatteryVolt","TagId":"67","TagDataType":"int","TagVal":"9785"},"Din1":{"TagName":"Din1","TagId":"1","TagDataType":"int","TagVal":"1"},"Din2":{"TagName":"Din2","TagId":"2","TagDataType":"int","TagVal":"0"},"Din3":{"TagName":"Din3","TagId":"3","TagDataType":"int","TagVal":"0"},"Din4":{"TagName":"Din4","TagId":"4","TagDataType":"int","TagVal":"0"},"Dist":{"TagName":"Dist","TagId":"199","TagDataType":"float32","TagVal":"7"},"GsmSignalSensor":{"TagName":"GsmSignalSensor","TagId":"21","TagDataType":"int","TagVal":"4"},"MovementSensor":{"TagName":"MovementSensor","TagId":"240","TagDataType":"int","TagVal":"0"},"PowerVolt":{"TagName":"PowerVolt","TagId":"66","TagDataType":"int","TagVal":"14796"}}},{"Tstamp":"2018-11-16T01:13:22+07:00","TstampInt":1542305602000,"Imei":352094085830856,"Longitude":106.78794,"Latitude":-6.263105,"Altitude":38,"Angle":60,"Satelites":15,"Speed":0,"TagDevices":{"Ain1":{"TagName":"Ain1","TagId":"9","TagDataType":"int","TagVal":"121"},"Ain2":{"TagName":"Ain2","TagId":"10","TagDataType":"int","TagVal":"21"},"BatteryVolt":{"TagName":"BatteryVolt","TagId":"67","TagDataType":"int","TagVal":"9785"},"Din1":{"TagName":"Din1","TagId":"1","TagDataType":"int","TagVal":"1"},"Din2":{"TagName":"Din2","TagId":"2","TagDataType":"int","TagVal":"0"},"Din3":{"TagName":"Din3","TagId":"3","TagDataType":"int","TagVal":"0"},"Din4":{"TagName":"Din4","TagId":"4","TagDataType":"int","TagVal":"0"},"Dist":{"TagName":"Dist","TagId":"199","TagDataType":"float32","TagVal":"7"},"GsmSignalSensor":{"TagName":"GsmSignalSensor","TagId":"21","TagDataType":"int","TagVal":"4"},"MovementSensor":{"TagName":"MovementSensor","TagId":"240","TagDataType":"int","TagVal":"0"},"PowerVolt":{"TagName":"PowerVolt","TagId":"66","TagDataType":"int","TagVal":"14796"}}},{"Tstamp":"2018-11-16T01:18:22+07:00","TstampInt":1542305902000,"Imei":352094085830856,"Longitude":106.78794,"Latitude":-6.263105,"Altitude":38,"Angle":60,"Satelites":14,"Speed":0,"TagDevices":{"Ain1":{"TagName":"Ain1","TagId":"9","TagDataType":"int","TagVal":"121"},"Ain2":{"TagName":"Ain2","TagId":"10","TagDataType":"int","TagVal":"21"},"BatteryVolt":{"TagName":"BatteryVolt","TagId":"67","TagDataType":"int","TagVal":"9785"},"Din1":{"TagName":"Din1","TagId":"1","TagDataType":"int","TagVal":"1"},"Din2":{"TagName":"Din2","TagId":"2","TagDataType":"int","TagVal":"0"},"Din3":{"TagName":"Din3","TagId":"3","TagDataType":"int","TagVal":"0"},"Din4":{"TagName":"Din4","TagId":"4","TagDataType":"int","TagVal":"0"},"Dist":{"TagName":"Dist","TagId":"199","TagDataType":"float32","TagVal":"7"},"GsmSignalSensor":{"TagName":"GsmSignalSensor","TagId":"21","TagDataType":"int","TagVal":"4"},"MovementSensor":{"TagName":"MovementSensor","TagId":"240","TagDataType":"int","TagVal":"0"},"PowerVolt":{"TagName":"PowerVolt","TagId":"66","TagDataType":"int","TagVal":"14796"}}},{"Tstamp":"2018-11-16T01:23:22+07:00","TstampInt":1542306202000,"Imei":352094085830856,"Longitude":106.7880066,"Latitude":-6.2631683,"Altitude":56,"Angle":311,"Satelites":15,"Speed":0,"TagDevices":{"Ain1":{"TagName":"Ain1","TagId":"9","TagDataType":"int","TagVal":"121"},"Ain2":{"TagName":"Ain2","TagId":"10","TagDataType":"int","TagVal":"21"},"BatteryVolt":{"TagName":"BatteryVolt","TagId":"67","TagDataType":"int","TagVal":"9785"},"Din1":{"TagName":"Din1","TagId":"1","TagDataType":"int","TagVal":"1"},"Din2":{"TagName":"Din2","TagId":"2","TagDataType":"int","TagVal":"0"},"Din3":{"TagName":"Din3","TagId":"3","TagDataType":"int","TagVal":"0"},"Din4":{"TagName":"Din4","TagId":"4","TagDataType":"int","TagVal":"0"},"Dist":{"TagName":"Dist","TagId":"199","TagDataType":"float32","TagVal":"7"},"GsmSignalSensor":{"TagName":"GsmSignalSensor","TagId":"21","TagDataType":"int","TagVal":"4"},"MovementSensor":{"TagName":"MovementSensor","TagId":"240","TagDataType":"int","TagVal":"0"},"PowerVolt":{"TagName":"PowerVolt","TagId":"66","TagDataType":"int","TagVal":"14796"}}},{"Tstamp":"2018-11-16T01:28:22+07:00","TstampInt":1542306502000,"Imei":352094085830856,"Longitude":106.7879216,"Latitude":-6.2631716,"Altitude":0,"Angle":0,"Satelites":0,"Speed":0,"TagDevices":{"Ain1":{"TagName":"Ain1","TagId":"9","TagDataType":"int","TagVal":"121"},"Ain2":{"TagName":"Ain2","TagId":"10","TagDataType":"int","TagVal":"21"},"BatteryVolt":{"TagName":"BatteryVolt","TagId":"67","TagDataType":"int","TagVal":"9785"},"Din1":{"TagName":"Din1","TagId":"1","TagDataType":"int","TagVal":"1"},"Din2":{"TagName":"Din2","TagId":"2","TagDataType":"int","TagVal":"0"},"Din3":{"TagName":"Din3","TagId":"3","TagDataType":"int","TagVal":"0"},"Din4":{"TagName":"Din4","TagId":"4","TagDataType":"int","TagVal":"0"},"Dist":{"TagName":"Dist","TagId":"199","TagDataType":"float32","TagVal":"7"},"GsmSignalSensor":{"TagName":"GsmSignalSensor","TagId":"21","TagDataType":"int","TagVal":"4"},"MovementSensor":{"TagName":"MovementSensor","TagId":"240","TagDataType":"int","TagVal":"0"},"PowerVolt":{"TagName":"PowerVolt","TagId":"66","TagDataType":"int","TagVal":"14796"}}}]}`
      p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(value)}
   }
	
	// wait for delivery report goroutine to finish
	_ = <-doneChan

	p.Close()
}
