package main

import (
	"log"
	"os/exec"
	"time"
)

func main() {

	_, err := exec.Command("./docker.sh").Output()
	if err != nil {
		log.Fatalf("Docker Error : %v", err)
	}

	time.Sleep(10 * time.Second)

	err = setup()

	if err != nil {
		log.Fatalf("Error : %v", err)
	}

	// When this example is run the first time, wait for creation of all internal topics (this is done
	// by goka.NewProcessor)
	initialized := make(chan struct{})

	go runEmitter(avrocodec)
	go runProcessor(initialized)
	runView(initialized)
}
