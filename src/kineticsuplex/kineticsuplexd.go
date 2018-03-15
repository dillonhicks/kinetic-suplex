package main

import (
	"os"
	"os/signal"

	"kineticsuplex/goro"
	"kineticsuplex/kintaro"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
)

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	kintaroServer := kintaro.NewKintaroCacheServer()
	awsConfig := aws.Config{}
	awsConfig.WithRegion(endpoints.UsWest2RegionID)


	go goro.Run(goro.NewGoroStreamDemuxer(&awsConfig))
	go kintaroServer.Run("", 8000)
	<-sigChan
	goro.Shutdown()
	goro.Wait()

}
