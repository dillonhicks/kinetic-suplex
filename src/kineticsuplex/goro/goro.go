package goro

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"kineticsuplex/goro/consts"
)

var goroSingleton *GoroStreamDemuxer = nil


func setInstance(inst *GoroStreamDemuxer) {
	goroSingleton = inst
}


func goro() *GoroStreamDemuxer {
	return goroSingleton
}


type GoroStreamDemuxer struct {
	 mapMutex sync.Mutex
	 newPartitionsChan chan string
	 partitionKeyToChannelsMap map[string]chan string
	 kinesisClient *kinesis.Kinesis
	 ctrlLeases int64
	 masterCtrlChan chan int
	 masterAckChan chan int
	 serverCtrlChan chan int
	 serverAckChan chan int
	 streamNames map[string]bool
	
}


func NewGoroStreamDemuxer(awsConfig *aws.Config) *GoroStreamDemuxer {
	sess := session.Must(session.NewSession(awsConfig))

	inst := GoroStreamDemuxer{
		kinesisClient:                   kinesis.New(sess),
		mapMutex:                  sync.Mutex{},
		partitionKeyToChannelsMap: make(map[string]chan string),
		ctrlLeases:                      0,
		masterCtrlChan:                  make(chan int),
		masterAckChan:                   make(chan int),
		serverCtrlChan:                  make(chan int, 1),
		serverAckChan:                   make(chan int, 1),
		streamNames:                     make(map[string]bool),
	}

	return &inst
}


func GetOrCreateChannel(key string) chan string {
	fmt.Println(fmt.Sprintf("[GLBL] [INFO] - get partition channel %s", key))
	goro().mapMutex.Lock()
	ch, ok := goro().partitionKeyToChannelsMap[key]
	defer goro().mapMutex.Unlock()

	if !ok {
		fmt.Println(fmt.Sprintf("[GLBL] [INFO] - partition channel does not exist %s", key))

		ch2, ok := goro().partitionKeyToChannelsMap[key]
		if !ok {
			ch2 = make(chan string, consts.PageSize)
			goro().partitionKeyToChannelsMap[key] = ch2
			defer func() {
				fmt.Println(fmt.Sprintf("[GLBL] [INFO] - new partition channel %s", key))
				//goro().newPartitionsChan <- key
			}()
		}

		return ch2
	}

	return ch
}

func getRecieverChannel(key string) (chan string, error) {
	goro().mapMutex.Lock()
	defer goro().mapMutex.Unlock()

	ch, ok := goro().partitionKeyToChannelsMap[key]
	if !ok {
		return nil, errors.New("No such channel " + key)
	}

	return ch, nil
}



func newShardIterator(kns *kinesis.Kinesis, streamName *string, shardId *string) (*kinesis.GetShardIteratorOutput, error) {

	shardType := kinesis.ShardIteratorTypeLatest

	return kns.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:        streamName,
		ShardId:           shardId,
		ShardIteratorType: &shardType,
	})

}

func continuationIterator(kns *kinesis.Kinesis, streamName *string, shardId *string, lastSequenceNumber *string) (*kinesis.GetShardIteratorOutput, error) {
	shardType := kinesis.ShardIteratorTypeAfterSequenceNumber
	return kns.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:             streamName,
		ShardId:                shardId,
		ShardIteratorType:      &shardType,
		StartingSequenceNumber: lastSequenceNumber,
	})
}

func getNextRecordsBatch(currentRequest *kinesis.GetRecordsOutput, kns *kinesis.Kinesis, streamName *string, shardId *string) (*kinesis.GetRecordsOutput, error) {
	shortStreamName := (*streamName)[:7]
	shortShardId := ""
	intShardId, err := strconv.ParseUint(strings.SplitN(*shardId, "-", 2)[1], 10, 64)

	if err != nil {
		shortShardId = *shardId
	} else {
		shortShardId = fmt.Sprintf("%v", intShardId)
	}

	var iterator *string = nil
	if currentRequest == nil {
		fmt.Println(fmt.Sprintf("[ITER   ] [NEW 1] - %s/%s", shortStreamName, shortShardId))

		resp, err := newShardIterator(kns, streamName, shardId)
		if err != nil {
			return nil, err
		}

		iterator = resp.ShardIterator
	} else {
		iterator = currentRequest.NextShardIterator
	}

	if iterator == nil {
		return nil, errors.New(fmt.Sprintf("Iterator is nil after refresh, %v/%v has probably been closed", *streamName, *shardId))
	}

	fmt.Println(fmt.Sprintf("[ITER   ] [GET 1] - %s/%s", shortStreamName, shortShardId))
	limit := consts.ShardTaskletMaxRecordsPerRequest
	resp, err := kns.GetRecords(&kinesis.GetRecordsInput{
		Limit:         &limit,
		ShardIterator: iterator,
	})

	if err != nil {
		if err.Error() != kinesis.ErrCodeExpiredIteratorException {
			return nil, err
		}
		fmt.Println(fmt.Sprintf("[ITER   ] [EXPIR] - %s/%s", shortStreamName, shortShardId))
		fmt.Println(fmt.Sprintf("[ITER   ] [NEW 2] - %s/%s", shortStreamName, shortShardId))

		lastSequenceNumber := currentRequest.Records[len(currentRequest.Records)-1].SequenceNumber
		resp2, err := continuationIterator(kns, streamName, shardId, lastSequenceNumber)

		if err != nil {
			return nil, err
		}

		iterator = resp2.ShardIterator

		fmt.Println(fmt.Sprintf("[ITER   ] [GET 2] - %s/%s, attempt with new iterator", shortStreamName, shortShardId))
		resp, err = kns.GetRecords(&kinesis.GetRecordsInput{
			Limit:         &limit,
			ShardIterator: iterator,
		})

		if err != nil {
			return nil, err
		}
	}

	return resp, nil
}


func runShardTasklet(ctrlChan <-chan int, ackChan chan<- int, kns *kinesis.Kinesis, streamName string, shardId string) {
	shortStreamName := streamName[:7]
	shortShardId := ""
	intShardId, err := strconv.ParseUint(strings.SplitN(shardId, "-", 2)[1], 10, 64)

	if err != nil {
		shortShardId = shardId
	} else {
		shortShardId = fmt.Sprintf("%v", intShardId)
	}

	fmt.Println(fmt.Sprintf("[TASKLET] [START] - %s/%s", shortStreamName, shortShardId))
	localChannelCache := make(map[string]chan string)

	getChannel := func(key string) chan string {
		ch, ok := localChannelCache[key]
		if !ok {
			ch, err := getRecieverChannel(key)
			if err != nil {
				return nil
			}

			localChannelCache[key] = ch
		}

		return ch
	}

	recordCounter := 0
	timesEmpty := time.Duration(1)

	var recordsResponse *kinesis.GetRecordsOutput = nil
	maxWindow := 10
	movingAverage := float64(1)
	movingWindow := make([]int, 0)
	averageDecayFactor := 100

	dropCount := 0
	warnDropEvery := 100

	for {
		fmt.Println(fmt.Sprintf("[TASKLET] [INFO ] - %s/%s: Getting stream records", shortStreamName, shortShardId))

		select {

		case <-ctrlChan:
			fmt.Println(fmt.Sprintf("[TASKLET] [STOP ] - %s/%s: Received shutdown ctrl msg", shortStreamName, shortShardId))
			ackChan <- 1
			break
		default:
			// fall through
		}

		recordsResponse, err = getNextRecordsBatch(recordsResponse, kns, &streamName, &shardId);
		if err != nil {
			fmt.Println(fmt.Sprintf("[TASKLET] [ERROR] - %s/%s: %v", shortStreamName, shortShardId, err))
			ackChan <- 1
			return
		}

		batchSize := len(recordsResponse.Records)
		movingWindow = append(movingWindow, batchSize)

		if batchSize > 0 {
			fmt.Println(fmt.Sprintf("[TASKLET] [INFO ] - %s/%s: Read %v records", shortStreamName, shortShardId, batchSize))
		}

		for _, record := range recordsResponse.Records {
			recordCounter++
			if ch := getChannel(*record.PartitionKey); ch != nil {
				ch <- string(record.Data)
			} else if dropCount++; dropCount%warnDropEvery == 0 {
				fmt.Println(fmt.Sprintf("[TASKLET] [WARN ] - %s/%s: Reciever does not exist, dropping some records", shortStreamName, shortShardId))
			}
		}

		nextIterator := recordsResponse.NextShardIterator
		if nextIterator == nil || batchSize == 0 {
			fmt.Println(fmt.Sprintf("[TASKLET] [PAUSE] - %s/%s: Sleeping a few seconds because the next iterator is empty ", shortStreamName, shortShardId))
			if sleepTime := 2 * timesEmpty * time.Second; sleepTime < 30*time.Second {
				time.Sleep(sleepTime)
			} else {
				time.Sleep(30 * time.Second)
			}

			timesEmpty++
		} else {
			timesEmpty = 1

			sleepMicros := time.Duration(1000.0 * 1000.0 * (float64(averageDecayFactor) / (movingAverage)))
			sleepMicros *= time.Microsecond

			if sleepMicros > 10*time.Second {
				sleepMicros = 10 * time.Second
			}

			sleepDuration := time.Duration(sleepMicros)

			fmt.Println(fmt.Sprintf("[TASKLET] [WAIT ] - %s/%s: throttling for %v before next read",
				shortStreamName, shortShardId, sleepDuration))
			time.Sleep(sleepDuration)
		}

		if len(movingWindow) == maxWindow {
			movingAverage += (float64(batchSize) - float64(movingWindow[0])) / float64(maxWindow)
			movingWindow = movingWindow[1:]
		} else {
			sum := 0
			for _, value := range movingWindow {
				sum += value
			}
			movingAverage = float64(sum) / float64(len(movingWindow))

		}

		fmt.Println(fmt.Sprintf("[TASKLET] [WAIT ] - MovingAverage: %v", movingAverage));
	}

	fmt.Println(fmt.Sprintf("[TASKLET] [DONE ] - %s/%s: Exiting...", shortStreamName, shortShardId))
}

func runStreamCollectorTask(ctrlChan <-chan int, ackChan chan<- int, kns *kinesis.Kinesis, streamName *string) {
	fmt.Println(fmt.Sprintf("[TASK  ] [START] - %s", *streamName))

	description, err := kns.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: streamName,
	})

	if err != nil {
		fmt.Println(fmt.Sprintf("[TASK  ] [ERROR] - %s: Error describing stream, %v", streamName, err))
		ackChan <- 1
		return
	}

	shardCount := len(description.StreamDescription.Shards)
	shardCtrlChan := make(chan int, shardCount)
	shardAckChan := make(chan int, shardCount)

	// todo: occasionally poll the stream for shard and spawn more if the shards were split
	for _, shard := range description.StreamDescription.Shards {
		if shard.SequenceNumberRange.EndingSequenceNumber != nil {
			fmt.Println(fmt.Sprintf("[TASK   ] [INFO ] - %s: Skipping closed shard %s", *streamName, shard.ShardId))
		}

		go runShardTasklet(shardCtrlChan, shardAckChan, kns, *streamName, *shard.ShardId)
	}

	acks := 0

	done := false
	for !done {
		select {
		case <-ctrlChan:
			fmt.Println(fmt.Sprintf("[TASK   ] [STOP ] - %s: Received shutdown ctrl msg", *streamName))
			done = true
			break
		case <-shardAckChan:
			fmt.Println(fmt.Sprintf("[TASK   ] [WARN ] - %s: Observed early tasklet stop msg", *streamName))
			acks++
		default:
			time.Sleep(432 * time.Millisecond)
		}

		if acks == shardCount {
			fmt.Println(fmt.Sprintf("[TASK   ] [ERROR] - %s: All tasklets stopped before recieving the ctrl message", *streamName))
			ackChan <- 1
			done = true
		}
	}

	for range description.StreamDescription.Shards {
		shardCtrlChan <- 1
	}

	for acks < shardCount {
		select {
		case <-shardAckChan:
			acks++
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	fmt.Println(fmt.Sprintf("[TASK   ] [DONE ] - %s: Exiting...", *streamName))
	ackChan <- 1
}


func Shutdown() {
	goro().serverCtrlChan <- 1
}


func StartStreamCollectorTask(streamName string) {
	if _, ok := goro().streamNames[streamName]; ok {
		fmt.Println(fmt.Sprintf("[GORO ] [INFO ]: Stream task already started for %v", streamName))
	} else {
		goro().ctrlLeases++
		go runStreamCollectorTask(goro().masterCtrlChan, goro().masterAckChan, goro().kinesisClient, &streamName)
	}
}


func Run(instance *GoroStreamDemuxer) {
	setInstance(instance)

	fmt.Println(fmt.Sprintf("[GORO ] [START]: Running the stream task server"))

	startTime := time.Now()

	acks := int64(0)
	errorCount := 0

	done := false
	for !done {

		select {
		case <-goro().serverCtrlChan:
			fmt.Println(fmt.Sprintf("[GORO ] [STOP ]: Received stop signal"))
			done = true
			break
		case <-goro().masterAckChan:
			fmt.Println(fmt.Sprintf("[GORO ] [WARN ]: Observed early task stop "))
			errorCount++
			acks++
		default:
			time.Sleep(65 * time.Millisecond)
		}

		if goro().ctrlLeases > 0 && acks == goro().ctrlLeases {
			fmt.Println(fmt.Sprintf("[GORO ] [ERROR]: All stream collector tasks stopped before recieving the master ctrl message"))
			errorCount++
			done = true
		}
	}

	for i := int64(0); i < goro().ctrlLeases; i++ {
		goro().masterCtrlChan <- 1
	}

	for {

		select {
		case <-goro().masterAckChan:
			acks++
			fmt.Println(fmt.Sprintf("[GORO ] [INFO ]: Received task stop ack #%v", acks))
		default:
		}
		if acks >= goro().ctrlLeases {
			fmt.Println("[GORO ] [INFO ]: All workers have stopped")
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	os.Stdout.Sync()
	os.Stderr.Sync()

	close(goro().serverCtrlChan)
	close(goro().masterAckChan)
	close(goro().masterCtrlChan)

	duration := time.Now().Sub(startTime)
	fmt.Println(fmt.Sprintf("[GORO ] [STATS]: Ran for %v", duration))
	fmt.Println("[GORO ] [DONE ]")
	goro().serverAckChan <- 1
}


func Wait() {
	<-goro().serverAckChan
}
