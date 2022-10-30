package cmd

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/rebuy-de/aws-nuke/v2/pkg/awsutil"
	"github.com/rebuy-de/aws-nuke/v2/pkg/util"
	"github.com/rebuy-de/aws-nuke/v2/resources"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

const ScannerParallelQueries = 16

func Scan(region *Region, resourceTypes []string) <-chan *Item {
	s := &scanner{
		items:     make(chan *Item, 100), // 채널 큐 버퍼 100개
		semaphore: semaphore.NewWeighted(ScannerParallelQueries),
	}
	go s.run(region, resourceTypes)

	return s.items
}

type scanner struct {
	items     chan *Item
	semaphore *semaphore.Weighted
}

func (s *scanner) run(region *Region, resourceTypes []string) {
	ctx := context.Background()

	for _, resourceType := range resourceTypes {
		s.semaphore.Acquire(ctx, 1)
		go s.list(region, resourceType)
	}

	// 16개의 세마포어가 모두 종료될 때 까지 대기
	s.semaphore.Acquire(ctx, ScannerParallelQueries)

	close(s.items)
}

func (s *scanner) list(region *Region, resourceType string) {
	defer func() {
		// 혹시나 뭔가 실패하면 오류 띄우고 recover 통해서 계속 실행되도록 함
		if r := recover(); r != nil {
			err := fmt.Errorf("%v\n\n%s", r.(error), string(debug.Stack()))
			dump := util.Indent(fmt.Sprintf("%v", err), "    ")
			log.Errorf("Listing %s failed:\n%s", resourceType, dump)
		}
	}()
	// 리스트(리소스 타입) 하나 끝날 때 마다 세마포어 릴리즈
	defer s.semaphore.Release(1)

	// resources 패키이에서 적당한(?) 함수 가져옴(list)
	lister := resources.GetLister(resourceType)
	var rs []resources.Resource
	sess, err := region.Session(resourceType)
	if err == nil {
		rs, err = lister(sess)
	}
	if err != nil {
		_, ok := err.(awsutil.ErrSkipRequest)
		if ok {
			log.Debugf("skipping request: %v", err)
			return
		}

		_, ok = err.(awsutil.ErrUnknownEndpoint)
		if ok {
			log.Warnf("skipping request: %v", err)
			return
		}

		dump := util.Indent(fmt.Sprintf("%v", err), "    ")
		log.Errorf("Listing %s failed:\n%s", resourceType, dump)
		return
	}

	// 획득한 리소스 채널로 전송(수신만 하는 채널임)
	for _, r := range rs {
		s.items <- &Item{
			Region:   region,
			Resource: r,
			State:    ItemStateNew,
			Type:     resourceType,
		}
	}
}
