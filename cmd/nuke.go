package cmd

import (
	"fmt"
	"time"

	"github.com/rebuy-de/aws-nuke/v2/pkg/awsutil"
	"github.com/rebuy-de/aws-nuke/v2/pkg/config"
	"github.com/rebuy-de/aws-nuke/v2/pkg/types"
	"github.com/rebuy-de/aws-nuke/v2/resources"
	"github.com/sirupsen/logrus"
)

type Nuke struct {
	Parameters NukeParameters
	Account    awsutil.Account
	Config     *config.Nuke

	ResourceTypes types.Collection

	items Queue
}

func NewNuke(params NukeParameters, account awsutil.Account) *Nuke {
	n := Nuke{
		Parameters: params,
		Account:    account,
	}

	return &n
}

func (n *Nuke) Run() error {
	var err error

	// 실제 동작할거면 3초 이상 설정해라
	if n.Parameters.ForceSleep < 3 && n.Parameters.NoDryRun {
		return fmt.Errorf("value for --force-sleep cannot be less than 3 seconds if --no-dry-run is set. This is for your own protection")
	}
	forceSleep := time.Duration(n.Parameters.ForceSleep) * time.Second

	fmt.Printf("aws-nuke version %s - %s - %s\n\n", BuildVersion, BuildDate, BuildHash)

	// 계정 유효성 점검
	err = n.Config.ValidateAccount(n.Account.ID(), n.Account.Aliases())
	if err != nil {
		return err
	}

	fmt.Printf("Do you really want to nuke the account with "+
		"the ID %s and the alias '%s'?\n", n.Account.ID(), n.Account.Alias())
	if n.Parameters.Force {
		fmt.Printf("Waiting %v before continuing.\n", forceSleep)
		time.Sleep(forceSleep)
	} else {
		fmt.Printf("Do you want to continue? Enter account alias to continue.\n")
		err = Prompt(n.Account.Alias())
		if err != nil {
			return err
		}
	}

	// TODO: 처리해야할 리소스들에 대해서 스캔 진행
	// 최종적으로 필터된 리소스, 삭제 가능 리소스 식별
	// n.items(*Nuke.items Queue(type Queue []*Item -> ...)) 에 저장
	err = n.Scan()
	if err != nil {
		return err
	}

	// 없으면
	if n.items.Count(ItemStateNew) == 0 {
		fmt.Println("No resource to delete.")
		return nil
	}

	// 드라이런이 아니면
	if !n.Parameters.NoDryRun {
		fmt.Println("The above resources would be deleted with the supplied configuration. Provide --no-dry-run to actually destroy resources.")
		return nil
	}

	fmt.Printf("Do you really want to nuke these resources on the account with "+
		"the ID %s and the alias '%s'?\n", n.Account.ID(), n.Account.Alias())
	if n.Parameters.Force {
		fmt.Printf("Waiting %v before continuing.\n", forceSleep)
		time.Sleep(forceSleep)
	} else {
		fmt.Printf("Do you want to continue? Enter account alias to continue.\n")
		// account alias 치면 바로 실행
		err = Prompt(n.Account.Alias())
		if err != nil {
			return err
		}
	}

	failCount := 0
	waitingCount := 0

	// 무한으로 즐기면서 상태 체크
	for {
		// 큐에 들어가 있는 상태 체크
		n.HandleQueue()

		if n.items.Count(ItemStatePending, ItemStateWaiting, ItemStateNew) == 0 && n.items.Count(ItemStateFailed) > 0 {
			if failCount >= 2 {
				logrus.Errorf("There are resources in failed state, but none are ready for deletion, anymore.")
				fmt.Println()

				for _, item := range n.items {
					if item.State != ItemStateFailed {
						continue
					}

					item.Print()
					logrus.Error(item.Reason)
				}

				return fmt.Errorf("failed")
			}

			failCount = failCount + 1
		} else {
			failCount = 0
		}
		if n.Parameters.MaxWaitRetries != 0 && n.items.Count(ItemStateWaiting, ItemStatePending) > 0 && n.items.Count(ItemStateNew) == 0 {
			if waitingCount >= n.Parameters.MaxWaitRetries {
				return fmt.Errorf("max wait retries of %d exceeded", n.Parameters.MaxWaitRetries)
			}
			waitingCount = waitingCount + 1
		} else {
			waitingCount = 0
		}
		if n.items.Count(ItemStateNew, ItemStatePending, ItemStateFailed, ItemStateWaiting) == 0 {
			break
		}

		time.Sleep(5 * time.Second)
	}

	fmt.Printf("Nuke complete: %d failed, %d skipped, %d finished.\n\n",
		n.items.Count(ItemStateFailed), n.items.Count(ItemStateFiltered), n.items.Count(ItemStateFinished))

	return nil
}

func (n *Nuke) Scan() error {
	accountConfig := n.Config.Accounts[n.Account.ID()]

	// include, exclude 리소스 식별
	resourceTypes := ResolveResourceTypes(
		resources.GetListerNames(),
		resources.GetCloudControlMapping(),
		[]types.Collection{
			n.Parameters.Targets,
			n.Config.ResourceTypes.Targets,
			accountConfig.ResourceTypes.Targets,
		},
		[]types.Collection{
			n.Parameters.Excludes,
			n.Config.ResourceTypes.Excludes,
			accountConfig.ResourceTypes.Excludes,
		},
		[]types.Collection{
			n.Parameters.CloudControl,
			n.Config.ResourceTypes.CloudControl,
			accountConfig.ResourceTypes.CloudControl,
		},
	)

	queue := make(Queue, 0)

	for _, regionName := range n.Config.Regions {
		region := NewRegion(regionName, n.Account.ResourceTypeToServiceType, n.Account.NewSession)

		// 세마포어를 통해 동시 최대 16개 API가 동작하여 스캔
		// 고루틴을 이용함
		items := Scan(region, resourceTypes)
		// 100개 까지는 쭉쭉 받음 그 이후로는 채널 대기 상태로 들어감
		for item := range items {
			ffGetter, ok := item.Resource.(resources.FeatureFlagGetter)
			if ok {
				ffGetter.FeatureFlags(n.Config.FeatureFlags)
			}

			queue = append(queue, item)
			// TODO: filter는 인터페이스로 되어있으며,
			// 필요한 구조체들이 인터페이스를 기준으로 구현함
			err := n.Filter(item)
			if err != nil {
				return err
			}

			if item.State != ItemStateFiltered || !n.Parameters.Quiet {
				item.Print()
			}
		}
	}

	fmt.Printf("Scan complete: %d total, %d nukeable, %d filtered.\n\n",
		queue.CountTotal(), queue.Count(ItemStateNew), queue.Count(ItemStateFiltered))

	n.items = queue

	return nil
}

func (n *Nuke) Filter(item *Item) error {

	checker, ok := item.Resource.(resources.Filter)
	if ok {
		err := checker.Filter()
		if err != nil {
			item.State = ItemStateFiltered
			item.Reason = err.Error()

			// Not returning the error, since it could be because of a failed
			// request to the API. We do not want to block the whole nuking,
			// because of an issue on AWS side.
			return nil
		}
	}

	accountFilters, err := n.Config.Filters(n.Account.ID())
	if err != nil {
		return err
	}

	itemFilters, ok := accountFilters[item.Type]
	if !ok {
		return nil
	}

	for _, filter := range itemFilters {
		prop, err := item.GetProperty(filter.Property)
		if err != nil {
			logrus.Warnf(err.Error())
			continue
		}
		match, err := filter.Match(prop)
		if err != nil {
			return err
		}

		if IsTrue(filter.Invert) {
			match = !match
		}

		if match {
			item.State = ItemStateFiltered
			item.Reason = "filtered by config"
			return nil
		}
	}

	return nil
}

func (n *Nuke) HandleQueue() {
	listCache := make(map[string]map[string][]resources.Resource)

	for _, item := range n.items {
		switch item.State {
		case ItemStateNew:
			n.HandleRemove(item)
			item.Print()
		case ItemStateFailed:
			n.HandleRemove(item)
			n.HandleWait(item, listCache)
			item.Print()
		case ItemStatePending:
			n.HandleWait(item, listCache)
			item.State = ItemStateWaiting
			item.Print()
		case ItemStateWaiting:
			n.HandleWait(item, listCache)
			item.Print()
		}

	}

	fmt.Println()
	fmt.Printf("Removal requested: %d waiting, %d failed, %d skipped, %d finished\n\n",
		n.items.Count(ItemStateWaiting, ItemStatePending), n.items.Count(ItemStateFailed),
		n.items.Count(ItemStateFiltered), n.items.Count(ItemStateFinished))
}

func (n *Nuke) HandleRemove(item *Item) {
	err := item.Resource.Remove()
	if err != nil {
		item.State = ItemStateFailed
		item.Reason = err.Error()
		return
	}

	item.State = ItemStatePending
	item.Reason = ""
}

func (n *Nuke) HandleWait(item *Item, cache map[string]map[string][]resources.Resource) {
	var err error
	region := item.Region.Name
	_, ok := cache[region]
	if !ok {
		cache[region] = map[string][]resources.Resource{}
	}
	left, ok := cache[region][item.Type]
	if !ok {
		left, err = item.List()
		if err != nil {
			item.State = ItemStateFailed
			item.Reason = err.Error()
			return
		}
		cache[region][item.Type] = left
	}

	for _, r := range left {
		if item.Equals(r) {
			checker, ok := r.(resources.Filter)
			if ok {
				err := checker.Filter()
				if err != nil {
					break
				}
			}

			return
		}
	}

	item.State = ItemStateFinished
	item.Reason = ""
}
