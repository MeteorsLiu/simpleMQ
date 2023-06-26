package queue

import (
	"fmt"

	"github.com/alphadose/haxmap"
)

var (
	ErrPollableEventsNotExists = fmt.Errorf("the pollable event doesn't exist")
)

type Pollable interface {
	Task
}

type PollTask struct {
	m *haxmap.Map[string, Pollable]
}

func NewPoll() *PollTask {
	return &PollTask{
		m: haxmap.New[string, Pollable](),
	}
}

func (p *PollTask) Register(pb Pollable) {
	p.m.Set(pb.ID(), pb)
	pb.OnDone(func() {
		p.Remove(pb)
	})
}

func (p *PollTask) Remove(pb Pollable) {
	p.m.Del(pb.ID())
}

func (p *PollTask) PollOne(id string) (bool, error) {
	pb, ok := p.m.Get(id)
	if !ok {
		return false, ErrPollableEventsNotExists
	}
	return pb.IsDone(), nil
}

func (p *PollTask) Poll(f func(string, Pollable)) {
	p.m.ForEach(func(id string, pb Pollable) bool {
		if pb.IsDone() {
			f(id, pb)
		}
		return true
	})
}

func (p *PollTask) Callback(id string, f func()) error {
	pb, ok := p.m.Get(id)
	if !ok {
		return ErrPollableEventsNotExists
	}
	pb.OnDone(f)
	return nil
}

func (p *PollTask) Kill(id string) error {
	pb, ok := p.m.Get(id)
	if !ok {
		return ErrPollableEventsNotExists
	}
	pb.Stop()
	return nil
}
