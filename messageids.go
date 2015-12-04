package mqtt

import (
	"github.com/pborman/uuid"
	"sync"
)

type messageIds struct {
	sync.RWMutex
	//idChan chan uint16
	index map[uint16]uuid.UUID
}

const (
	msgIdMax uint16 = 65535
	msgIdMin uint16 = 1
)

func newMessageIds() *messageIds {
	return &messageIds{
		index: make(map[uint16]uuid.UUID),
	}
}

// get one unused message id and mark it as used.
func (m *messageIds) use(id uuid.UUID) uint16 {
	m.Lock()
	defer m.Unlock()
	for i := msgIdMin; i < msgIdMax; i++ {
		if m.index[i] == nil {
			m.index[i] = id
			return i
		}
	}
	return 0
}

// check the given id is used or not
func (m *messageIds) used(id uint16) bool {
	m.RLock()
	defer m.RUnlock()
	return m.index[id] != nil
}

// release an id
func (m *messageIds) free(id uint16) {
	m.Lock()
	defer m.Unlock()
	m.index[id] = nil
}
