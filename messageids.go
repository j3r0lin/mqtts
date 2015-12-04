package mqtt

import (
	"sync"
)

type messageIds struct {
	sync.RWMutex
	//idChan chan uint16
	index map[string]map[uint16]interface{}
}

const (
	msgIdMax uint16 = 65535
	msgIdMin uint16 = 1
)

func newMessageIds() *messageIds {
	return &messageIds{
		index: make(map[string]map[uint16]interface{}),
	}
}

// get one unused message id and mark it as used.
func (m *messageIds) request(cid string) uint16 {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.index[cid]; !ok {
		m.index[cid] = make(map[uint16]interface{})
	}

	for i := msgIdMin; i < msgIdMax; i++ {
		if m.index[cid][i] == nil {
			m.index[cid][i] = cid
			return i
		}
	}
	return 0
}

// check the given id is used or not
func (m *messageIds) used(cid string, mid uint16) bool {
	m.RLock()
	defer m.RUnlock()
	if _, ok := m.index[cid]; ok {
		return m.index[cid][mid] != nil
	}
	return false
}

// release an id
func (m *messageIds) free(cid string, mid uint16) {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.index[cid]; ok {
		delete(m.index[cid], mid)
	}
}

func (m *messageIds) clean(cid string) {
	m.Lock()
	defer m.Unlock()
	delete(m.index, cid)
}
