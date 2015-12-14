package mqtt

import (
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git/packets"
	"github.com/syndtr/goleveldb/leveldb"
	"strconv"
	"github.com/syndtr/goleveldb/leveldb/util"
	"strings"
)

type LevelStore struct {
	db *leveldb.DB
}


func newLevelStore() Store {
	db, err := leveldb.OpenFile("store.db", nil)
//	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		log.Fatal(err)
	}

	store := &LevelStore{
		db:       db,
	}

	return store
}

func (this *LevelStore) StoreSubscription(filter, cid string, qos byte) {
	key := "subscribe:" + cid + ":" + filter
	this.db.Put([]byte(key), []byte{qos}, nil)
}

func (this *LevelStore) DeleteSubscription(filter, cid string) {
	key := "subscribe:" + cid + ":" + filter
	this.db.Delete([]byte(key), nil)
}

func (this *LevelStore) CleanSubscription(cid string) {
	iter := this.db.NewIterator(util.BytesPrefix([]byte("subscribe:" + cid)), nil)
	defer iter.Release()

	b := new(leveldb.Batch)
	for iter.Next() {
		b.Delete(iter.Key())
	}
	this.db.Write(b, nil)
}


func (this *LevelStore) LookupSubscriptions(callback func(filter, cid string, qos byte)) {
	iter := this.db.NewIterator(util.BytesPrefix([]byte("subscribe")), nil)
	defer iter.Release()

	for iter.Next() {
		key := string(iter.Key())
		value := iter.Value()
		slice := strings.SplitN(key, ":", 3)
		cid := slice[1]
		filter := slice[2]
		qos := value[0]
		callback(filter, cid, qos)
	}
}

func (this *LevelStore) StoreRetained(p *packets.PublishPacket) {
	key := "retain:" + p.TopicName
	if len(p.Payload) == 0 {
		this.db.Delete([]byte(key), nil)
		return
	}

	value := MarshalPacket(p)
	this.db.Put([]byte(key), value, nil)
}

func (this *LevelStore) LookupRetained(callback func(*packets.PublishPacket)) {
	iter := this.db.NewIterator(util.BytesPrefix([]byte("retain")), nil)
	defer iter.Release()

	for iter.Next() {
		cp := UnmarshalPacket(iter.Value()).(*packets.PublishPacket)
		callback(cp)
	}
}

func (this *LevelStore) FindInboundPacket(cid string, mid uint16) packets.ControlPacket {
	key := levelPacketKey(cid, mid, true)
	if value, err := this.db.Get([]byte(key), nil); err == nil {
		return UnmarshalPacket(value)
	}

	return nil
}

func (this *LevelStore) StoreInboundPacket(cid string, p packets.ControlPacket) error {
	key := levelPacketKey(cid, p.Details().MessageID, true)
	value := MarshalPacket(p)
	return this.db.Put([]byte(key), value, nil)
}

func (this *LevelStore) StoreOutboundPacket(cid string, p packets.ControlPacket) error {
	key := levelPacketKey(cid, p.Details().MessageID, false)
	value := MarshalPacket(p)
	return this.db.Put([]byte(key), value, nil)
}

func (this *LevelStore) StreamOfflinePackets(cid string, callback func(packets.ControlPacket)) {
	iter := this.db.NewIterator(util.BytesPrefix([]byte("packets:out:" + cid)), nil)
	defer iter.Release()

	for iter.Next() {
		cp := UnmarshalPacket(iter.Value())
		callback(cp)
	}
}

func (this *LevelStore) DeleteInboundPacket(cid string, mid uint16) {
	this.db.Delete([]byte(levelPacketKey(cid, mid, true)), nil)
}

func (this *LevelStore) DeleteOutboundPacket(cid string, mid uint16) {
	this.db.Delete([]byte(levelPacketKey(cid, mid, false)), nil)
}


func (this *LevelStore) CleanPackets(cid string) {
	b := new(leveldb.Batch)
	iter := this.db.NewIterator(util.BytesPrefix([]byte("packets:in:" + cid)), nil)
	for iter.Next() {
		b.Delete(iter.Key())
	}
	iter.Release()

	iter = this.db.NewIterator(util.BytesPrefix([]byte("packets:out:" + cid)), nil)
	for iter.Next() {
		b.Delete(iter.Key())
	}
	iter.Release()

	this.db.Write(b, nil)
}

func (this *LevelStore) InPacketsSize() int {
	iter := this.db.NewIterator(util.BytesPrefix([]byte("packets:in")), nil)
	count := 0
	for iter.Next() {
		count++
	}
	iter.Release()
	return count
}

func (this *LevelStore) OutPacketsSize() int {
	iter := this.db.NewIterator(util.BytesPrefix([]byte("packets:out")), nil)
	count := 0
	for iter.Next() {
		count++
	}
	iter.Release()
	return count
}



func levelPacketKey(cid string, mid uint16, in bool) string {
	direction := "out"
	if in {
		direction = "in"
	}

	return "packets:" + direction + ":" + cid + ":" + strconv.Itoa(int(mid))
}
