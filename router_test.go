package mqtt
import (
	"testing"
	"strings"
	"code.google.com/p/go-uuid/uuid"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/surgemq/surgemq/topics"
)


func TestRouter(t *testing.T) {
	subhier := newSubhier()
	subhier.subscribe("a/b/+", &client{id: "1"}, 0)
	subhier.subscribe("a/+/c", &client{id: "1"}, 0)
	subhier.subscribe("a/#", &client{id: "1"}, 0)
	subhier.subscribe("a/b/#", &client{id: "1"}, 0)
	subhier.subscribe("#", &client{id: "1"}, 0)


	l, err := subhier.search("a/b/d/d", 0)
	assert.NoError(t, err)
	assert.Equal(t, l.Len(), 3)

	l, err = subhier.search("a/b/c", 0)
	assert.NoError(t, err)
	assert.Equal(t, l.Len(), 5)
}

func TestNewRouter(t *testing.T) {

	subhier := newSubhier()
	count := 1000
	start := time.Now()
	var id string
	for _ = range strings.Repeat("1", count) {
		id = uuid.New()
		path := "a/b/c/d/" + id
		subhier.processSubscribe(strings.Split(path, "/"), &client{id:id}, 0)
	}
	log.Println(float64(count) / time.Now().Sub(start).Seconds())

	start = time.Now()
	//	for _ = range strings.Repeat("1", count) {
	//		err := subhier.search("a/b/c/d/" + id, 0, func(subs *subscribes) {
	//			assert.Equal(t, subs.len(), 1)
	//		})
	//		assert.NoError(t, err)
	//	}
	log.Println(float64(count) / time.Now().Sub(start).Seconds())


}

func TestSugerRouters(t *testing.T) {

	tree := topics.NewMemProvider()
	count := 1000
	start := time.Now()
	var id string
	for _ = range strings.Repeat("1", count) {
		id = uuid.New()
		path := "a/b/c/d/" + id
		tree.Subscribe([]byte(path), 0, id)
	}
	log.Println(float64(count) / time.Now().Sub(start).Seconds())

	tree.Subscribe([]byte("a/#"), 0, id)
	start = time.Now()
	subs := make([]interface{}, 10)
	qoss := make([]byte, 10)
	for _ = range strings.Repeat("1", count) {
		tree.Subscribers([]byte("a/b/c/d/" + id), 0, &subs, &qoss)
		assert.EqualValues(t, len(subs), 2)
	}
	log.Println(float64(count) / time.Now().Sub(start).Seconds())


}


